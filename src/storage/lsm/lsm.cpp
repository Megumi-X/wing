#include "storage/lsm/lsm.hpp"

#include <fstream>

#include "common/stopwatch.hpp"
#include "storage/lsm/compaction_job.hpp"
#include "storage/lsm/stats.hpp"

namespace wing {

namespace lsm {

DBImpl::DBImpl(const Options& options)
  : options_(options), cache_(options_.cache) {
  if (options_.create_new) {
    seq_ = 0;
    sv_ = std::make_shared<SuperVersion>(std::make_shared<MemTable>(),
        std::make_shared<std::vector<std::shared_ptr<MemTable>>>(),
        std::make_shared<Version>());
    filename_gen_ =
        std::make_unique<FileNameGenerator>(options_.db_path.string() + "/", 0);
  } else {
    LoadMetadata();
  }
  if (options_.compaction_strategy_name == "leveled") {
    compaction_picker_ = std::make_unique<LeveledCompactionPicker>(
        options_.compaction_size_ratio,
        options_.level0_compaction_trigger * options_.sst_file_size,
        options_.level0_compaction_trigger);
  } else if (options_.compaction_strategy_name == "tiered") {
    compaction_picker_ =
        std::make_unique<TieredCompactionPicker>(options_.compaction_size_ratio,
            options_.level0_compaction_trigger * options_.sst_file_size,
            options_.level0_compaction_trigger);
  } else if (options_.compaction_strategy_name == "lazyleveling") {
    compaction_picker_ = std::make_unique<LazyLevelingCompactionPicker>(
        options_.compaction_size_ratio,
        options_.level0_compaction_trigger * options_.sst_file_size,
        options_.level0_compaction_trigger);
  } else if (options_.compaction_strategy_name == "fluid") {
    compaction_picker_ = std::make_unique<FluidCompactionPicker>(
        options_.target_alpha_part3, options_.target_scan_length_part3,
        options_.level0_compaction_trigger * options_.sst_file_size,
        options_.level0_compaction_trigger);
  }

  threads_.emplace_back([&]() { FlushThread(); });
  threads_.emplace_back([&]() { CompactionThread(); });
}

DBImpl::~DBImpl() {
  FlushAll();
  stop_signal_ = true;
  flush_cv_.notify_all();
  compact_cv_.notify_all();
  for (auto& thread : threads_) {
    thread.join();
  }
  Save();
}

void DBImpl::StopWrite() {
  db_mutex_.unlock();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  db_mutex_.lock();
}

void DBImpl::SwitchMemtable(bool force) {
  std::unique_lock db_lck(db_mutex_);
  auto old_sv = GetSV();
  while (old_sv->GetImms()->size() >= options_.max_immutable_count) {
    old_sv.reset();
    StopWrite();
    old_sv = GetSV();
  }
  if ((force && old_sv->GetMt()->size() > 0) ||
      old_sv->GetMt()->size() > options_.sst_file_size) {
    auto mt = old_sv->GetMt();
    auto new_imm = std::make_shared<std::vector<std::shared_ptr<MemTable>>>();
    auto version = old_sv->GetVersion();
    new_imm->push_back(mt);
    new_imm->insert(
        new_imm->end(), old_sv->GetImms()->begin(), old_sv->GetImms()->end());
    auto new_mt = std::make_shared<MemTable>();
    auto new_sv = std::make_shared<SuperVersion>(new_mt, new_imm, version);
    InstallSV(new_sv);
    DB_INFO("{}", new_sv->ToString());
    flush_cv_.notify_one();
  }
}

void DBImpl::Put(Slice key, Slice value) {
  std::unique_lock lck(write_mutex_);
  auto seq = ++seq_;
  auto sv = GetSV();
  sv->GetMt()->Put(key, seq, value);
  if (sv->GetMt()->size() > options_.sst_file_size) {
    SwitchMemtable();
  }
}

void DBImpl::Del(Slice key) {
  std::unique_lock lck(write_mutex_);
  auto seq = ++seq_;
  auto sv = GetSV();
  sv->GetMt()->Del(key, seq);
  if (sv->GetMt()->size() > options_.sst_file_size) {
    SwitchMemtable();
  }
}

void DBImpl::DropAll() {
  WaitForFlushAndCompaction();
  std::unique_lock db_lck(db_mutex_);
  auto sv = GetSV();
  auto new_sv = std::make_shared<SuperVersion>(std::make_shared<MemTable>(),
      std::make_shared<std::vector<std::shared_ptr<MemTable>>>(),
      std::make_shared<Version>());
  auto version = sv->GetVersion();
  for (auto& level : version->GetLevels()) {
    for (auto& sr : level.GetRuns()) {
      sr->SetRemoveTag(true);
    }
  }
  InstallSV(new_sv);
}

bool DBImpl::Get(Slice key, std::string* value) {
  auto sv = GetSV();
  auto seq = seq_;
  return sv->Get(key, seq, value);
}

void DBImpl::SaveMetadata() {
  auto metadata_file = options_.db_path.string() + "/metadata";
  FileWriter writer(
      std::make_unique<SeqWriteFile>(metadata_file, options_.use_direct_io),
      1 << 20);
  auto sv = GetSV();
  auto version = sv->GetVersion();
  writer.AppendValue<uint64_t>(seq_)
      .AppendValue<uint64_t>(filename_gen_->GetID())
      .AppendValue<uint64_t>(version->GetLevels().size());
  for (auto& level : version->GetLevels()) {
    writer.AppendValue<uint64_t>(level.GetID())
        .AppendValue<uint64_t>(level.GetRuns().size());
    for (auto& run : level.GetRuns()) {
      writer.AppendValue<uint64_t>(run->GetSSTs().size());
      for (auto& sst : run->GetSSTs()) {
        auto& info = sst->GetSSTInfo();
        writer.AppendValue<uint64_t>(info.count_)
            .AppendValue<uint64_t>(info.size_)
            .AppendValue<uint64_t>(info.sst_id_)
            .AppendValue<uint64_t>(info.index_offset_)
            .AppendValue<uint64_t>(info.bloom_filter_offset_)
            .AppendValue<uint64_t>(info.filename_.size())
            .AppendString(info.filename_);
      }
    }
  }
  writer.Flush();
}

void DBImpl::LoadMetadata() {
  auto metadata_filename = options_.db_path.string() + "/metadata";
  auto file =
      std::make_unique<ReadFile>(metadata_filename, options_.use_direct_io);
  FileReader reader(file.get(), 1 << 20, 0);
  seq_ = reader.ReadValue<uint64_t>();
  auto latest_file_id = reader.ReadValue<uint64_t>();
  auto num_levels = reader.ReadValue<uint64_t>();
  std::vector<Level> levels;
  for (uint64_t i = 0; i < num_levels; i++) {
    auto id = reader.ReadValue<uint64_t>();
    auto num_run = reader.ReadValue<uint64_t>();
    std::vector<std::shared_ptr<SortedRun>> runs;
    for (uint64_t j = 0; j < num_run; j++) {
      auto num_sst = reader.ReadValue<uint64_t>();
      std::vector<SSTInfo> ssts;
      for (uint64_t k = 0; k < num_sst; k++) {
        SSTInfo info;
        info.count_ = reader.ReadValue<uint64_t>();
        info.size_ = reader.ReadValue<uint64_t>();
        info.sst_id_ = reader.ReadValue<uint64_t>();
        info.index_offset_ = reader.ReadValue<uint64_t>();
        info.bloom_filter_offset_ = reader.ReadValue<uint64_t>();
        auto len = reader.ReadValue<uint64_t>();
        info.filename_ = reader.ReadString(len);
        ssts.push_back(info);
      }
      runs.push_back(std::make_shared<SortedRun>(
          ssts, options_.block_size, options_.use_direct_io));
    }
    levels.emplace_back(id, std::move(runs));
  }
  auto version = std::make_shared<Version>(std::move(levels));
  sv_ = std::make_shared<SuperVersion>(std::make_shared<MemTable>(),
      std::make_shared<std::vector<std::shared_ptr<MemTable>>>(),
      std::move(version));
  DB_INFO("SuperVersion: {}", sv_->ToString());
  filename_gen_ = std::make_unique<FileNameGenerator>(
      options_.db_path.string() + "/", latest_file_id);
}

void DBImpl::Save() { SaveMetadata(); }

void DBImpl::FlushAll() {
  SwitchMemtable(true);
  while (true) {
    {
      auto sv = GetSV();
      if (sv->GetMt()->size() == 0 && sv->GetImms()->size() == 0) {
        break;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

void DBImpl::WaitForFlushAndCompaction() {
  while (true) {
    db_mutex_.lock();
    if (!flush_flag_ && !compact_flag_) {
      db_mutex_.unlock();
      return;
    }
    db_mutex_.unlock();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

void DBImpl::FlushThread() {
  while (!stop_signal_) {
    /* Wait for the signal from SwitchMemtable */
    std::unique_lock lck(db_mutex_);
    if (stop_signal_) {
      flush_flag_ = false;
      return;
    }
    /* Pick the memtables that require flushing */
    std::vector<std::shared_ptr<MemTable>> imms;
    {
      auto old_sv = GetSV();
      while (old_sv->GetVersion()->GetLevels().size() > 0 &&
             old_sv->GetVersion()->GetLevels()[0].GetRuns().size() >=
                 options_.level0_stop_writes_trigger) {
        old_sv.reset();
        StopWrite();
        old_sv = GetSV();
      }
      imms = PickMemTables();
      if (imms.empty()) {
        old_sv.reset();
        flush_flag_ = false;
        flush_cv_.wait(lck);
        continue;
      }

      for (auto& imm : imms) {
        imm->SetFlushInProgress(true);
      }
      flush_flag_ = true;
    }
    /* Flush the memtables */
    std::vector<std::shared_ptr<SortedRun>> runs;
    {
      db_mutex_.unlock();
      for (auto& imm : imms) {
        CompactionJob worker(filename_gen_.get(), options_.block_size,
            options_.sst_file_size, options_.write_buffer_size,
            options_.bloom_bits_per_key, options_.use_direct_io);
        auto ssts = worker.Run(imm->Begin());
        if (ssts.empty()) {
          continue;
        }
        runs.push_back(std::make_shared<SortedRun>(
            ssts, options_.block_size, options_.use_direct_io));
        GetStatsContext()->total_input_bytes.fetch_add(
            runs.back()->size(), std::memory_order_relaxed);
      }
      db_mutex_.lock();
    }
    /* Install the new SuperVersion */
    {
      for (auto& imm : imms) {
        imm->SetFlushComplete(true);
      }
      auto old_sv = GetSV();
      auto mt = old_sv->GetMt();
      auto new_imm = std::make_shared<std::vector<std::shared_ptr<MemTable>>>();
      auto new_version = std::make_shared<Version>(*old_sv->GetVersion());
      /* Filter out all completed Memtables */
      for (auto imm : *old_sv->GetImms()) {
        if (!imm->GetFlushComplete()) {
          new_imm->push_back(imm);
        }
      }
      /* Append the sorted runs to the first level (L0) of the LSM tree. */
      new_version->Append(0, std::move(runs));
      auto new_sv =
          std::make_shared<SuperVersion>(std::move(mt), new_imm, new_version);
      DB_INFO("{}", new_sv->ToString());
      InstallSV(std::move(new_sv));
      compact_cv_.notify_one();
    }
  }
}

void DBImpl::CompactionThread() {
  while (!stop_signal_) {
    std::unique_lock lck(db_mutex_);
    // Check if it has to stop. 
    // It has to stop when the LSM-tree shutdowns.
    if (stop_signal_) {
      compact_flag_ = false;
      return;
    }
    std::unique_ptr<Compaction> compaction = compaction_picker_->Get(sv_->GetVersion().get());
    if (!compaction) {
      compact_flag_ = false;
      compact_cv_.wait(lck);
      continue;
    }
    // DB_INFO("Compaction: {}, {}, {} -> {}", compaction->input_runs().size(), compaction->input_ssts().size(), compaction->src_level(), compaction->target_level());
    compact_flag_ = true;
    // Do some other things
    db_mutex_.unlock();
    // Do compaction
    // int count1 = 0;
    // int count2 = 0;
    CompactionJob worker(filename_gen_.get(), options_.block_size,
        options_.sst_file_size, options_.write_buffer_size,
        options_.bloom_bits_per_key, options_.use_direct_io);
    IteratorHeap<Iterator> heap;
    std::vector<std::shared_ptr<SSTableIterator>> iters;
    std::shared_ptr<SortedRunIterator> run_iter;
    if (compaction->input_ssts().size() > 0) {
      for (auto& sst: compaction->input_ssts()) {
        iters.push_back(std::make_shared<SSTableIterator>(sst.get()));
        heap.Push(iters.back().get());
        sst->SetCompactionInProcess(true);
        // count1 += sst->GetSSTInfo().count_;
      }
    }
    // std::cout << "tmp_count1: " << count1 << std::endl;
    int overlap_count = 0;
    if (compaction->target_sorted_run() && compaction->type == "level") {
      if (compaction->src_level() > 0) {
        for (auto& sst: compaction->target_sorted_run()->GetSSTs()) {
          if (sst->GetLargestKey().user_key_ < compaction->input_ssts()[0]->GetSmallestKey().user_key_) continue;
          else if (sst->GetSmallestKey().user_key_ > compaction->input_ssts()[0]->GetLargestKey().user_key_) break;
          iters.push_back(std::make_shared<SSTableIterator>(sst.get()));
          heap.Push(iters.back().get());
          overlap_count++;
          sst->SetCompactionInProcess(true);
          sst->SetRemoveTag(true);
          // count1 += sst->GetSSTInfo().count_;
        }
        // DB_INFO("{}", iters.size());
      } else {
        run_iter = std::make_shared<SortedRunIterator>(compaction->target_sorted_run().get(),
          SSTableIterator(compaction->target_sorted_run()->GetSSTs()[0].get()), 0);
        heap.Push(run_iter.get());
        for (auto& sst: compaction->target_sorted_run()->GetSSTs()) {
          sst->SetCompactionInProcess(true);
          sst->SetRemoveTag(true);
        }
      }
    } else if (compaction->target_sorted_run() && compaction->type == "lazy") {
      run_iter = std::make_shared<SortedRunIterator>(compaction->target_sorted_run().get(),
        SSTableIterator(compaction->target_sorted_run()->GetSSTs()[0].get()), 0);
      heap.Push(run_iter.get());
      for (auto& sst: compaction->target_sorted_run()->GetSSTs()) {
        sst->SetCompactionInProcess(true);
        sst->SetRemoveTag(true);
      }
    }
    std::vector<std::shared_ptr<SSTable>> ssts;
    // StopWatch sw;
    if (compaction->type == "level") {
      if (compaction->target_sorted_run() && overlap_count > 0){
        std::vector<SSTInfo> sst_infos;
        sst_infos = worker.Run(heap);
        for (auto& sst: compaction->input_ssts()) {
          sst->SetCompactionInProcess(true);
          sst->SetRemoveTag(true);
        }
        SortedRun run(sst_infos, options_.block_size, options_.use_direct_io);
        ssts = run.GetSSTs();
        // for (auto& sst: ssts) count2 += sst.count_;
      } else if (compaction->src_level() == 0) {
        std::vector<SSTInfo> sst_infos;
        sst_infos = worker.Run(heap);
        for (auto& sst: compaction->input_ssts()) {
          sst->SetCompactionInProcess(true);
          sst->SetRemoveTag(true);
        }
        SortedRun run(sst_infos, options_.block_size, options_.use_direct_io);
        ssts = run.GetSSTs();
      } else {
        ssts = compaction->input_ssts();
      }
    } else if (compaction->type == "lazy") {
      if (compaction->trivial_move() == false) {
        std::vector<SSTInfo> sst_infos;
        sst_infos = worker.Run(heap);
        for (auto& sst: compaction->input_ssts()) {
          sst->SetCompactionInProcess(true);
          sst->SetRemoveTag(true);
        }
        SortedRun run(sst_infos, options_.block_size, options_.use_direct_io);
        ssts = run.GetSSTs();
      }
    }
    // DB_INFO("Cost {}s", sw.GetTimeInSeconds());
    // std::cout << "count1: " << count1 << " count2: " << count2 << std::endl;
    db_mutex_.lock();
    // Create a new superversion and install it
    // size_t old_count = sv_->count_keys();
    std::shared_ptr<Version> new_version = std::make_shared<Version>();
    size_t src_level_index = compaction->src_level();
    size_t target_level_index = compaction->target_level();
    if (compaction->type == "level") {
      if (src_level_index == 0) {
        new_version->Append(target_level_index, std::make_shared<SortedRun>(ssts, options_.block_size, options_.use_direct_io));
        std::vector<std::shared_ptr<SortedRun>> inputs;
        int count = 0;
        for (auto& run: sv_->GetVersion()->GetLevels()[src_level_index].GetRuns()) {
          std::vector<std::shared_ptr<SSTable>> ssts;
          for (auto& sst: run->GetSSTs()) {
            if (sst->GetCompactionInProcess()) {
              sst->SetRemoveTag(true);
              continue;
            }
            ssts.push_back(sst);
            count++;
          }
          if (ssts.size() == 0) continue;
          inputs.push_back(std::make_shared<SortedRun>(ssts, options_.block_size, options_.use_direct_io));
        }
        if (compaction->target_sorted_run()) {
          for (auto& sst: compaction->target_sorted_run()->GetSSTs()) sst->SetRemoveTag(true);
        }
        new_version->Append(src_level_index, inputs);
        for (size_t i = 2; i < sv_->GetVersion()->GetLevels().size(); i++) {
          new_version->Append(i, sv_->GetVersion()->GetLevels()[i].GetRuns());
        }
        // DB_INFO("Cost source 0 {}s", sw.GetTimeInSeconds());
      } else {
        size_t levels_num = sv_->GetVersion()->GetLevels().size();
        for (size_t i = 0; i < levels_num; i++) {
          if (i == target_level_index) {
            // new_version->Append(i, std::make_shared<SortedRun>(ssts, options_.block_size, options_.use_direct_io));
            if (!compaction->target_sorted_run()) {
              new_version->Append(i, std::make_shared<SortedRun>(ssts, options_.block_size, options_.use_direct_io));
              continue;
            }
            std::vector<std::shared_ptr<SSTable>> outputs;
            if (overlap_count == 0) {
              if (compaction->input_ssts()[0]->GetLargestKey().user_key_ < compaction->target_sorted_run()->GetSmallestKey().user_key_) {
                outputs.push_back(compaction->input_ssts()[0]);
                for (auto& sst: compaction->target_sorted_run()->GetSSTs()) {
                  outputs.push_back(sst);
                }             
              } else if (compaction->input_ssts()[0]->GetSmallestKey().user_key_ > compaction->target_sorted_run()->GetLargestKey().user_key_) {
                for (auto& sst: compaction->target_sorted_run()->GetSSTs()) {
                  outputs.push_back(sst);
                }
                outputs.push_back(compaction->input_ssts()[0]);
              } else {
                int count = 0;
                for (auto& sst: compaction->target_sorted_run()->GetSSTs()) {
                  if (sst->GetLargestKey().user_key_ < compaction->input_ssts()[0]->GetSmallestKey().user_key_){
                    outputs.push_back(sst);
                  } else {
                    if (count == 0){
                      outputs.push_back(compaction->input_ssts()[0]);
                      count++;
                    }
                    outputs.push_back(sst);
                  }
                }
              }
              new_version->Append(i, std::make_shared<SortedRun>(outputs, options_.block_size, options_.use_direct_io));
              // DB_INFO("Cost overlap = 0 {}s", sw.GetTimeInSeconds());
              continue;
            }     
            // DB_INFO("Cost target level 1 {}s", sw.GetTimeInSeconds());    
            int count = 0;
            for (auto& sst: sv_->GetVersion()->GetLevels()[target_level_index].GetRuns()[0]->GetSSTs()) {
              if (sst->GetCompactionInProcess()) {
                if (count == 0) {
                  outputs.insert(outputs.end(), ssts.begin(), ssts.end());
                  count++;
                } 
                sst->SetRemoveTag(true);
                continue;
              }
              outputs.push_back(sst);
            }
            new_version->Append(i, std::make_shared<SortedRun>(outputs, options_.block_size, options_.use_direct_io));
            // DB_INFO("Cost target level 2 {}s", sw.GetTimeInSeconds()); 
            
          } else if (i == src_level_index) {
            std::vector<std::shared_ptr<SSTable>> inputs;
            // int count = 0;
            for (auto& sst: sv_->GetVersion()->GetLevels()[src_level_index].GetRuns()[0]->GetSSTs()) {
              if (sst->GetCompactionInProcess()){
                if (overlap_count > 0)
                  sst->SetRemoveTag(true);
                continue;
              }
              inputs.push_back(sst);
            }
            // std::cout << "count: " << count << std::endl;
            new_version->Append(i, std::make_shared<SortedRun>(inputs, options_.block_size, options_.use_direct_io));
            // DB_INFO("Cost source level {}s", sw.GetTimeInSeconds());
          } else {
            new_version->Append(i, sv_->GetVersion()->GetLevels()[i].GetRuns());
          }
        }
        if (target_level_index == levels_num) {
          new_version->Append(target_level_index, std::make_shared<SortedRun>(ssts, options_.block_size, options_.use_direct_io));
        }
        // DB_INFO("Cost source 1+ {}s", sw.GetTimeInSeconds());
      }
    } else if (compaction->type == "lazy") {
      size_t levels_num = sv_->GetVersion()->GetLevels().size();
      if (levels_num == 1) {
        for (size_t i = 1; i < sv_->GetVersion()->GetLevels().back().GetRuns().size(); i++) {
            new_version->Append(levels_num - 1, sv_->GetVersion()->GetLevels().back().GetRuns()[i]);
          }
        new_version->Append(levels_num, compaction->input_runs());
      } else {
        if (target_level_index <= levels_num - 2) {
          for (size_t i = 0; i < levels_num; i++) {
            if (i == src_level_index && i != 0) continue;
            if (i == src_level_index && i == 0) {
              for (auto& run : sv_->GetVersion()->GetLevels()[i].GetRuns()) {
                std::vector<std::shared_ptr<SSTable>> ssts;
                for (auto& sst : run->GetSSTs()) {
                  if (sst->GetCompactionInProcess()) {
                    sst->SetRemoveTag(true);
                    continue;
                  }
                  ssts.push_back(sst);
                }
                if (ssts.size() != 0)
                  new_version->Append(i, std::make_shared<SortedRun>(ssts, options_.block_size, options_.use_direct_io));
              }
              continue;
            }
            if (i == target_level_index) {
              new_version->Append(i, std::make_shared<SortedRun>(ssts, options_.block_size, options_.use_direct_io));
              new_version->Append(i, sv_->GetVersion()->GetLevels()[i].GetRuns());
              continue;
            }
            new_version->Append(i, sv_->GetVersion()->GetLevels()[i].GetRuns());
          }
        } else if (target_level_index == levels_num) {
          for (size_t i = 0; i <= levels_num - 2; i++) {
            new_version->Append(i, sv_->GetVersion()->GetLevels()[i].GetRuns());
          }
          for (size_t i = 1; i < sv_->GetVersion()->GetLevels().back().GetRuns().size(); i++) {
            new_version->Append(levels_num - 1, sv_->GetVersion()->GetLevels().back().GetRuns()[i]);
          }
          new_version->Append(levels_num, compaction->input_runs());
        } else if (target_level_index == levels_num - 1) {
          if (levels_num >= 3) {
            for (size_t i = 0; i <= levels_num - 3; i++) {
              new_version->Append(i, sv_->GetVersion()->GetLevels()[i].GetRuns());
            }
          }
          new_version->Append(levels_num - 1, std::make_shared<SortedRun>(ssts, options_.block_size, options_.use_direct_io));
          if (src_level_index == 0) {
            for (auto& run : sv_->GetVersion()->GetLevels()[0].GetRuns()) {
              std::vector<std::shared_ptr<SSTable>> ssts;
              for (auto& sst : run->GetSSTs()) {
                if (sst->GetCompactionInProcess()) {
                  sst->SetRemoveTag(true);
                  continue;
                }
                ssts.push_back(sst);
              }
              if (ssts.size() != 0)
                new_version->Append(0, std::make_shared<SortedRun>(ssts, options_.block_size, options_.use_direct_io));
            }
          }
        }
      }
    }
    std::shared_ptr<SuperVersion> new_sv = std::make_shared<SuperVersion>(sv_->GetMt(), sv_->GetImms(), new_version);
    // size_t new_count = new_sv->count_keys();
    // if (new_count != old_count)
    //   std::cerr << "old count: " << old_count << " new count: " << new_count << " src_level: " << compaction->src_level() << " target_level: " << compaction->target_level() << " input size: " << compaction->input_ssts().size() << " overlap: " << overlap_count << std::endl;
    for (auto& sst: compaction->input_ssts()) {
      sst->SetCompactionInProcess(false);
    }
    if (compaction->target_sorted_run()) {
      for (auto& sst: compaction->target_sorted_run()->GetSSTs()) {
        sst->SetCompactionInProcess(false);
      }
    }
    InstallSV(new_sv);
  }
}

std::vector<std::shared_ptr<MemTable>> DBImpl::PickMemTables() {
  std::vector<std::shared_ptr<MemTable>> ret;
  for (auto imm : *sv_->GetImms()) {
    if (!imm->GetFlushInProgress() && !imm->GetFlushComplete()) {
      ret.push_back(std::move(imm));
    }
  }
  return ret;
}

std::shared_ptr<SuperVersion> DBImpl::GetSV() {
  std::shared_lock lck(sv_mutex_);
  auto new_sv = sv_;
  return new_sv;
}

void DBImpl::InstallSV(std::shared_ptr<SuperVersion> sv) {
  std::unique_lock lck(sv_mutex_);
  sv_ = std::move(sv);
}

DBIterator DBImpl::Begin() {
  DBIterator it(GetSV(), seq_);
  it.SeekToFirst();
  return it;
}

DBIterator DBImpl::Seek(Slice key) {
  DBIterator it(GetSV(), seq_);
  it.Seek(key);
  return it;
}

void DBIterator::SeekToFirst() {
  it_.SeekToFirst();
  if (it_.Valid()) {
    current_key_ = ParsedKey(it_.key());
    if (current_key_.record_type() == RecordType::Deletion ||
        current_key_.seq() > seq_) {
      Next();
    }
  }
}

void DBIterator::Seek(Slice key) {
  it_.Seek(key, seq_);
  if (it_.Valid()) {
    current_key_ = ParsedKey(it_.key());
    if (current_key_.record_type() == RecordType::Deletion ||
        current_key_.seq() > seq_) {
      Next();
    }
  }
}

bool DBIterator::Valid() { return it_.Valid(); }

Slice DBIterator::key() const { return current_key_.user_key(); }

Slice DBIterator::value() const { return it_.value(); }

void DBIterator::Next() {
  it_.Next();
  while (true) {
    while (it_.Valid() && (seq_ < ParsedKey(it_.key()).seq_ ||
                              (current_key_.seq() <= seq_ &&
                                  current_key_.user_key() ==
                                      ParsedKey(it_.key()).user_key_))) {
      it_.Next();
    }
    if (it_.Valid()) {
      current_key_ = ParsedKey(it_.key());
      if (current_key_.record_type() == RecordType::Deletion) {
        it_.Next();
        continue;
      }
    }
    break;
  }
}

}  // namespace lsm

}  // namespace wing
