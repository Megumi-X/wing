#include "storage/lsm/compaction_pick.hpp"
#include <cmath>

namespace wing {

namespace lsm {

std::unique_ptr<Compaction> LeveledCompactionPicker::Get(Version* version) {
  std::vector<Level> levels = version->GetLevels();
  if (levels.size() == 0) return nullptr; 
  for (int i = levels.size() - 1; i >= 1; i--) {
    if (levels[i].size() > base_level_size_ * std::pow(ratio_, i)) {
      auto input_runs = levels[i].GetRuns();
      if (i == levels.size() - 1 || levels[i + 1].GetRuns().size() == 0 ||
         levels[i + 1].GetRuns()[0]->GetSSTs().size() == 0 || levels[i + 1].GetRuns()[0]->GetSSTs()[0] == nullptr) {
        std::vector<std::shared_ptr<SSTable>> input_tables;
        input_tables.insert(input_tables.end(), levels[i].GetRuns()[0]->GetSSTs().begin(), levels[i].GetRuns()[0]->GetSSTs().end());
        // input_tables.push_back(levels[i].GetRuns()[0]->GetSSTs()[0]);
        return std::make_unique<Compaction>(input_tables, input_runs, i, i + 1, nullptr, true);
      }
      auto target_runs = levels[i + 1].GetRuns();
      std::vector<std::shared_ptr<SSTable>> input_tables;
      int smallest_overlap = std::numeric_limits<int>::max();
      std::shared_ptr<SSTable> smallest_overlap_sst = input_runs[0]->GetSSTs()[0];
      size_t cursor = 0;
      for (auto& sst : input_runs[0]->GetSSTs()) {
        if (sst->GetLargestKey().user_key_ < target_runs[0]->GetSmallestKey().user_key_ || sst->GetSmallestKey().user_key_ > target_runs[0]->GetLargestKey().user_key_){
          smallest_overlap_sst = std::shared_ptr<SSTable>(sst);
          smallest_overlap = 0;
          break;
        }
        int overlap_count = 0;
        for (int i = cursor; i < target_runs[0]->GetSSTs().size(); i++) {
          if (sst->GetSmallestKey().user_key_ > target_runs[0]->GetSSTs()[i]->GetLargestKey().user_key_) {
            cursor++;
            continue;
          } else if (sst->GetLargestKey().user_key_ < target_runs[0]->GetSSTs()[i]->GetSmallestKey().user_key_) {
            break;
          }
          overlap_count++;
        }
        // DB_INFO("{}, {}", cursor, overlap_count);
        if (overlap_count < smallest_overlap) {
          smallest_overlap = overlap_count;
          smallest_overlap_sst = std::shared_ptr<SSTable>(sst);
        }
      }
      input_tables.push_back(smallest_overlap_sst);
      return std::make_unique<Compaction>(input_tables, input_runs, i, i + 1, target_runs[0], false);
    }
  }
  if (levels[0].GetRuns().size() > level0_compaction_trigger_) {
    auto input_runs = levels[0].GetRuns();
    std::vector<std::shared_ptr<SSTable>> input_tables;
    for (auto& run : input_runs) {
      input_tables.insert(input_tables.end(), run->GetSSTs().begin(), run->GetSSTs().end());
    }
    if (levels.size() == 1 || levels[1].GetRuns().size() == 0 ||
         levels[1].GetRuns()[0]->GetSSTs().size() == 0 || levels[1].GetRuns()[0]->GetSSTs()[0] == nullptr){
      return std::make_unique<Compaction>(input_tables, input_runs, 0, 1, nullptr, true);
    }
    auto target_runs = levels[1].GetRuns();
    return std::make_unique<Compaction>(input_tables, input_runs, 0, 1, target_runs[0], false);
  }
  return nullptr;
}

std::unique_ptr<Compaction> TieredCompactionPicker::Get(Version* version) {
  DB_ERR("Not implemented!");
}

std::unique_ptr<Compaction> FluidCompactionPicker::Get(Version* version) {
  std::vector<Level> levels = version->GetLevels();
  if (levels.size() == 0) return nullptr;
  if (levels.size() == 1) {
    std::vector<std::shared_ptr<SortedRun>> input_runs;
    input_runs.push_back(levels.back().GetRuns()[0]);
    std::vector<std::shared_ptr<SSTable>> input_tables = input_runs[0]->GetSSTs();
    return std::make_unique<Compaction>(input_tables, input_runs, levels.size() - 1, levels.size(), nullptr, true, "lazy");
  }
  while (k_i.size() < levels.size() - 1) {
    k_i.push_back(2);
  }
  size_t last_size = base_level_size_ * ratio_;
  for (size_t i = 1; i < levels.size() - 1; i++){
    last_size *= k_i[i];
  }
  if (levels.back().size() >= last_size) {
    std::vector<std::shared_ptr<SortedRun>> input_runs;
    input_runs.push_back(levels.back().GetRuns()[0]);
    std::vector<std::shared_ptr<SSTable>> input_tables = input_runs[0]->GetSSTs();
    return std::make_unique<Compaction>(input_tables, input_runs, levels.size() - 1, levels.size(), nullptr, true, "lazy");
  }
  if (levels.size() >= 3) {
    for (int i = 0; i <= levels.size() - 3; i++) {
      if (levels[i].GetRuns().size() >= k_i[i]) {
        auto input_runs = levels[i].GetRuns();
        std::vector<std::shared_ptr<SSTable>> input_tables;
        for (auto& run : input_runs) {
          input_tables.insert(input_tables.end(), run->GetSSTs().begin(), run->GetSSTs().end());
        }
        return std::make_unique<Compaction>(input_tables, input_runs, i, i + 1, nullptr, false, "lazy");
      }
    }
  }
  if (levels.size() >= 2 && levels[levels.size() - 2].GetRuns().size() >= k_i[levels.size() - 2]) {
    auto input_runs = levels[levels.size() - 2].GetRuns();
    std::vector<std::shared_ptr<SSTable>> input_tables;
    for (auto& run : input_runs) {
      input_tables.insert(input_tables.end(), run->GetSSTs().begin(), run->GetSSTs().end());
    }
    return std::make_unique<Compaction>(input_tables, input_runs, levels.size() - 2, levels.size() - 1, levels.back().GetRuns()[0], false, "lazy");
  }
  return nullptr;
}

std::unique_ptr<Compaction> LazyLevelingCompactionPicker::Get(
    Version* version) {
  std::vector<Level> levels = version->GetLevels();
  if (levels.size() == 0) return nullptr;
  if (levels.size() == 1) {
    std::vector<std::shared_ptr<SortedRun>> input_runs;
    input_runs.push_back(levels.back().GetRuns()[0]);
    std::vector<std::shared_ptr<SSTable>> input_tables = input_runs[0]->GetSSTs();
    return std::make_unique<Compaction>(input_tables, input_runs, levels.size() - 1, levels.size(), nullptr, true, "lazy");
  }
  if (levels.back().size() >= std::pow(ratio_, levels.size() - 1) * base_level_size_) {
    std::vector<std::shared_ptr<SortedRun>> input_runs;
    input_runs.push_back(levels.back().GetRuns()[0]);
    std::vector<std::shared_ptr<SSTable>> input_tables = input_runs[0]->GetSSTs();
    return std::make_unique<Compaction>(input_tables, input_runs, levels.size() - 1, levels.size(), nullptr, true, "lazy");
  }
  if (levels.size() >= 3) {
    for (int i = 0; i <= levels.size() - 3; i++) {
      if (levels[i].GetRuns().size() >= ratio_) {
        auto input_runs = levels[i].GetRuns();
        std::vector<std::shared_ptr<SSTable>> input_tables;
        for (auto& run : input_runs) {
          input_tables.insert(input_tables.end(), run->GetSSTs().begin(), run->GetSSTs().end());
        }
        return std::make_unique<Compaction>(input_tables, input_runs, i, i + 1, nullptr, false, "lazy");
      }
    }
  }
  if (levels.size() >= 2 && levels[levels.size() - 2].GetRuns().size() >= ratio_) {
    auto input_runs = levels[levels.size() - 2].GetRuns();
    std::vector<std::shared_ptr<SSTable>> input_tables;
    for (auto& run : input_runs) {
      input_tables.insert(input_tables.end(), run->GetSSTs().begin(), run->GetSSTs().end());
    }
    return std::make_unique<Compaction>(input_tables, input_runs, levels.size() - 2, levels.size() - 1, levels.back().GetRuns()[0], false, "lazy");
  }
  return nullptr;
}

}  // namespace lsm

}  // namespace wing
