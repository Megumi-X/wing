#include "storage/lsm/version.hpp"
#include <iostream>

namespace wing {

namespace lsm {

bool Version::Get(std::string_view user_key, seq_t seq, std::string* value) {
  for (auto& lev : levels_) {
    GetResult res = lev.Get(user_key, seq, value);
    if (res == GetResult::kFound) {
      return true;
    }
  }
  return false;
}

void Version::Append(
    uint32_t level_id, std::vector<std::shared_ptr<SortedRun>> sorted_runs) {
  while (levels_.size() <= level_id) {
    levels_.push_back(Level(levels_.size()));
  }
  levels_[level_id].Append(std::move(sorted_runs));
}
void Version::Append(uint32_t level_id, std::shared_ptr<SortedRun> sorted_run) {
  while (levels_.size() <= level_id) {
    levels_.push_back(Level(levels_.size()));
  }
  levels_[level_id].Append(std::move(sorted_run));
}

bool SuperVersion::Get(
    std::string_view user_key, seq_t seq, std::string* value) {
  if (mt_->Get(user_key, seq, value) == GetResult::kFound) return true;
  for (auto& imm : *imms_) {
    if (imm->Get(user_key, seq, value) == GetResult::kFound) return true;
  }
  return version_->Get(user_key, seq, value);
}

std::string SuperVersion::ToString() const {
  std::string ret;
  ret += fmt::format("Memtable: size {}, ", mt_->size());
  ret += fmt::format("Immutable Memtable: size {}, ", imms_->size());
  ret += fmt::format("Tree: [ ");
  for (auto& level : version_->GetLevels()) {
    size_t num_sst = 0;
    for (auto& run : level.GetRuns()) {
      num_sst += run->SSTCount();
    }
    ret += fmt::format("{}, ", num_sst);
  }
  ret += "]";
  return ret;
}

void SuperVersionIterator::SeekToFirst() {
  it_.Clear();
  mt_its_.clear();
  auto mt = sv_->GetMt();
  auto mt_it = mt->Begin();
  if (mt_it.Valid()) mt_its_.push_back(std::move(mt_it));
  for (auto& imm: *sv_->GetImms()) {
    auto imm_it = imm->Begin();
    if (imm_it.Valid()) mt_its_.push_back(std::move(imm_it));
  }
  sst_its_.clear();
  for (auto& level: sv_->GetVersion()->GetLevels()) {
    for (auto& run: level.GetRuns()) {
      auto sst_it = run->Begin();
      if (sst_it.Valid()) sst_its_.push_back(std::move(sst_it));
    }
  }
  for (auto& it: mt_its_) it_.Push(&it);
  for (auto& it: sst_its_) it_.Push(&it);
}

void SuperVersionIterator::Seek(Slice key, seq_t seq) {
  SeekToFirst();
  while (it_.Valid()){
    if (ParsedKey(it_.key()) >= ParsedKey(key, seq, RecordType::Value))
      return;
    it_.Next();
  }
}

bool SuperVersionIterator::Valid() { return it_.Valid(); }

Slice SuperVersionIterator::key() { return it_.key(); }

Slice SuperVersionIterator::value() { return it_.value(); }

void SuperVersionIterator::Next() { it_.Next(); }

}  // namespace lsm

}  // namespace wing
