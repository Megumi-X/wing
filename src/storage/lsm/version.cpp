#include "storage/lsm/version.hpp"
#include <iostream>

namespace wing {

namespace lsm {

bool Version::Get(std::string_view user_key, seq_t seq, std::string* value) {
  for (auto& lev : levels_) {
    GetResult res = lev.Get(user_key, seq, value);
    if (res == GetResult::kFound) {
      return true;
    } else if (res == GetResult::kDelete) {
      return false;
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
  GetResult res = mt_->Get(user_key, seq, value);
  if (res != GetResult::kNotFound) {
    if (res == GetResult::kDelete) return false;
    return true;
  }
  for (auto& imm : *imms_) {
    res = imm->Get(user_key, seq, value);
    if (res != GetResult::kNotFound) {
      if (res == GetResult::kDelete) return false;
      return true;
    }
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

size_t SuperVersion::count_keys() {
  size_t ret = 0;
  for (auto& level : version_->GetLevels()) {
    for (auto& run : level.GetRuns()) {
      for (auto& sst : run->GetSSTs()) {
        ret += sst->GetSSTInfo().count_;
      }
    }
  }
  return ret;

}

void SuperVersionIterator::SeekToFirst() {
  it_.Clear();
  for (auto& mt_it: mt_its_) {
    mt_it.SeekToFirst();
    it_.Push(&mt_it);
  }
  for (auto& sst_it: sst_its_) {
    sst_it.SeekToFirst();
    it_.Push(&sst_it);
  }
}

void SuperVersionIterator::Seek(Slice key, seq_t seq) {
  SeekToFirst();
  it_.Clear();
  for (auto& mt_it: mt_its_) {
    mt_it.Seek(key, seq);
    if (mt_it.Valid() && ParsedKey(mt_it.key()) >= ParsedKey(key, seq, RecordType::Value))
      it_.Push(&mt_it);
  }
  for (auto& sst_it: sst_its_) {
    sst_it.Seek(key, seq);
    if (sst_it.Valid() && ParsedKey(sst_it.key()) >= ParsedKey(key, seq, RecordType::Value))
      it_.Push(&sst_it);
  }
}

bool SuperVersionIterator::Valid() { return it_.Valid(); }

Slice SuperVersionIterator::key() const { return it_.key(); }

Slice SuperVersionIterator::value() const { return it_.value(); }

void SuperVersionIterator::Next() { it_.Next(); }

}  // namespace lsm

}  // namespace wing
