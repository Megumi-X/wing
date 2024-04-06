#include "storage/lsm/compaction_pick.hpp"

namespace wing {

namespace lsm {

std::unique_ptr<Compaction> LeveledCompactionPicker::Get(Version* version) {
  std::vector<Level> levels = version->GetLevels();
  if (levels.size() == 0) return nullptr; 
  for (int i = levels.size() - 1; i >= 1; i--) {
    if (levels[i].size() > base_level_size_ * std::pow(ratio_, i - 1)) {
      auto input_runs = levels[i].GetRuns();
      if (i == levels.size() - 1 || levels[i + 1].GetRuns().size() == 0 ||
         levels[i + 1].GetRuns()[0]->GetSSTs().size() == 0 || levels[i + 1].GetRuns()[0]->GetSSTs()[0] == nullptr) {
        std::vector<std::shared_ptr<SSTable>> input_tables;
        input_tables.insert(input_tables.end(), levels[i].GetRuns()[0]->GetSSTs().begin(), levels[i].GetRuns()[0]->GetSSTs().end());
        return std::make_unique<Compaction>(input_tables, input_runs, i, i + 1, nullptr, true);
      }
      auto target_runs = levels[i + 1].GetRuns();
      std::vector<std::shared_ptr<SSTable>> input_tables;
      int smallest_overlap = std::numeric_limits<int>::max();
      std::shared_ptr<SSTable> smallest_overlap_sst;
      for (auto& sst : input_runs[0]->GetSSTs()) {
        if (sst->GetLargestKey().user_key_ < target_runs[0]->GetSmallestKey().user_key_ || sst->GetSmallestKey().user_key_ > target_runs[0]->GetLargestKey().user_key_){
          smallest_overlap_sst = std::shared_ptr<SSTable>(sst);
          smallest_overlap = 0;
          break;
        }
        int overlap_count = 0;
        for (auto& target_sst : target_runs[0]->GetSSTs()) {
          if (sst->GetLargestKey().user_key_ < target_sst->GetSmallestKey().user_key_) {
            continue;
          } else if (sst->GetSmallestKey().user_key_ > target_sst->GetLargestKey().user_key_) {
            break;
          }
          overlap_count++;
        }
        if (overlap_count <= 1) {
          smallest_overlap = overlap_count;
          smallest_overlap_sst = std::shared_ptr<SSTable>(sst);
          break;
        }
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

}  // namespace lsm

}  // namespace wing
