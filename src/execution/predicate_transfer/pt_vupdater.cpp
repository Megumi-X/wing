#include "execution/predicate_transfer/pt_vupdater.hpp"

#include "common/bloomfilter.hpp"
#include "execution/executor.hpp"
#include "execution/vec/expr_vexecutor.hpp"

namespace wing {

void PtVecUpdater::Execute(
    const std::vector<std::string>& bloom_filter, BitVector& valid_bits, const std::vector<size_t>& bf_map) {
  size_t index = 0;
  input_->Init();
  TupleBatch tuple_batch = input_->Next();
  size_t count = 0;

  const auto count_zeros = [](const BitVector& bv) {
    size_t n = 0;
    for (size_t i = 0; i < bv.size(); ++i) {
      if ((bv)[i] == 0) ++n;
    }
    return n;
  };

  while (tuple_batch.size() > 0) {
    /* Iterate over valid tuples in the tuple batch */
    for (auto tuple : tuple_batch) {
      while (true) {
        /* skip invalid */
        if (index < valid_bits.size() && valid_bits[index] == 0) {
          index += 1;
          continue;
        }
        if (index >= valid_bits.size()) {
          /* resize the bit vector */
          size_t old_size = valid_bits.size();
          valid_bits.Resize(valid_bits.size() * 2 + 10);
          for (size_t i = index; i < valid_bits.size(); i++) {
            valid_bits[i] = 1;
          }
          break;
        }
        break;
      }
      bool hash_found = true;
      for (size_t bi = 0; bi < bloom_filter.size(); ++bi) {
        const auto& bf = bloom_filter[bi];
        size_t hash = 0;
        size_t i = bf_map[bi];
        if (tuple.GetElemType(i) == LogicalType::INT ||
            tuple.GetElemType(i) == LogicalType::FLOAT) {
          uint64_t data = tuple[i].ReadInt();
          hash = utils::BloomFilter::BloomHash(std::string_view(
              reinterpret_cast<const char*>(&data), sizeof(uint64_t)));
        } else if (tuple.GetElemType(i) == LogicalType::STRING) {
          hash = utils::BloomFilter::BloomHash(tuple[i].ReadStringView());
        }
        if (!utils::BloomFilter::Find(hash, bf)) {
          hash_found = false;
          break;
        }
      }
      if (!hash_found) {
        /* set the bit to 0 */
        valid_bits[index] = 0;
        count++;
      }
      index += 1;
    }
    tuple_batch = input_->Next();
  }
}

}  // namespace wing
