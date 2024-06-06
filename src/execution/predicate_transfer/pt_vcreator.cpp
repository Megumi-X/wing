#include "execution/predicate_transfer/pt_vcreator.hpp"

#include "common/bloomfilter.hpp"

namespace wing {

void PtVecCreator::Execute() {
    input_->Init();
    TupleBatch tuple_batch = input_->Next();
    result_.clear();
    for (size_t i = 0; i < num_cols_; ++i) {
        result_.push_back(std::string());
        utils::BloomFilter::Create(1e5, 20, result_[i]);
    }
    while (tuple_batch.size() > 0) {
        for (auto tuple : tuple_batch) {
            for (size_t i = 0; i < num_cols_; i++) {
                size_t hash = 0;
                if (tuple.GetElemType(i) == LogicalType::INT || tuple.GetElemType(i) == LogicalType::FLOAT) {
                    uint64_t data = tuple[i].ReadInt();
                    hash = utils::BloomFilter::BloomHash(std::string_view(
                        reinterpret_cast<const char*>(&data), sizeof(uint64_t)));
                } else if (tuple.GetElemType(i) == LogicalType::STRING) {
                    hash = utils::BloomFilter::BloomHash(tuple[i].ReadStringView());
                }
                utils::BloomFilter::Add(hash, result_[i]);
            }
        }
        tuple_batch = input_->Next();
    }
}

}  // namespace wing
