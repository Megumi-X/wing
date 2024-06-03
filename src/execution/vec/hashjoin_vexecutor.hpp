#pragma once

#include "execution/executor.hpp"
#include "common/murmurhash.hpp"
#include "common/stopwatch.hpp"
#include <iostream>

namespace wing {

class HashJoinVecExecutor : public VecExecutor {
public:
    HashJoinVecExecutor(const ExecOptions& options,
        const std::unique_ptr<Expr>& expr, OutputSchema output_schema,
        std::unique_ptr<VecExecutor> ch, std::unique_ptr<VecExecutor> ch2, 
        const std::vector<std::unique_ptr<Expr>>& left_hash_exprs,
        const std::vector<std::unique_ptr<Expr>>& right_hash_exprs,
        OutputSchema left_output_schema, OutputSchema right_output_schema): 
    VecExecutor(options), ch_(std::move(ch)), ch2_(std::move(ch2)), output_schema_(output_schema) {
        max_batch_size_ = options.max_batch_size;
        expr_ = ExprVecExecutor::Create(expr.get(), output_schema_);
        left_hash_exprs_.reserve(left_hash_exprs.size());
        for (auto& ex : left_hash_exprs) {
            left_hash_exprs_.emplace_back(ExprVecExecutor::Create(ex.get(), left_output_schema));
        }
        right_hash_exprs_.reserve(right_hash_exprs.size());
        for (auto& ex : right_hash_exprs) {
            right_hash_exprs_.emplace_back(ExprVecExecutor::Create(ex.get(), right_output_schema));
        }
    }
    void Init() noexcept {
        ch_->Init();
        ch2_->Init();
        build_tuples_.clear();
        hash_table_.clear();
        for (TupleBatch tb = ch_->Next(); tb.size() > 0; tb = ch_->Next()) {
            TupleBatch tb2;
            tb2.Init(tb);
            build_tuples_.push_back(tb2);
            std::vector<Vector> hash_cols(left_hash_exprs_.size());
            for (size_t i = 0; i < left_hash_exprs_.size(); i++) {
                left_hash_exprs_[i].Evaluate(tb2.GetCols(), tb2.size(), hash_cols[i]);
            }
            for (size_t i = 0; i < tb2.size(); i++) {
                size_t hash_val = 99;
                for (size_t j = 0; j < hash_cols.size(); j++) {
                    if (hash_cols[j].GetElemType() == LogicalType::STRING) {
                        hash_val = utils::Hash(hash_cols[j].Get(i).ReadString(), hash_val);
                    } else {
                        hash_val = utils::Hash8(hash_cols[j].Get(i).ReadInt(), hash_val);
                    }
                }
                if (hash_table_.find(hash_val) == hash_table_.end()) {
                    hash_table_[hash_val] = std::vector<std::pair<size_t, size_t>>();
                    hash_table_[hash_val].push_back(std::make_pair(build_tuples_.size() - 1, i));
                } else {
                    hash_table_[hash_val].push_back(std::make_pair(build_tuples_.size() - 1, i));
                }
            }
        }
        update_probe();
    }

    TupleBatch InternalNext() {
        TupleBatch ret;
        ret.Init(output_schema_.GetTypes(), max_batch_size_);
        while (probe_tb_.size() > 0) {    
            for (; probe_tb_idx_ < probe_tb_.size(); ++probe_tb_idx_) {
                size_t i = probe_tb_idx_;
                if (hash_table_end_) {
                    it_ = hash_table_.find(right_hash_vals_[i]);
                    hash_table_end_ = false;
                }
                if (it_ != hash_table_.end()) {
                    for (; hash_table_idx_ < it_->second.size(); ++hash_table_idx_) {
                        size_t build_tb_idx = it_->second[hash_table_idx_].first;
                        size_t build_tuple_idx = it_->second[hash_table_idx_].second;
                        TupleBatch& build_tb = build_tuples_[build_tb_idx];
                        std::vector<Vector> input_vec(build_tb.GetCols().size() + probe_tb_.GetCols().size());
                        for (size_t j = 0; j < build_tb.GetCols().size(); ++j) {
                            input_vec[j] = build_tb.GetCols()[j].Slice(build_tuple_idx, 1);
                        }
                        for (size_t j = 0; j < probe_tb_.GetCols().size(); ++j) {
                            input_vec[j + build_tb.GetCols().size()] = probe_tb_.GetCols()[j].Slice(i, 1);
                        }
                        expr_result_ = Vector(VectorType::Flat, LogicalType::INT, 1);
                        if (expr_.IsValid()) {
                            expr_.Evaluate(input_vec, 1, expr_result_);
                        } else {
                            expr_result_.Set(0, int64_t(1));
                        }
                        if (expr_result_.Get(0).ReadInt() != 0) {
                            ret.Append(input_vec, 0);
                            if (ret.size() == max_batch_size_) {
                                ++hash_table_idx_;
                                return ret;
                            }
                        }
                    }
                    hash_table_idx_ = 0;       
                }
                hash_table_end_ = true; 
            }
            probe_tb_idx_ = 0;
            update_probe();
        }
        return ret;
    }

    virtual size_t GetTotalOutputSize() const override {
        return ch_->GetTotalOutputSize() + ch2_->GetTotalOutputSize() +
                stat_output_size_;
    }

private:
    void update_probe() {
        probe_tb_ = ch2_->Next();
        if (probe_tb_.size() == 0) return;
        right_hash_cols_.clear();
        right_hash_cols_.resize(right_hash_exprs_.size());
        for (size_t i = 0; i < right_hash_exprs_.size(); ++i) {
            right_hash_exprs_[i].Evaluate(probe_tb_.GetCols(), probe_tb_.size(), right_hash_cols_[i]);
        }
        right_hash_vals_.clear();
        right_hash_vals_.resize(probe_tb_.size());
        for (size_t i = 0; i < probe_tb_.size(); ++i) {
            size_t hash_val = 99;
            for (size_t j = 0; j < right_hash_exprs_.size(); ++j) {
                if (right_hash_cols_[j].GetElemType() == LogicalType::STRING) {
                    hash_val = utils::Hash(right_hash_cols_[j].Get(i).ReadString(), hash_val);
                } else {
                    hash_val = utils::Hash8(right_hash_cols_[j].Get(i).ReadInt(), hash_val);
                }
            }
            right_hash_vals_[i] = hash_val;
        }
    }

    std::unique_ptr<VecExecutor> ch_;
    std::unique_ptr<VecExecutor> ch2_;
    OutputSchema output_schema_;
    ExprVecExecutor expr_;
    std::vector<ExprVecExecutor> left_hash_exprs_;
    std::vector<ExprVecExecutor> right_hash_exprs_;
    std::vector<TupleBatch> build_tuples_;
    std::unordered_map<size_t, std::vector<std::pair<size_t, size_t>>> hash_table_;
 
    Vector expr_result_;
    TupleBatch probe_tb_;
    size_t probe_tb_idx_{0};
    size_t hash_table_idx_{0};
    bool hash_table_end_{true};
    std::unordered_map<size_t, std::vector<std::pair<size_t, size_t>>>::iterator it_ = hash_table_.end();

    std::vector<Vector> right_hash_cols_;
    std::vector<size_t> right_hash_vals_;
    size_t right_hash_cols_idx_{0};
};

}