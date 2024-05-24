#pragma once

#include "execution/executor.hpp"
#include <iostream>

namespace wing {

class JoinVecExecutor : public VecExecutor {
public:
    JoinVecExecutor(const ExecOptions& options,
        const std::unique_ptr<Expr>& expr, OutputSchema output_schema,
        std::unique_ptr<VecExecutor> ch, std::unique_ptr<VecExecutor> ch2): 
    VecExecutor(options), ch_(std::move(ch)), ch2_(std::move(ch2)), output_schema_(output_schema) {
        max_batch_size_ = options.max_batch_size;
        expr_ = ExprVecExecutor::Create(expr.get(), output_schema_);
    }
    void Init() {
        ch_->Init();
        ch2_->Init();
        build_tuples_.clear();
        for (TupleBatch tb = ch_->Next(); tb.size() > 0; tb = ch_->Next()) {
            TupleBatch tb2;
            tb2.Init(tb);
            build_tuples_.push_back(tb2);
        }
        rest_tuple_batch_.Init(output_schema_.GetTypes(), max_batch_size_);
        probe_tb_ = ch2_->Next();
    }

    ~JoinVecExecutor() noexcept override {}

    TupleBatch InternalNext() {
        TupleBatch ret;
        size_t ret_size = 0;
        ret.Init(output_schema_.GetTypes(), max_batch_size_);
        for (; probe_tb_.size() > 0; probe_tb_ = ch2_->Next()) {
            for (; rest_tuple_batch_idx_ < rest_tuple_batch_.size(); rest_tuple_batch_idx_++) {
                if (expr_result_.Get(rest_tuple_batch_idx_).ReadInt() != 0) {
                    ret.Append(rest_tuple_batch_.GetSingleTuple(rest_tuple_batch_idx_));
                    ++ret_size;
                    if (ret_size == max_batch_size_) {
                        return ret;
                    }
                }
            }
            for (; build_tuple_idx_ < build_tuples_.size(); build_tuple_idx_++) {
                size_t i = build_tuple_idx_;
                TupleBatch build_tb = build_tuples_[i];
                for (; build_single_tuple_idx_ < build_tuples_[build_tuple_idx_].size(); build_single_tuple_idx_++) {
                    size_t j = build_single_tuple_idx_;
                    TupleBatch tmp_tb;
                    tmp_tb.Init(probe_tb_.GetColElemTypes(), probe_tb_.Capacity());
                    for (size_t k = 0; k < probe_tb_.size(); k++) {
                        tmp_tb.Append(build_tb.GetSingleTuple(j));
                    }
                    std::vector<Vector> cols = tmp_tb.GetCols();
                    cols.insert(cols.end(), probe_tb_.GetCols().begin(), probe_tb_.GetCols().end());
                    rest_tuple_batch_.Init(cols, probe_tb_.size(), probe_tb_.GetSelVector());
                    expr_.Evaluate(rest_tuple_batch_.GetCols(), rest_tuple_batch_.size(), expr_result_);
                    // std::cout << "expr size: " << expr_result_.size() << " expr: " << expr_result_.Get(0).ReadInt() << " rtbs: " << rest_tuple_batch_.ValidSize() << "\n";
                    for (rest_tuple_batch_idx_ = 0; rest_tuple_batch_idx_ < rest_tuple_batch_.size(); ++rest_tuple_batch_idx_) {
                        if (!rest_tuple_batch_.IsValid(rest_tuple_batch_idx_)) continue;
                        if (expr_result_.Get(rest_tuple_batch_idx_).ReadInt() != 0) {
                            ret.Append(rest_tuple_batch_.GetSingleTuple(rest_tuple_batch_idx_));
                            ++ret_size;
                            if (ret_size == max_batch_size_) {
                                build_single_tuple_idx_++;
                                rest_tuple_batch_idx_++;
                                return ret;
                            }
                        }
                    }
                }
                build_single_tuple_idx_ = 0;
                // std::cout << build_tuple_idx_ << std::endl;
            }
            build_tuple_idx_ = 0;
        }
        return ret;
    }

private:
    ExprVecExecutor expr_;
    std::vector<TupleBatch> build_tuples_;
    OutputSchema output_schema_;
    std::unique_ptr<VecExecutor> ch_;
    std::unique_ptr<VecExecutor> ch2_;

    size_t build_tuple_idx_{0};
    size_t build_single_tuple_idx_{0};  
    TupleBatch rest_tuple_batch_;
    Vector expr_result_;
    size_t rest_tuple_batch_idx_{0};  
    TupleBatch probe_tb_;
    bool probe_end_{true};
};

}