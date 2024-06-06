#include "plan/predicate_transfer/pt_graph.hpp"

namespace wing {

PtGraph::PtGraph(const PlanNode* plan) { Dfs(plan); }

void PtGraph::Dfs(const PlanNode* plan) {
    graph_.clear();
    table_scan_plans_.clear();
    size_t id = 0;
    std::function<void(PlanNode*)> extract = [&](PlanNode* node) {
        if (node->type_ == PlanType::SeqScan) {
            std::string alias = static_cast<SeqScanPlanNode*>(node)->table_name_in_sql_;
            table_scan_plans_[alias] = node->clone();
        } else if (node->type_ == PlanType::Join || node->type_ == PlanType::HashJoin) {
            PredicateVec* pred_vec;
            if (node->type_ == PlanType::Join) {
                pred_vec = &static_cast<JoinPlanNode*>(node)->predicate_;
            } else {
                pred_vec = &static_cast<HashJoinPlanNode*>(node)->predicate_;
            }
            for (auto& pred : pred_vec->GetVec()) {
                if (!pred.IsEq()) continue;
                auto L = pred.GetLeftTableName();
                auto R = pred.GetRightTableName();
                if (L && R && L.value().size() > 0 && R.value().size() > 0) {
                    auto edge_1 = Edge(L.value(), R.value(), pred.GetLeftExpr()->clone(), pred.GetRightExpr()->clone(), id);
                    auto edge_2 = Edge(R.value(), L.value(), pred.GetRightExpr()->clone(), pred.GetLeftExpr()->clone(), id);
                    ++id;
                    if (graph_.find(L.value()) == graph_.end()) {
                        graph_[L.value()] = std::vector<Edge>();
                    }
                    if (graph_.find(R.value()) == graph_.end()) {
                        graph_[R.value()] = std::vector<Edge>();
                    }
                    graph_[L.value()].emplace_back(std::move(edge_1));
                    graph_[R.value()].emplace_back(std::move(edge_2));
                }
            }
            extract(node->ch_.get());
            extract(node->ch2_.get());
        }
    };
    extract(plan->ch_.get());
}

}  // namespace wing
