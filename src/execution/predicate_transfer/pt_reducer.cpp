#include "execution/predicate_transfer/pt_reducer.hpp"

#include "common/bloomfilter.hpp"
#include "execution/executor.hpp"
#include "execution/predicate_transfer/pt_vcreator.hpp"
#include "execution/predicate_transfer/pt_vupdater.hpp"

namespace wing {

void PtReducer::Execute() {
  std::map<std::string, size_t> order_map;
  std::vector<std::string> order;
  auto iter = graph_->Graph().begin();
  if (iter == graph_->Graph().end()) return;
  std::function<void(std::string)> dfs = [&](std::string node) {
    order_map[node] = order.size();
    order.push_back(node);
    for (const PtGraph::Edge& edge : graph_->Graph().at(node)) {
      if (order_map.find(edge.to) == order_map.end())
        dfs(edge.to);
    }
  };
  // dfs((graph_->Graph().size() > 1) ? (++iter)->first : iter->first);
  for (const auto& table : graph_->Graph()) {
    order.push_back(table.first);
  }
  const auto cmp = [&](const std::string& table_1, const std::string& table_2) {
    return graph_->Graph().at(table_1).size() < graph_->Graph().at(table_2).size();
  };
  std::sort(order.begin(), order.end(), cmp);
  for (size_t i = 0; i < order.size(); ++i) {
    order_map[order[i]] = i;
  }

  std::map<std::string, std::vector<std::string>> bloom_filters;
  std::map<std::string, std::vector<size_t>> bf_maps;
  std::map<std::string, std::map<size_t, size_t>> edge_id_order_map;
  for (const auto& table : order) {
    size_t count = 0;
    edge_id_order_map[table] = std::map<size_t, size_t>();
    for (const auto& edge : graph_->Graph().at(table)) {
      edge_id_order_map[table][edge.id]= count;
      count++;
    }
  }

  for (const auto& table : order) {
    result_bv_[table] = static_cast<SeqScanPlanNode*>(graph_->GetTableScanPlans().at(table).get())->valid_bits_;
    (*result_bv_[table])[0] = 1;
    auto bfs = bloom_filters.find(table);
    auto proj_plan = std::make_unique<ProjectPlanNode>();
    std::vector<std::unique_ptr<Expr>> exprs;
    for (const PtGraph::Edge& edge : graph_->Graph().at(table)) {
      exprs.push_back(edge.pred_from->clone());  
    }
    for (auto& expr : exprs) {
      proj_plan->output_exprs_.push_back(expr->clone());
      proj_plan->output_schema_.Append(
          OutputColumnData{0, "", "a", expr->ret_type_, 0});
    }
    proj_plan->ch_ = std::move(graph_->GetTableScanPlans().at(table)->clone());
    if (bfs != bloom_filters.end()) {
      auto exe = ExecutorGenerator::GenerateVec(proj_plan.get(), db_, txn_id_);
      PtVecUpdater vec_update(std::move(exe), proj_plan->output_schema_.GetCols().size());
      vec_update.Execute(bfs->second, *result_bv_[table], bf_maps[table]);
    }
    auto exe = ExecutorGenerator::GenerateVec(proj_plan.get(), db_, txn_id_);
    PtVecCreator vec_create(n_bits_per_key_, std::move(exe), proj_plan->output_schema_.GetCols().size());
    vec_create.Execute();
    const std::vector<std::string>& bf = vec_create.GetResult();
    for (size_t j = 0; j < graph_->Graph().at(table).size(); ++j) {
      const auto& edge = graph_->Graph().at(table)[j];
      std::string dst = edge.to;
      if (order_map[dst] < order_map[table]) continue;
      if (bloom_filters.find(dst) == bloom_filters.end()) {
        bloom_filters[dst] = std::vector<std::string>();
        bf_maps[dst] = std::vector<size_t>();
      }
      bloom_filters[dst].push_back(bf[j]);
      bf_maps[dst].push_back(edge_id_order_map[dst][edge.id]);
    }
  }

  std::map<std::string, std::vector<std::string>> bloom_filters_backward;
  std::map<std::string, std::vector<size_t>> bf_maps_backward;
  for (int i = order.size() - 1; i >= 0; --i) {
    const std::string& table = order[i];
    auto bfs = bloom_filters_backward.find(table);
    auto proj_plan = std::make_unique<ProjectPlanNode>();
    std::vector<std::unique_ptr<Expr>> exprs;
    for (const PtGraph::Edge& edge : graph_->Graph().at(table)) {
      exprs.push_back(edge.pred_from->clone());
    }
    for (auto& expr : exprs) {
      proj_plan->output_exprs_.push_back(expr->clone());
      proj_plan->output_schema_.Append(
          OutputColumnData{0, "", "a", expr->ret_type_, 0});
    }
    proj_plan->ch_ = std::move(graph_->GetTableScanPlans().at(table)->clone());
    proj_plan->type_ = PlanType::Project;
    if (bfs != bloom_filters_backward.end()) {
      auto exe = ExecutorGenerator::GenerateVec(proj_plan.get(), db_, txn_id_);
      PtVecUpdater vec_update(std::move(exe), proj_plan->output_schema_.GetCols().size());
      vec_update.Execute(bfs->second, *result_bv_[table], bf_maps_backward[table]);
    }
    auto exe = ExecutorGenerator::GenerateVec(proj_plan.get(), db_, txn_id_);
    PtVecCreator vec_create(n_bits_per_key_, std::move(exe), proj_plan->output_schema_.GetCols().size());
    vec_create.Execute();
    const std::vector<std::string>& bf = vec_create.GetResult();
    for (size_t j = 0; j < graph_->Graph().at(table).size(); ++j) {
      const auto& edge = graph_->Graph().at(table)[j];
      std::string dst = edge.to;
      if (order_map[dst] > order_map[table]) continue;
      if (bloom_filters_backward.find(dst) == bloom_filters_backward.end()) {
        bloom_filters_backward[dst] = std::vector<std::string>();
        bf_maps_backward[dst] = std::vector<size_t>();
      }
      bloom_filters_backward[dst].push_back(bf[j]);
      bf_maps_backward[dst].push_back(edge_id_order_map[dst][edge.id]);
    }
  }
}

std::vector<std::string> PtReducer::GenerateFilter(
    const std::string& table, const std::vector<const Expr*>& exprs) {
  DB_ERR("Not implemented!");
}

void PtReducer::PredicateTransfer(const std::string& table,
    const std::vector<const Expr*>& exprs,
    const std::vector<std::string>& filters) {
  DB_ERR("Not implemented!");
}

}  // namespace wing
