#include <queue>

#include "plan/optimizer.hpp"
#include "plan/predicate_transfer/pt_graph.hpp"
#include "rules/convert_to_hash_join.hpp"

namespace wing {

std::unique_ptr<PlanNode> Apply(std::unique_ptr<PlanNode> plan,
    const std::vector<std::unique_ptr<OptRule>>& rules, const DB& db) {
  for (auto& a : rules) {
    if (a->Match(plan.get())) {
      plan = a->Transform(std::move(plan));
      break;
    }
  }
  if (plan->ch2_ != nullptr) {
    plan->ch_ = Apply(std::move(plan->ch_), rules, db);
    plan->ch2_ = Apply(std::move(plan->ch2_), rules, db);
  } else if (plan->ch_ != nullptr) {
    plan->ch_ = Apply(std::move(plan->ch_), rules, db);
  }
  return plan;
}

size_t GetTableNum(const PlanNode* plan) {
  /* We don't want to consider values clause in cost based optimizer. */
  if (plan->type_ == PlanType::Print) {
    return 10000;
  }

  if (plan->type_ == PlanType::SeqScan) {
    return 1;
  }

  size_t ret = 0;
  if (plan->ch2_ != nullptr) {
    ret += GetTableNum(plan->ch_.get());
    ret += GetTableNum(plan->ch2_.get());
  } else if (plan->ch_ != nullptr) {
    ret += GetTableNum(plan->ch_.get());
  }
  return ret;
}

bool CheckIsAllJoin(const PlanNode* plan) {
  if (plan->type_ == PlanType::Print || plan->type_ == PlanType::SeqScan ||
      plan->type_ == PlanType::RangeScan) {
    return true;
  }
  if (plan->type_ != PlanType::Join) {
    return false;
  }
  return CheckIsAllJoin(plan->ch_.get()) && CheckIsAllJoin(plan->ch2_.get());
}

bool CheckHasStat(const PlanNode* plan, const DB& db) {
  if (plan->type_ == PlanType::Print) {
    return false;
  }
  if (plan->type_ == PlanType::SeqScan) {
    auto stat =
        db.GetTableStat(static_cast<const SeqScanPlanNode*>(plan)->table_name_);
    return stat != nullptr;
  }
  if (plan->type_ == PlanType::RangeScan) {
    auto stat = db.GetTableStat(
        static_cast<const RangeScanPlanNode*>(plan)->table_name_);
    return stat != nullptr;
  }
  if (plan->type_ != PlanType::Join) {
    return false;
  }
  return CheckHasStat(plan->ch_.get(), db) &&
         CheckHasStat(plan->ch2_.get(), db);
}

/**
 * Check whether we can use cost based optimizer.
 * For simplicity, we only use cost based optimizer when:
 * (1) The root plan node is Project, and there is only one Project.
 * (2) The other plan nodes can only be Join or SeqScan or RangeScan.
 * (3) The number of tables is <= 20.
 * (4) All tables have statistics or true cardinality is provided.
 */
bool CheckCondition(const PlanNode* plan, const DB& db) {
  if (GetTableNum(plan) > 20)
    return false;
  if (plan->type_ != PlanType::Project && plan->type_ != PlanType::Aggregate)
    return false;
  if (!CheckIsAllJoin(plan->ch_.get()))
    return false;
  return db.GetOptions().optimizer_options.true_cardinality_hints ||
         CheckHasStat(plan->ch_.get(), db);
}

std::unique_ptr<PlanNode> CostBasedOptimizer::Optimize(
    std::unique_ptr<PlanNode> plan, DB& db) {
  if (CheckCondition(plan.get(), db) &&
      db.GetOptions().optimizer_options.enable_cost_based) {
    // std::vector<std::unique_ptr<OptRule>> R;
    // R.push_back(std::make_unique<ConvertToHashJoinRule>());
    // plan = Apply(std::move(plan), R, db);
    // TODO...
    std::vector<std::unique_ptr<PlanNode>> nodes;
    std::vector<PredicateVec> predicates;
    std::function<void(std::unique_ptr<PlanNode>)> extract = [&](const std::unique_ptr<PlanNode>& node) {
      if (node->type_ == PlanType::SeqScan || node->type_ == PlanType::RangeScan) {
        nodes.push_back(node->clone());
      } else {
        predicates.emplace_back(static_cast<JoinPlanNode*>(node.get())->predicate_.clone());
        extract(node->ch_->clone());
        extract(node->ch2_->clone());
      }
    };
    extract(plan->ch_->clone());
    const std::vector<std::pair<std::vector<std::string>, double>> true_cardinality = db.GetOptions().optimizer_options.true_cardinality_hints.value();
    const double hash_cost = db.GetOptions().optimizer_options.hash_join_cost;
    const double scan_cost = db.GetOptions().optimizer_options.scan_cost;
    std::map<std::string, size_t> order_map;
    std::vector<size_t> bitset_map(1 << nodes.size());
    for (size_t i = 0; i < std::log2(true_cardinality.size()); i++) {
      order_map[true_cardinality[1 << i].first[0]] = 1 << i;
    }
    for (auto& iter : order_map) {
      // DB_INFO("table name: {}, order: {}", iter.first, iter.second);
    }
    for (size_t i = 0; i < nodes.size(); i++) {
      const auto name = static_cast<SeqScanPlanNode*>(nodes[i].get())->table_name_;
      bitset_map[1 << i] = order_map[name];
      // DB_INFO("table name: {}", name);
    }

    const auto list_elements = [](size_t S) {
      std::vector<size_t> ret;
      for (size_t i = 0; i < 64; i++) {
        if ((S >> i) & 1) {
          ret.push_back(i);
        }
      }
      return ret;
    };

    std::vector<std::pair<size_t, size_t>> opt_plans(1 << nodes.size(), {0, 0});
    std::vector<double> costs(1 << nodes.size(), 0);
    std::vector<size_t> enable_hash_join(1 << nodes.size(), 0);
    std::vector<BitVector> bitsets(1 << nodes.size(), 0);
    std::vector<PredicateVec> relative_predicates(1 << nodes.size());
    for (size_t S = 1; S < (1 << nodes.size()); S++) {
      std::vector<size_t> elements = list_elements(S);
      if (elements.size() == 1) {
        opt_plans[S] = {S, 0};
        bitsets[S] = nodes[elements[0]]->table_bitset_;
        // DB_INFO("Bitset for {} is {}", S, bitsets[S].ToString())
        costs[S] = scan_cost * true_cardinality[bitset_map[S]].second;
        continue;
      }
      bitsets[S] = bitsets[(S - 1) & S] | bitsets[S ^ ((S - 1) & S)];
      bitset_map[S] = bitset_map[(S - 1) & S] | bitset_map[S ^ ((S - 1) & S)];
      double cost = std::numeric_limits<double>::max();
      PredicateVec rela_preds;
      for (auto& pred : predicates) {
        for (auto& a : pred.GetVec()) {
          if (a.CheckLeftIntersection(bitsets[S]) && a.CheckRightIntersection(bitsets[S])) {
            rela_preds.Append(PredicateElement(pred._trans(a.expr_->clone()), a.left_bits_, a.right_bits_));
          }
        }
      }
      std::pair<size_t, size_t> best_plan = {0, 0};
      for (size_t T = (S - 1) & S; T != 0; T = (T - 1) & S) {
        size_t U = S ^ T;
        size_t hashable = 0;
        for (auto&& a : rela_preds.GetVec()) {
          const BitVector L = bitsets[T];
          const BitVector R = bitsets[U];
          // only equal condition can use it
          if (a.expr_->op_ == OpType::EQ) {
            // CheckLeft checks if L has intersection with tables used in left expression.
            // CheckRight checks if R has intersection with tables used in right expression.
            if (!a.CheckRight(L) && !a.CheckLeft(R) && a.CheckRight(R) &&
                a.CheckLeft(L)) {
              hashable = 1;
              break;
            }
            if (!a.CheckLeft(L) && !a.CheckRight(R) && a.CheckRight(L) &&
                a.CheckLeft(R)) {
              hashable = 1;
              break;
            }
          }
        }
        double tmp_cost = 0;
        if (hashable > 0) {
          auto tmp_cost_hash = hash_cost * (true_cardinality[bitset_map[T]].second + true_cardinality[bitset_map[U]].second) + scan_cost * true_cardinality[bitset_map[S]].second;
          auto tmp_cost_no_hash = scan_cost * true_cardinality[bitset_map[T]].second * true_cardinality[bitset_map[U]].second;
          if (tmp_cost_hash < tmp_cost_no_hash) {
            tmp_cost = tmp_cost_hash;
          } else {
            tmp_cost = tmp_cost_no_hash;
            hashable = 0;
          }
        } else {
          tmp_cost = scan_cost * true_cardinality[bitset_map[T]].second * true_cardinality[bitset_map[U]].second;
        }
        tmp_cost += costs[T] + costs[U];
        if (tmp_cost < cost) {
          cost = tmp_cost;
          best_plan = {T, U};
          enable_hash_join[S] = hashable;
        }
      }
      opt_plans[S] = best_plan;
      costs[S] = cost;
      // DB_INFO("cost: {}", cost);
      relative_predicates[S] = rela_preds.clone();
    }

    std::function<std::unique_ptr<PlanNode>(size_t)> build_plan_tree = [&](size_t S) {
      size_t lc = opt_plans[S].first;
      size_t rc = opt_plans[S].second;
      if (rc == 0) {
        return nodes[list_elements(S)[0]]->clone();
      }
      const size_t hash_join = enable_hash_join[S];
      if (hash_join == 0) {
        auto join_plan = std::make_unique<JoinPlanNode>();
        join_plan->table_bitset_ = bitsets[S];
        join_plan->predicate_ = relative_predicates[S].clone();
        join_plan->ch_ = build_plan_tree(lc);
        join_plan->ch2_ = build_plan_tree(rc);
        join_plan->output_schema_ = OutputSchema::Concat(join_plan->ch_->output_schema_, join_plan->ch2_->output_schema_);
        join_plan->cost_ = costs[S];
        return join_plan->clone(); 
      } else {
        auto hash_join_plan = std::make_unique<HashJoinPlanNode>();
        hash_join_plan->table_bitset_ = bitsets[S];
        hash_join_plan->predicate_ = relative_predicates[S].clone();
        for (auto&& a : hash_join_plan->predicate_.GetVec()) {
          if (a.expr_->op_ == OpType::EQ) {
            if (a.CheckRight(bitsets[rc]) &&
                a.CheckLeft(bitsets[lc])) {
              hash_join_plan->left_hash_exprs_.push_back(a.expr_->ch0_->clone());
              hash_join_plan->right_hash_exprs_.push_back(a.expr_->ch1_->clone());
            } else if (a.CheckRight(bitsets[lc]) &&
                      a.CheckLeft(bitsets[rc])) {
              hash_join_plan->right_hash_exprs_.push_back(a.expr_->ch0_->clone());
              hash_join_plan->left_hash_exprs_.push_back(a.expr_->ch1_->clone());
            }
          }
        }
        hash_join_plan->ch_ = build_plan_tree(lc);
        hash_join_plan->ch2_ = build_plan_tree(rc);
        hash_join_plan->output_schema_ = OutputSchema::Concat(hash_join_plan->ch_->output_schema_, hash_join_plan->ch2_->output_schema_);
        hash_join_plan->cost_ = costs[S];
        return hash_join_plan->clone();
      }
    };
    plan->ch_ = build_plan_tree((1 << nodes.size()) - 1);
    plan->cost_ = costs[(1 << nodes.size()) - 1];
  } else {
    std::vector<std::unique_ptr<OptRule>> R;
    R.push_back(std::make_unique<ConvertToHashJoinRule>());
    plan = Apply(std::move(plan), R, db);
  }
  if (db.GetOptions().exec_options.enable_predicate_transfer) {
    if (plan->type_ != PlanType::Insert && plan->type_ != PlanType::Delete &&
        plan->type_ != PlanType::Update) {
      auto pt_plan = std::make_unique<PredicateTransferPlanNode>();
      pt_plan->graph_ = std::make_shared<PtGraph>(plan.get());
      pt_plan->output_schema_ = plan->output_schema_;
      pt_plan->table_bitset_ = plan->table_bitset_;
      pt_plan->ch_ = std::move(plan);
      plan = std::move(pt_plan);
    }
  }
  return plan;
}

}  // namespace wing
