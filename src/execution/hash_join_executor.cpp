//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

// Note for 2022 Fall: You don't need to implement HashJoinExecutor to pass all tests. You ONLY need to implement it
// if you want to get faster in leaderboard tests.

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  Tuple tmp_tuple{};
  RID rid;
  /**
   * 初始化哈希表，注意这里只把右表的key value存入哈希表
   * 因为左表需要遍历，并且保证顺序
   */
  auto &right_output_schema = plan_->GetRightPlan()->OutputSchema();
  auto &left_output_schema = plan_->GetLeftPlan()->OutputSchema();
  while (right_executor_->Next(&tmp_tuple, &rid)) {
    auto key = plan_->RightJoinKeyExpression().Evaluate(&tmp_tuple, right_output_schema);
    hash_join_table_[HashUtil::HashValue(&key)].push_back(tmp_tuple);
  }

  while (left_executor_->Next(&tmp_tuple, &rid)) {
    // 计算左表的key
    auto join_key = plan_->LeftJoinKeyExpression().Evaluate(&tmp_tuple, left_output_schema);
    auto left_col_count = left_output_schema.GetColumnCount();
    auto right_col_count = right_output_schema.GetColumnCount();

    if (hash_join_table_.count(HashUtil::HashValue(&join_key)) > 0) {
      auto &right_tuples = hash_join_table_[HashUtil::HashValue(&join_key)];
      for (const auto &tuple : right_tuples) {
        auto right_join_key = plan_->RightJoinKeyExpression().Evaluate(&tuple, right_output_schema);
        // 防止出现hash相同，值不同的情况
        if (right_join_key.CompareEquals(join_key) == CmpBool::CmpTrue) {
          std::vector<Value> values;
          values.reserve(left_col_count + right_col_count);
          for (uint32_t i = 0; i < left_col_count; i++) {
            values.push_back(tmp_tuple.GetValue(&left_output_schema, i));
          }
          for (uint32_t i = 0; i < right_col_count; i++) {
            values.push_back(tuple.GetValue(&right_output_schema, i));
          }
          output_tuples_.emplace_back(values, &GetOutputSchema());
        }
      }
    } else if (plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      values.reserve(left_col_count + right_col_count);
      for (uint32_t i = 0; i < left_col_count; i++) {
        values.push_back(tmp_tuple.GetValue(&left_output_schema, i));
      }
      for (uint32_t i = 0; i < right_col_count; i++) {
        values.push_back(ValueFactory::GetNullValueByType(right_output_schema.GetColumn(i).GetType()));
      }
      output_tuples_.emplace_back(values, &GetOutputSchema());
    }
  }

  output_tuples_iter_ = output_tuples_.cbegin();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (output_tuples_iter_ != output_tuples_.cend()) {
    *tuple = *output_tuples_iter_;
    output_tuples_iter_++;
    return true;
  }
  return false;
}

}  // namespace bustub
