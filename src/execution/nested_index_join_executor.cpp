//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  is_left_ = (plan_->GetJoinType() == JoinType::LEFT);
  /*索引信息和表信息，child是左表，右表是当前，并且有索引*/
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
}

void NestIndexJoinExecutor::Init() { child_executor_->Init(); }

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple left_tuple;
  RID lef_rid;
  /*
   * 1. 首先从child也就是左表，Next获取一个tupleA
   * 2. 从tupleA中获取key需要的几列，转换为key，这里有个隐含的信息是keyA = keyB，但是tupleA不一定等于tupleB
   * 3. 去索引中查是否有这个key对应的tupleB
   * */
  while (child_executor_->Next(&left_tuple, &lef_rid)) {
    /*扫描左表，然后直接查索引*/
    auto key_schema = index_info_->index_->GetKeySchema();
    /*这个tuple对应的schema*/
    auto value = plan_->KeyPredicate()->Evaluate(&left_tuple, child_executor_->GetOutputSchema());
    std::vector<Value> values;
    values.push_back(value);
    Tuple key(values, key_schema);
    std::vector<RID> results;
    index_info_->index_->ScanKey(key, &results, exec_ctx_->GetTransaction());
    /*如果匹配上了因为keyB不存在重复，所以如果能匹配，肯定匹配一次就结束。inner*/
    if (!results.empty()) {
      for (auto rid_b : results) {
        Tuple right_tuple;  // 对于每个rid，可以通过catalog获得对应的tuple，如果tuple存在
        if (table_info_->table_->GetTuple(rid_b, &right_tuple, exec_ctx_->GetTransaction())) {
          std::vector<Value> tuple_values;
          for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
            tuple_values.push_back(left_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
          }
          for (uint32_t i = 0; i < table_info_->schema_.GetColumnCount(); i++) {
            tuple_values.push_back(right_tuple.GetValue(&table_info_->schema_, i));
          }
          *tuple = {tuple_values, &plan_->OutputSchema()};
          return true;
        }
      }
    }
    /*右表没有元素时，并且是left join，则需要填null
     * 如果是inner join，没有任何行匹配，则不用管，直接忽略
     * */
    if (is_left_) {
      std::vector<Value> tuple_values;
      for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
        tuple_values.push_back(left_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < table_info_->schema_.GetColumnCount(); i++) {
        tuple_values.push_back(ValueFactory::GetNullValueByType(table_info_->schema_.GetColumn(i).GetType()));
      }
      *tuple = {tuple_values, &plan_->OutputSchema()};
      return true;
    }
  }
  return false;
}
}  // namespace bustub
