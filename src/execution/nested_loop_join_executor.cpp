#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      left_schema_(left_executor_->GetOutputSchema()),
      right_schema_(right_executor_->GetOutputSchema()) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  is_ineer_ = (plan_->GetJoinType() == JoinType::INNER);
}

void NestedLoopJoinExecutor::Init() {
  Tuple tuple;
  RID rid;
  left_executor_->Init();
  right_executor_->Init();
  /*先把右边的tuple缓存起来*/
  while (right_executor_->Next(&tuple, &rid)) {
    right_tuples_.push_back(tuple);
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::vector<Column> columns(left_schema_.GetColumns());
  for (const auto &column : right_schema_.GetColumns()) {
    columns.push_back(column);
  }
  /*总的属性是两个表的属性水平拼接起来*/
  Schema schema(columns);
  if (is_ineer_) {
    return InnerJoin(schema, tuple);
  }
  return LeftJoin(schema, tuple);
}

auto NestedLoopJoinExecutor::InnerJoin(const Schema &schema, Tuple *tuple) -> bool {
  if (index_ > right_tuples_.size()) {
    return false;
  }
  /* 上一次的left_tuple没有和right_tuple匹配完时，先处理把上次的结果处理完，再调用Next获取下一个left_tuple
   * 如果right_tuples总共有7条
   * 可能这里要执行6次
   * */
  if (index_ != 0) {
    /*注意是从index开始*/
    for (uint32_t j = index_; j < right_tuples_.size(); j++) {
      index_ = (index_ + 1) % right_tuples_.size();
      if (plan_->Predicate().EvaluateJoin(&left_tuple_, left_schema_, &right_tuples_[j], right_schema_).GetAs<bool>()) {
        std::vector<Value> value;
        for (uint32_t i = 0; i < left_schema_.GetColumnCount(); i++) {
          value.push_back(left_tuple_.GetValue(&left_schema_, i));
        }
        for (uint32_t i = 0; i < right_schema_.GetColumnCount(); i++) {
          value.push_back(right_tuples_[j].GetValue(&right_schema_, i));
        }
        *tuple = {value, &schema};
        return true;
      }
    }
  }
  if (index_ == 0) {
    /*每次获取一个left元素，right缓存全部数据*/
    while (left_executor_->Next(&left_tuple_, &left_rid_)) {
      for (const auto &right_tuple : right_tuples_) {
        /*索引*/
        index_ = (index_ + 1) % right_tuples_.size();
        if (plan_->Predicate().EvaluateJoin(&left_tuple_, left_schema_, &right_tuple, right_schema_).GetAs<bool>()) {
          std::vector<Value> value;
          for (uint32_t i = 0; i < left_schema_.GetColumnCount(); i++) {
            value.push_back(left_tuple_.GetValue(&left_schema_, i));
          }
          for (uint32_t i = 0; i < right_schema_.GetColumnCount(); i++) {
            value.push_back(right_tuple.GetValue(&right_schema_, i));
          }
          /*注意：这里是在for循环内部返回的合并后的tuple信息，确实没有遍历完，
          可能for循环right_tuple有10条，但是只合并了一条，就返回了，这个时候剩下的九条，
          就需要在下次进入函数时，先返回*/
          *tuple = {value, &schema};
          return true;
        }
      }
    }
    index_ = right_tuples_.size() + 1;
  }

  return false;
}

auto NestedLoopJoinExecutor::LeftJoin(const Schema &schema, Tuple *tuple) -> bool {
  if (index_ > right_tuples_.size()) {
    return false;
  }
  if (index_ != 0) {
    for (uint32_t j = index_; j < right_tuples_.size(); j++) {
      index_ = (index_ + 1) % right_tuples_.size();
      if (plan_->Predicate().EvaluateJoin(&left_tuple_, left_schema_, &right_tuples_[j], right_schema_).GetAs<bool>()) {
        std::vector<Value> value;
        for (uint32_t i = 0; i < left_schema_.GetColumnCount(); i++) {
          value.push_back(left_tuple_.GetValue(&left_schema_, i));
        }
        for (uint32_t i = 0; i < right_schema_.GetColumnCount(); i++) {
          value.push_back(right_tuples_[j].GetValue(&right_schema_, i));
        }
        is_match_ = true;
        *tuple = {value, &schema};
        return true;
      }
    }
  }
  if (index_ == 0) {
    while (left_executor_->Next(&left_tuple_, &left_rid_)) {
      is_match_ = false;
      for (const auto &right_tuple : right_tuples_) {
        index_ = (index_ + 1) % right_tuples_.size();
        if (plan_->Predicate().EvaluateJoin(&left_tuple_, left_schema_, &right_tuple, right_schema_).GetAs<bool>()) {
          std::vector<Value> value;
          for (uint32_t i = 0; i < left_schema_.GetColumnCount(); i++) {
            value.push_back(left_tuple_.GetValue(&left_schema_, i));
          }
          for (uint32_t i = 0; i < right_schema_.GetColumnCount(); i++) {
            value.push_back(right_tuple.GetValue(&right_schema_, i));
          }
          is_match_ = true;
          *tuple = {value, &schema};
          return true;
        }
      }
      /*右表为空和没有任何匹配的情况*/
      /*如果跟右边没有任何一行能匹配，则需要构造一个空tuple来join*/
      if (!is_match_) {
        std::vector<Value> value;

        for (uint32_t i = 0; i < left_schema_.GetColumnCount(); i++) {
          value.push_back(left_tuple_.GetValue(&left_schema_, i));
        }
        /*右边全部填空值*/
        for (uint32_t i = 0; i < right_schema_.GetColumnCount(); i++) {
          value.push_back(ValueFactory::GetNullValueByType(right_schema_.GetColumn(i).GetType()));
        }
        *tuple = {value, &schema};
        is_match_ = true;
        return true;
      }
    }
    /*左边遍历完之后，不再进入后面的逻辑，直接在函数入口处返回false*/
    index_ = right_tuples_.size() + 1;
  }
  return false;
}

}  // namespace bustub
