#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  sorted_tuples_.clear();
  RID rid;
  Tuple tuple;
  while (child_executor_->Next(&tuple, &rid)) {
    sorted_tuples_.push_back(tuple);
  }

  sort(sorted_tuples_.begin(), sorted_tuples_.end(), [this](const Tuple &a, const Tuple &b) {
    for (auto [type, expr] : plan_->GetOrderBy()) {
      // 判断是否为升序
      bool asc_group_by = (type == OrderByType::DEFAULT || type == OrderByType::ASC);
      Value value_a = expr->Evaluate(&a, child_executor_->GetOutputSchema());
      Value value_b = expr->Evaluate(&b, child_executor_->GetOutputSchema());
      /**
       * 注意这里相等的情况不处理，因为可能有多个比较条件，
       * 留到下个比较条件处理
       */
      if (asc_group_by) {
        if (value_a.CompareLessThan(value_b) == CmpBool::CmpTrue) {
          return true;
        }
        if (value_a.CompareGreaterThan(value_b) == CmpBool::CmpTrue) {
          return false;
        }
      } else {
        if (value_a.CompareGreaterThan(value_b) == CmpBool::CmpTrue) {
          return true;
        }
        if (value_a.CompareLessThan(value_b) == CmpBool::CmpTrue) {
          return false;
        }
      }
    }
    // 全部相等的时候返回false，这样不改变原本的数组
    return false;
  });

  iterator_ = sorted_tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iterator_ != sorted_tuples_.end()) {
    *tuple = *iterator_;
    iterator_++;
    return true;
  }
  return false;
}
}  // namespace bustub
