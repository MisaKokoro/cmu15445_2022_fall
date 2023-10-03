#include "execution/executors/topn_executor.h"
#include <queue>

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  /**
   * 这里的cmp实际的实现与sort保持了一致，但是优先队列先要输出升序输出所有的数
   * 判断条件应该与sort是相反的，
   * 这里是因为如果想要TopK大的数据，应该使用小根堆实现
   * 这样一来 order by v1 dec limit k 就转换为了前k个最大的数据
   * 这样就应该使用小根堆 小根堆的判断条件正好是 return a > b 而dec的判断条件也是 return a > b
   * 这样一来cmp的判断条件就与sort保持一致了
   */
  auto cmp = [order_bys = plan_->GetOrderBy(), schema = child_executor_->GetOutputSchema()](const Tuple &a,
                                                                                            const Tuple &b) {
    for (const auto &[type, expr] : order_bys) {
      auto value_a = expr->Evaluate(&a, schema);
      auto value_b = expr->Evaluate(&b, schema);
      switch (type) {
        case OrderByType::INVALID:
        case OrderByType::DEFAULT:
        case OrderByType::ASC:
          if (value_a.CompareLessThan(value_b) == CmpBool::CmpTrue) {
            return true;
          } else if (value_a.CompareGreaterThan(value_b) == CmpBool::CmpTrue) {
            return false;
          }
          break;
        case OrderByType::DESC:
          if (value_a.CompareGreaterThan(value_b) == CmpBool::CmpTrue) {
            return true;
          } else if (value_a.CompareLessThan(value_b) == CmpBool::CmpTrue) {
            return false;
          }
          break;
      }
    }
    return false;
  };
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> q(cmp);
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    q.push(tuple);
    if (q.size() > plan_->GetN()) {
      q.pop();
    }
  }

  while (!q.empty()) {
    child_tuples_.push(q.top());
    q.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!child_tuples_.empty()) {
    *tuple = child_tuples_.top();
    child_tuples_.pop();
    return true;
  }
  return false;
}

}  // namespace bustub
