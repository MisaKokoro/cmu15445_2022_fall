
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      /* Q: aht_哈希表入参说明
       * 通过构造AggregationPlanNode时传进来的
       * GetAggregates()返回的是一个vector，本质上是AbstractExpression
       * GetAggregateTypes()返回也是一个vector，有时候是CountStarAggregate，聚集的规则，count,max,min
       * */
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      /*这里仅仅是为了不报错*/
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  /*从子节点获取tuple和rid的数据*/
  while (child_->Next(&tuple, &rid)) {
    /* 根据tuple生成对应的key和value放入哈希表中
     * 拿到一个新的tuple，把这个tuple中的数据，建立起映射关系
     **/
    auto aggregate_key = MakeAggregateKey(&tuple);
    auto aggregate_value = MakeAggregateValue(&tuple);
    aht_.InsertCombine(aggregate_key, aggregate_value);
  }
  aht_iterator_ = aht_.Begin();  // 插入后需要重新指定 aht_iterator_ 因为哈希表是无序的
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Schema schema(plan_->OutputSchema());
  if (aht_iterator_ != aht_.End()) {
    /*
     *  每次获取一个hashmap中的key
     *  key_1 = 【上等仓，女】
     *  key_2 = 【下等仓，男】
     * */
    std::vector<Value> value(aht_iterator_.Key().group_bys_);
    /*先放分组，再放数据*/
    for (const auto &aggregate : aht_iterator_.Val().aggregates_) {
      value.push_back(aggregate);
    }
    *tuple = {value, &schema};
    ++aht_iterator_;
    successful_ = true;
    return true;
  }
  /* 特殊处理空表的情况：当为空表，且想获得统计信息时，只有countstar返回0，其他情况返回无效null*/
  // 空表执行 select count(*) from t1;
  if (!successful_) {
    /*跟迭代器无关的逻辑，只需要返回一次*/
    successful_ = true;
    if (plan_->group_bys_.empty()) {
      std::vector<Value> value;
      for (auto aggregate : plan_->agg_types_) {
        switch (aggregate) {
          case AggregationType::CountStarAggregate:
            value.push_back(ValueFactory::GetIntegerValue(0));
            break;
          case AggregationType::CountAggregate:
          case AggregationType::SumAggregate:
          case AggregationType::MinAggregate:
          case AggregationType::MaxAggregate:
            value.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
            break;
        }
      }
      *tuple = {value, &schema};
      successful_ = true;
      return true;
    }
    return false;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
