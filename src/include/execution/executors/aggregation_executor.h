#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "container/hash/hash_function.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/aggregation_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * A simplified hash table that has all the necessary functionality for aggregations.
 */
class SimpleAggregationHashTable {
 public:
  /**
   * Construct a new SimpleAggregationHashTable instance.
   * @param agg_exprs the aggregation expressions
   * @param agg_types the types of aggregations
   */
  SimpleAggregationHashTable(const std::vector<AbstractExpressionRef> &agg_exprs,
                             const std::vector<AggregationType> &agg_types)
      : agg_exprs_{agg_exprs}, agg_types_{agg_types} {}

  /** @return The initial aggregrate value for this aggregation executor */
  auto GenerateInitialAggregateValue() -> AggregateValue {
    std::vector<Value> values{};
    for (const auto &agg_type : agg_types_) {
      switch (agg_type) {
        case AggregationType::CountStarAggregate:
          /*count(*)初始化为0*/
          values.emplace_back(ValueFactory::GetIntegerValue(0));
          break;
          /*注意中间没有break,都往下执行赋值都为null*/
        case AggregationType::CountAggregate:
        case AggregationType::SumAggregate:
        case AggregationType::MinAggregate:
        case AggregationType::MaxAggregate:
          values.emplace_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
          break;
      }
    }
    return {values};
  }

  /**
   * TODO(Student)
   *
   * Combines the input into the aggregation result.
   * @param[out] result The output aggregate value
   * @param input The input value
   */
  void CombineAggregateValues(AggregateValue *result, const AggregateValue &input) {
    /**
     * * 注意输入参数：
     *  参数1. 已经有的聚集内容   举例：【李四，20】,【蔡徐坤，25】, 【马保国，69】
     *  参数2. 即将新加入的数据   举例： 【张三，18】
     *  将属性列按照对应的聚集信息进行归类
     * */
    for (uint32_t i = 0; i < agg_exprs_.size(); i++) {
      switch (agg_types_[i]) {
        case AggregationType::CountStarAggregate:
          /*数量加1*/
          result->aggregates_[i] = result->aggregates_[i].Add({INTEGER, 1});
          break;
        case AggregationType::CountAggregate:
          /* Q: count(*)和count有什么区别？
           * A:count(*)包括了所有的列，相当于行数，在统计结果的时候，不会忽略列值为NULL。
           * count(列名)只包括列名那一列，在统计结果的时候，会忽略列值为空。
           * */
          if (!input.aggregates_[i].IsNull()) {
            if (result->aggregates_[i].IsNull()) {
              result->aggregates_[i] = Value(INTEGER, 0);
            }
            result->aggregates_[i] = result->aggregates_[i].Add({INTEGER, 1});
          }
          break;
        case AggregationType::SumAggregate:
          /*result没有值的时候，先赋初值*/
          if (!input.aggregates_[i].IsNull() && result->aggregates_[i].IsNull()) {
            result->aggregates_[i] = Value(INTEGER, 0);
          }
          /*只有是整数时才能累加*/
          if (!input.aggregates_[i].IsNull() && input.aggregates_[i].CheckInteger()) {
            result->aggregates_[i] = result->aggregates_[i].Add(input.aggregates_[i]);
          }
          break;
        case AggregationType::MinAggregate:
          /* 1. 为空
           * 2. 比现有的小
           * */
          if (!input.aggregates_[i].IsNull() &&
              (result->aggregates_[i].IsNull() ||
               input.aggregates_[i].CompareLessThan(result->aggregates_[i]) == CmpBool::CmpTrue)) {
            result->aggregates_[i] = input.aggregates_[i];
          }
          break;
        case AggregationType::MaxAggregate:
          /* 1. 为空
           * 2. 比现有的大
           * */
          if (!input.aggregates_[i].IsNull() &&
              (result->aggregates_[i].IsNull() ||
               input.aggregates_[i].CompareGreaterThan(result->aggregates_[i]) == CmpBool::CmpTrue)) {
            result->aggregates_[i] = input.aggregates_[i];
          }
          break;
      }
    }
  }

  /**
   * Inserts a value into the hash table and then combines it with the current aggregation.
   * @param agg_key the key to be inserted
   * @param agg_val the value to be inserted
   */
  void InsertCombine(const AggregateKey &agg_key, const AggregateValue &agg_val) {
    /*真正的哈希表unordered_map<key,value>, 如果没有数据，第一次插入，先初始化*/
    if (ht_.count(agg_key) == 0) {
      ht_.insert({agg_key, GenerateInitialAggregateValue()});
    }
    /*
     * 如果哈希表中已经有数据了，那么就需要按照group的类别进行分组统计*/
    CombineAggregateValues(&ht_[agg_key], agg_val);
  }

  /**
   * Clear the hash table
   */
  void Clear() { ht_.clear(); }

  /** An iterator over the aggregation hash table */
  class Iterator {
   public:
    /** Creates an iterator for the aggregate map. */
    explicit Iterator(std::unordered_map<AggregateKey, AggregateValue>::const_iterator iter) : iter_{iter} {}

    /** @return The key of the iterator */
    auto Key() -> const AggregateKey & { return iter_->first; }

    /** @return The value of the iterator */
    auto Val() -> const AggregateValue & { return iter_->second; }

    /** @return The iterator before it is incremented */
    auto operator++() -> Iterator & {
      ++iter_;
      return *this;
    }

    /** @return `true` if both iterators are identical */
    auto operator==(const Iterator &other) -> bool { return this->iter_ == other.iter_; }

    /** @return `true` if both iterators are different */
    auto operator!=(const Iterator &other) -> bool { return this->iter_ != other.iter_; }

   private:
    /** Aggregates map */
    std::unordered_map<AggregateKey, AggregateValue>::const_iterator iter_;
  };

  /** @return Iterator to the start of the hash table */
  auto Begin() -> Iterator { return Iterator{ht_.cbegin()}; }

  /** @return Iterator to the end of the hash table */
  auto End() -> Iterator { return Iterator{ht_.cend()}; }

 private:
  /** The hash table is just a map from aggregate keys to aggregate values */
  std::unordered_map<AggregateKey, AggregateValue> ht_{};
  /** The aggregate expressions that we have */
  const std::vector<AbstractExpressionRef> &agg_exprs_;
  /** The types of aggregations that we have */
  const std::vector<AggregationType> &agg_types_;
};

/**
 * AggregationExecutor executes an aggregation operation (e.g. COUNT, SUM, MIN, MAX)
 * over the tuples produced by a child executor.
 */
class AggregationExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new AggregationExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The insert plan to be executed
   * @param child_executor The child executor from which inserted tuples are pulled (may be `nullptr`)
   */
  AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                      std::unique_ptr<AbstractExecutor> &&child);

  /** Initialize the aggregation */
  void Init() override;

  /**
   * Yield the next tuple from the insert.
   * @param[out] tuple The next tuple produced by the aggregation
   * @param[out] rid The next tuple RID produced by the aggregation
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the aggregation */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

  /** Do not use or remove this function, otherwise you will get zero points. */
  auto GetChildExecutor() const -> const AbstractExecutor *;

 private:
  /** @return The tuple as an AggregateKey */
  auto MakeAggregateKey(const Tuple *tuple) -> AggregateKey {
    /* Q: 把一个tuple转变为哈希表的key，做了什么工作呢？*/
    std::vector<Value> keys;
    /*这是按照group by把对应的值取出来，放到keys中*/
    /*   分组的列： plan_->GetGroupBys()
     * 比如按照两个字段分：
     *    key = 【上等仓，男】
     * */
    for (const auto &expr : plan_->GetGroupBys()) {
      keys.emplace_back(expr->Evaluate(tuple, child_->GetOutputSchema()));
    }
    return {keys};
  }

  /** @return The tuple as an AggregateValue */
  auto MakeAggregateValue(const Tuple *tuple) -> AggregateValue {
    std::vector<Value> vals;
    /*需要输出的列，聚集的列 GetAggregates()
     *    value = 【张三，18】
     *哈希表中放的数据就是
     *    key=【上等仓，男】 ---> value =【张三，18】
     * */
    for (const auto &expr : plan_->GetAggregates()) {
      vals.emplace_back(expr->Evaluate(tuple, child_->GetOutputSchema()));
    }
    return {vals};
  }

 private:
  /** The aggregation plan node */
  const AggregationPlanNode *plan_;
  /** The child executor that produces tuples over which the aggregation is computed */
  std::unique_ptr<AbstractExecutor> child_;
  /** Simple aggregation hash table */
  SimpleAggregationHashTable aht_; /*哈希表*/
  /** Simple aggregation hash table iterator */
  SimpleAggregationHashTable::Iterator aht_iterator_; /*迭代器*/
  bool successful_{false};
};
}  // namespace bustub
