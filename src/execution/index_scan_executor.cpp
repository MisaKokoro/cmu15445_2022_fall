//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      index_info_{this->exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)},
      table_info_{this->exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_)},
      tree_{dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get())},
      iter_{tree_->GetBeginIterator()} {}

void IndexScanExecutor::Init() {}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  /**
   * 这里判断条件改成iter_.IsEnd()会出现段错误问题，因为如果把表里面所有内容删除完的话，
   * 在执行index_scan_executor算子时，iter_是空迭代器，调用isEnd函数会出问题
   * 在IsEnd()函数中，调用leaf_->GetNextPageId()函数会访问已经delete的地址
   * 导致出现错误
   * 思考：IndexScan的过程中会出现迭代器失效的问题吗？
   */
  if (iter_ == tree_->GetEndIterator()) {
    return false;
  }

  *rid = (*iter_).second;
  auto result = table_info_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
  ++iter_;

  return result;
}

}  // namespace bustub
