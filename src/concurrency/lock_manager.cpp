//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

/**
 * 这个状态转移矩阵没有给出在commit，abort状态下的数据，存在一些bug
 * 暂时先不用
 */
// static int isolation_with_lockmode_state_martrix[][3][5] = {
//     // GROWING
//     {
//         //   S  X  IS IX SIX
//         {0, 1, 0, 1, 0},  // READ_UNCOMMITTED
//         {1, 1, 1, 1, 1},  // REPEATABLE_READ
//         {1, 1, 1, 1, 1},  // READ_COMMITTED
//     },
//     // SHRINKINNG
//     {
//         {1, 0, 1, 0, 1},  // READ_UNCOMMITTED
//         {0, 0, 0, 0, 0},  // REPEATABLE_READ
//         {1, 0, 1, 0, 0},  // READ_COMMITTED
//     }};
/**
 *    While upgrading, only the following transitions should be allowed:
 *        IS -> [S, X, IX, SIX]
 *        S -> [X, SIX]
 *        IX -> [X, SIX]
 *        SIX -> [X]
 */
static int lock_update_state_martrix[][5] = {
    {0, 1, 0, 0, 1},  // S
    {0, 0, 0, 0, 0},  // X
    {1, 1, 0, 1, 1},  // IS
    {0, 1, 0, 0, 1},  // IX
    {0, 1, 0, 0, 0},  // SIX
};

static int lock_compatible_matrix[][5] = {
    {1, 0, 1, 0, 0},  // S
    {0, 0, 0, 0, 0},  // X
    {1, 0, 1, 1, 1},  // IS
    {0, 0, 1, 1, 0},  // IX
    {0, 0, 1, 0, 0},  // SIX
};

static int unlock_change_state_matrix[][3] = {
    {0, 1, 0},  // S
    {1, 1, 1},  // X
    {0, 0, 0},  // IS
    {0, 0, 0},  // IX
    {0, 0, 0},  // SIX
};

static inline void TransctionThrowAbort(Transaction *txn, AbortReason reason) {
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), reason);
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  /**
   * 先把不合法的情况全部抛出异常
   * 这里主要是判断当前事务所处的阶段（GROWING 还是 SHRINKING)
   * 以及当前的隔离级别，判断当前要加的锁能不能加
   * 判断方法通过状态矩阵给出
   */
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    if (txn->GetState() == TransactionState::SHRINKING &&
        (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  // 读已提交
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::INTENTION_SHARED &&
        lock_mode != LockMode::SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  // 可重复读
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  // 当前的表还没有被加锁，将该表注册到lock map中
  table_lock_map_latch_.lock();
  if (table_lock_map_.count(oid) == 0) {
    table_lock_map_.emplace(oid, std::make_shared<LockRequestQueue>());
  }
  auto lock_request_queue = table_lock_map_.find(oid)->second;
  lock_request_queue->latch_.lock();  // 后续需要对队列进行操作，先把队列锁了
  table_lock_map_latch_.unlock();

  /**
   * 如果以前这个事务已经给这个表加过锁，
   * 那么尝试对锁进行升级
   */
  for (auto request : lock_request_queue->request_queue_) {  // NOLINT
    if (request->txn_id_ == txn->GetTransactionId()) {
      /**
       * 当前事务尝试上的锁与之前上的锁相同，直接返回即可
       * 注意需要释放lock_request_queue的锁，或许可以用lockguard解决这个问题
       * */
      if (request->lock_mode_ == lock_mode) {
        lock_request_queue->latch_.unlock();
        return true;
      }
      /**
       * 表只允许有一个事务进行升级，如果当前有其他事务正在升级
       * 我们应该abort当前事务，并且抛出异常
       */
      // TODO(yanxiang) 当前升级的事务是自己也不行吗？
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        lock_request_queue->latch_.unlock();
        TransctionThrowAbort(txn, AbortReason::UPGRADE_CONFLICT);
      }

      /**       允许的锁升级类型
       *        IS -> [S, X, IX, SIX]
       *        S -> [X, SIX]
       *        IX -> [X, SIX]
       *        SIX -> [X]
       * */
      if (!static_cast<bool>(
              lock_update_state_martrix[static_cast<int>(request->lock_mode_)][static_cast<int>(lock_mode)])) {
        lock_request_queue->latch_.unlock();
        TransctionThrowAbort(txn, AbortReason::INCOMPATIBLE_UPGRADE);
      }

      lock_request_queue->request_queue_.remove(request);
      InsertOrDeleteTableLockSet(txn, request, false);
      auto upgrade_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);

      // 找到第一个未加锁的位置，将当前请求插入进去，因为当前锁升级的优先级最高
      std::list<std::shared_ptr<LockRequest>>::iterator lr_iter;
      for (lr_iter = lock_request_queue->request_queue_.begin(); lr_iter != lock_request_queue->request_queue_.end();
           lr_iter++) {
        if (!(*lr_iter)->granted_) {
          break;
        }
      }
      lock_request_queue->request_queue_.insert(lr_iter, upgrade_lock_request);
      lock_request_queue->upgrading_ = txn->GetTransactionId();
      /**
       * 这里使用条件变量的编程模型来判断当前的锁是否授予成功
       * 如果没有授予成功的话则一直等待
       */
      std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
      while (!GrantLock(upgrade_lock_request, lock_request_queue)) {
        lock_request_queue->cv_.wait(lock);
        // 唤醒之后发现当前事务被abort了，那么应当删除该事务的request
        if (txn->GetState() == TransactionState::ABORTED) {
          lock_request_queue->upgrading_ = INVALID_TXN_ID;
          lock_request_queue->request_queue_.remove(upgrade_lock_request);
          lock_request_queue->cv_.notify_all();
          return false;
        }
      }

      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      upgrade_lock_request->granted_ = true;
      InsertOrDeleteTableLockSet(txn, upgrade_lock_request, true);
      /**
       * 如果当前加的锁不是排它锁，那么可以通知其他线程来加锁了
       * 因为排它锁不和任何锁兼容，索引没必要通知
       */
      if (lock_mode != LockMode::EXCLUSIVE) {
        lock_request_queue->cv_.notify_all();
      }
      return true;
    }
  }

  // 如果当前事务之前没有申请过锁，那么直接添加到队列末尾，等待授权
  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  lock_request_queue->request_queue_.push_back(lock_request);

  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  while (!GrantLock(lock_request, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
    /**
     * 这里检测事务是否abort是因为后台死锁检测线程
     * 可能会因为这个事务死锁将其abort了
     * 因此我们要在他abort之后去唤醒等待他的事务
     */
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }

  lock_request->granted_ = true;
  InsertOrDeleteTableLockSet(txn, lock_request, true);

  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }
  return true;
}

auto LockManager::GrantLock(const std::shared_ptr<LockRequest> &lock_request,
                            const std::shared_ptr<LockRequestQueue> &lock_request_queue) -> bool {
  // 授予锁的条件
  // 1. 前面事务都加锁
  // 2. 前面事务都兼容
  /**
   * 注意这个函数的lock_request_queue中已经包含了当前的lock_request
   * 因此如果是没有授权的锁，要么是当前的锁请求，要么是其他的锁请求
   * 根据设计，前面没有授权的锁拥有优先授权权，因此这里当找到一个
   * 没有授权的请求时，如果发现不是自己，那么应该立即返回false
   */
  for (auto &lr : lock_request_queue->request_queue_) {
    if (lr->granted_) {
      if (!static_cast<bool>(
              lock_compatible_matrix[static_cast<int>(lr->lock_mode_)][static_cast<int>(lock_request->lock_mode_)])) {
        return false;
      }
    } else if (lock_request.get() != lr.get()) {
      return false;
    } else {
      return true;
    }
  }
  return false;
}

void LockManager::InsertOrDeleteTableLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request,
                                             bool insert) {
  // TODO(yao): 这个要我们自己实现吗？这个函数的目的和作用是什么？插入或者删除表锁集合！
  // 从哪儿删和从哪儿插？从事务本身的集合中。
  // 要根据锁模式做区分...
  std::vector<std::shared_ptr<std::unordered_set<bustub::table_oid_t>>> table_lock_set = {
      txn->GetSharedTableLockSet(),
      txn->GetExclusiveTableLockSet(),
      txn->GetIntentionSharedTableLockSet(),
      txn->GetIntentionExclusiveTableLockSet(),
      txn->GetSharedIntentionExclusiveTableLockSet(),
  };
  if (insert) {
    table_lock_set[static_cast<int>(lock_request->lock_mode_)]->insert(lock_request->oid_);
  } else {
    table_lock_set[static_cast<int>(lock_request->lock_mode_)]->erase(lock_request->oid_);
  }
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // 当前事务所处的状态不能解除表锁
  // BUSTUB_ASSERT(txn->GetState() != TransactionState::ABORTED && txn->GetState() != TransactionState::COMMITTED,
  //               "Transation in error state try to unlock table\n");
  table_lock_map_latch_.lock();
  // 1.当前锁表里根本没有这个表的相关的事务，应该之间抛异常
  if (table_lock_map_.count(oid) == 0) {
    table_lock_map_latch_.unlock();
    TransctionThrowAbort(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  // 2.解锁表锁还需要满足当前的事务没有给这个表加行锁
  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();
  bool flag1 = s_row_lock_set->count(oid) == 0 || s_row_lock_set->at(oid).empty();
  bool flag2 = x_row_lock_set->count(oid) == 0 || x_row_lock_set->at(oid).empty();
  if (!(flag1 && flag2)) {
    table_lock_map_latch_.unlock();
    TransctionThrowAbort(txn, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }

  // 3.符合解除表锁的条件，尝试解锁
  auto lock_request_queue = table_lock_map_[oid];
  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();

  for (auto request : lock_request_queue->request_queue_) {  // NOLINT
    if (request->txn_id_ == txn->GetTransactionId() && request->granted_) {
      // 将请求从请求队列中移除，并唤醒其他被阻塞的线程
      lock_request_queue->request_queue_.remove(request);
      lock_request_queue->cv_.notify_all();
      lock_request_queue->latch_.unlock();

      /**
       *  因为是2pl协议，当开始unlock时说明要进入SHRINKING阶段了
       *  下面根据不同的隔离级别判断是否可以进入SHRINKING阶段
       *  这里为了少写if else 还是通过状态矩阵来判断
       * */
      if (static_cast<bool>(unlock_change_state_matrix[static_cast<int>(request->lock_mode_)]
                                                      [static_cast<int>(txn->GetIsolationLevel())])) {
        // 如果事务状态不为COMMITTED和ABORTED，则修改为SHRINKING收缩
        if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
          txn->SetState(TransactionState::SHRINKING);
          // 把锁状态设置为shrinking
        }
      }
      // 解锁成功，返回true，并且更新事务的lockset
      InsertOrDeleteTableLockSet(txn, request, false);
      return true;
    }
  }

  // 当前事务不在锁表中记录，那么应该抛出异常
  lock_request_queue->latch_.unlock();
  TransctionThrowAbort(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  return false;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // 1. 行锁不允许加意向锁，当尝试给行锁加意向锁时直接抛出异常
  if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED) {
    TransctionThrowAbort(txn, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  // 2. 依旧是根据隔离级别判断当前可以加什么锁，可以参考LockTable的注释
  // 读未提交隔离级别 不允许S IS SIX 为什么不允许呢？
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
        lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
    // x和ix只能在growing阶段
    if (txn->GetState() == TransactionState::SHRINKING &&
        (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  // 读提交 shrinking可以加S和IS
  // 为什么shrinking阶段还能加锁呢？TODO(yao)：为什么shrinking阶段还能加锁？？？？？

  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::INTENTION_SHARED &&
        lock_mode != LockMode::SHARED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  // 可重复读 shrinking阶段不能加任何锁
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    if (txn->GetState() == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  // 如果是排它锁
  if (lock_mode == LockMode::EXCLUSIVE) {  // 表 X IX SIX
    if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }

  /**   3.当给行加S或者X锁时，应当检查表锁是否已经加了应该加的锁
   *    参考下面的note
   *    MULTILEVEL LOCKING:
   *    While locking rows, Lock() should ensure that the transaction has an appropriate lock on the table which the row
   *    belongs to. For instance, if an exclusive lock is attempted on a row, the transaction must hold either
   *    X, IX, or SIX on the table. If such a lock does not exist on the table, Lock() should set the TransactionState
   *    as ABORTED and throw a TransactionAbortException (TABLE_LOCK_NOT_PRESENT)
   * */
  // 如果是排它锁
  if (lock_mode == LockMode::EXCLUSIVE) {  // 表 X IX SIX
    if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      TransctionThrowAbort(txn, AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }
  // 如果是共享锁
  if (lock_mode == LockMode::SHARED) {  // 表 X IX SIX S IS
    if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid) && !txn->IsTableSharedLocked(oid) &&
        !txn->IsTableIntentionSharedLocked(oid)) {
      TransctionThrowAbort(txn, AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }

  // 前置判断条件都符合了，可以尝试给行加锁了
  row_lock_map_latch_.lock();
  if (row_lock_map_.count(rid) == 0) {
    row_lock_map_.emplace(rid, std::make_shared<LockRequestQueue>());
  }
  auto lock_request_queue = row_lock_map_[rid];
  lock_request_queue->latch_.lock();
  row_lock_map_latch_.unlock();

  bool is_upgrade = false;
  for (auto request : lock_request_queue->request_queue_) {  // NOLINT
    if (request->txn_id_ == txn->GetTransactionId()) {
      if (request->lock_mode_ == lock_mode) {
        lock_request_queue->latch_.unlock();
        return true;
      }
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        lock_request_queue->latch_.unlock();
        TransctionThrowAbort(txn, AbortReason::UPGRADE_CONFLICT);
      }

      // 进行锁升级
      if (!static_cast<bool>(
              lock_update_state_martrix[static_cast<int>(request->lock_mode_)][static_cast<int>(lock_mode)])) {
        lock_request_queue->latch_.unlock();
        TransctionThrowAbort(txn, AbortReason::INCOMPATIBLE_UPGRADE);
      }
      // 如果是锁升级，则把原来的事务请求先删了，
      lock_request_queue->request_queue_.remove(request);
      InsertOrDeleteRowLockSet(txn, request, false);
      // 创建一个锁升级请求
      is_upgrade = true;
      break;
    }
  }
  // 如果lock table中没有有重复的txn_id
  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  if (is_upgrade) {
    std::list<std::shared_ptr<LockRequest>>::iterator lr_iter;
    for (lr_iter = lock_request_queue->request_queue_.begin(); lr_iter != lock_request_queue->request_queue_.end();
         lr_iter++) {
      if (!(*lr_iter)->granted_) {
        break;  // 找到第一个没授权的事务
      }
    }
    // 插入第一个没授权的事务之前，表示正在锁升级
    lock_request_queue->request_queue_.insert(lr_iter, lock_request);
    lock_request_queue->upgrading_ = txn->GetTransactionId();
  } else {
    lock_request_queue->request_queue_.push_back(lock_request);
  }

  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  while (!GrantLock(lock_request, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }

  if (is_upgrade) {
    lock_request_queue->upgrading_ = INVALID_TXN_ID;
  }
  lock_request->granted_ = true;
  InsertOrDeleteRowLockSet(txn, lock_request, true);

  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }

  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  row_lock_map_latch_.lock();

  if (row_lock_map_.count(rid) == 0) {
    row_lock_map_latch_.unlock();
    TransctionThrowAbort(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  auto lock_request_queue = row_lock_map_[rid];
  lock_request_queue->latch_.lock();
  row_lock_map_latch_.unlock();

  for (auto request : lock_request_queue->request_queue_) {  // NOLINT
    if (request->txn_id_ == txn->GetTransactionId() && request->granted_) {
      lock_request_queue->request_queue_.remove(request);
      lock_request_queue->cv_.notify_all();
      lock_request_queue->latch_.unlock();

      if (static_cast<bool>(unlock_change_state_matrix[static_cast<int>(request->lock_mode_)]
                                                      [static_cast<int>(txn->GetIsolationLevel())])) {
        // 如果事务状态不为COMMITTED和ABORTED，则修改为SHRINKING收缩
        if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
          txn->SetState(TransactionState::SHRINKING);
          // 把锁状态设置为shrinking
        }
      }

      InsertOrDeleteRowLockSet(txn, request, false);
      return true;
    }
  }

  lock_request_queue->latch_.unlock();
  TransctionThrowAbort(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  return false;
}

void LockManager::InsertOrDeleteRowLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request,
                                           bool insert) {
  auto s_row_lock_set = txn->GetSharedRowLockSet();
  auto x_row_lock_set = txn->GetExclusiveRowLockSet();
  // std::vector<std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>>> row_lock_set = {
  //   txn->GetSharedRowLockSet(),
  //   txn->GetExclusiveRowLockSet(),
  // };
  // if (insert) {
  //   (*row_lock_set[static_cast<int>(lock_request->lock_mode_)])[lock_request->oid_].insert(lock_request->rid_);
  // } else {
  //   (*row_lock_set[static_cast<int>(lock_request->lock_mode_)])[lock_request->oid_].erase(lock_request->rid_);
  // }
  switch (lock_request->lock_mode_) {
    case LockMode::SHARED:
      if (insert) {
        InsertRowLockSet(s_row_lock_set, lock_request->oid_, lock_request->rid_);
      } else {
        DeleteRowLockSet(s_row_lock_set, lock_request->oid_, lock_request->rid_);
      }
      break;
    case LockMode::EXCLUSIVE:
      if (insert) {
        InsertRowLockSet(x_row_lock_set, lock_request->oid_, lock_request->rid_);
      } else {
        DeleteRowLockSet(x_row_lock_set, lock_request->oid_, lock_request->rid_);
      }
      break;
    case LockMode::INTENTION_SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      break;
  }
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  txn_set_.insert(t1);
  txn_set_.insert(t2);
  waits_for_[t1].push_back(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto iter = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2);
  if (iter != waits_for_[t1].end()) {
    waits_for_[t1].erase(iter);
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  for (auto start_txn_id : txn_set_) {
    if (Dfs(start_txn_id)) {
      // 这里要设初值，不然可能会出错
      *txn_id = *active_set_.begin();
      auto &res = *txn_id;
      for (auto active_txn_id : active_set_) {
        // 每次检测到死锁时，返回环上一个最新的事务id进行abort
        res = std::max(res, active_txn_id);
      }
      active_set_.clear();
      return true;
    }
  }
  active_set_.clear();
  return false;
}

// auto LockManager::Dfs(txn_id_t start_id) -> bool {
//   // 记忆化搜索，将已经确定不在环上的事务记录下来
//   if (safe_set_.count(start_id)) {
//     return false;
//   }

//   if (waits_for_.count(start_id) == 0) {
//     safe_set_.insert(start_id);
//     return false;
//   }

//   active_set_.insert(start_id);
//   std::vector<txn_id_t> &next_node_set = waits_for_[start_id];
//   // 按照文档要求，DFS判环算法需要具有稳定性，因此需要对出边节点进行排序
//   std::sort(next_node_set.begin(), next_node_set.end());
//   for (auto next_node : next_node_set) {
//     // 当前节点在活跃节点集合出现过，说明出现了环
//     if (active_set_.count(next_node)) {
//       return true;
//     }

//     if (Dfs(next_node)) {
//       return true;
//     }
//   }

//   active_set_.erase(start_id);
//   safe_set_.insert(start_id);
//   return false;
// }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (auto &[from, tos] : waits_for_) {
    for (auto to : tos) {
      edges.emplace_back(from, to);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  // 判环检测代码比较多
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    /* TODO(yao)：为什么要睡眠一段时间呢？*/
    {
      table_lock_map_latch_.lock();
      row_lock_map_latch_.lock();
      /*行和表都要加锁*/

      for (auto &[oid, request_queue] : table_lock_map_) {
        /*注意：这个无序集合，是局部变量，每次处理一个数据项*/
        std::vector<txn_id_t> granted_set;
        request_queue->latch_.lock();                              // 请求队列先加锁
        for (auto lock_request : request_queue->request_queue_) {  // NOLINT
          if (lock_request->granted_) {                            // 如果这个事务被授权了
            granted_set.push_back(lock_request->txn_id_);          // 加入授权集合中
          } else {
            // 请求的事务id---> 当前事务id
            /*TODO(yao)：为什么要把当前的事务ID，和表的关系建立起来呢？*/
            map_txn_oid_.emplace(lock_request->txn_id_, lock_request->oid_);
            for (auto txn_id : granted_set) {  // 如果是没授权的事务，那么遍历已经授权的事务，
              // lock_request---> 表id

              // 有向图，箭头指向依赖方？
              AddEdge(lock_request->txn_id_, txn_id);
            }
          }
        }
        // 解锁当前资源的请求队列
        request_queue->latch_.unlock();
      }

      /*对于行map*/
      for (auto &[rid, request_queue] : row_lock_map_) {
        std::vector<txn_id_t> granted_set;
        request_queue->latch_.lock();
        for (auto lock_request : request_queue->request_queue_) {  // NOLINT
          if (lock_request->granted_) {
            granted_set.push_back(lock_request->txn_id_);
          } else {
            map_txn_rid_.emplace(lock_request->txn_id_, lock_request->rid_);
            for (auto txn_id : granted_set) {
              AddEdge(lock_request->txn_id_, txn_id);
            }
          }
        }
        request_queue->latch_.unlock();
      }

      row_lock_map_latch_.unlock();
      table_lock_map_latch_.unlock();

      txn_id_t txn_id;
      while (HasCycle(&txn_id)) {  // 只要有环
        // LOG_DEBUG("txn_id = %d\n", txn_id);
        Transaction *txn = TransactionManager::GetTransaction(txn_id);
        txn->SetState(TransactionState::ABORTED);  // 终止这个事务
        DeleteNode(txn_id);                        // 删除节点

        if (map_txn_oid_.count(txn_id) > 0) {  // 如果表还有事务 则通知 该数据项上的所有其他线程
          table_lock_map_[map_txn_oid_[txn_id]]->latch_.lock();
          table_lock_map_[map_txn_oid_[txn_id]]->cv_.notify_all();
          table_lock_map_[map_txn_oid_[txn_id]]->latch_.unlock();
        }

        if (map_txn_rid_.count(txn_id) > 0) {  // 如果行还有事务 则通知 该数据项上的所有其他线程
          row_lock_map_[map_txn_rid_[txn_id]]->latch_.lock();
          row_lock_map_[map_txn_rid_[txn_id]]->cv_.notify_all();
          row_lock_map_[map_txn_rid_[txn_id]]->latch_.unlock();
        }
      }
      // 清空数据
      waits_for_.clear();
      safe_set_.clear();
      txn_set_.clear();
      map_txn_oid_.clear();
      map_txn_rid_.clear();
    }
  }
}

auto LockManager::DeleteNode(txn_id_t txn_id) -> void {
  waits_for_.erase(txn_id);
  // 这种循环遍历的方式是不是太低效了
  for (auto a_txn_id : txn_set_) {
    if (a_txn_id != txn_id) {
      RemoveEdge(a_txn_id, txn_id);
    }
  }
}

}  // namespace bustub
