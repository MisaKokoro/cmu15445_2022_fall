//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  /*通过调用父类函数设置初始化元数据信息*/
  this->SetPageType(IndexPageType::LEAF_PAGE);
  this->SetSize(0);

  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);
  this->SetNextPageId(INVALID_PAGE_ID); /*这个地方是否正确呢？*/

  this->SetMaxSize(max_size);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &keyComparator)
    -> int {
  /*叶子节点插入kv，返回插入后kv数量*/
  auto insert_position = KeyIndex(key, keyComparator);
  if (insert_position == GetSize()) {
    /*说明在末尾追加, 这里代码逻辑可以稍微优化一下*/
    array_[insert_position] = {key, value};
    IncreaseSize(1);
    return GetSize();
  }
  /*key重复*/
  if (keyComparator(array_[insert_position].first, key) == 0) {
    return GetSize();
  }
  /*一般位置*/
  for (int i = GetSize(); i > insert_position; i--) {
    array_[i] = array_[i - 1];
  }
  array_[insert_position] = {key, value};
  IncreaseSize(1);
  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  int start_split_indx = GetMinSize();
  SetSize(start_split_indx);
  /*0 1    2 3 4
   * 一共5个元素 maxsize=5 那么这个时候要移动的数据是2到5之间的
   * 0 1 2 3
   * 4  2
   * */
  /*modify:*/
  recipient->CopyNFrom(array_ + start_split_indx, GetMaxSize() - start_split_indx);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  /*把前一半剩下的数据加在，另一个页面的末尾*/
  std::copy(items, items + size, array_ + GetSize());
  IncreaseSize(size);
}
/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  if (index < 0 || index >= GetSize()) {
    return {};
  }
  return array_[index].first;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &keyComparator) -> int {
  int target_in_array = KeyIndex(key, keyComparator);
  if (target_in_array == GetSize() || keyComparator(array_[target_in_array].first, key) != 0) {
    return GetSize();
  }
  std::move(array_ + target_in_array + 1, array_ + GetSize(), array_ + target_in_array);
  IncreaseSize(-1);
  return GetSize();
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, const KeyComparator &keyComparator) const
    -> bool {
  int l = 0;
  int r = GetSize() - 1;
  while (l < r) {
    int mid = (l + r) / 2;
    if (keyComparator(array_[mid].first, key) >= 0) {
      r = mid;
    } else {
      l = mid + 1;
    }
  }
  if (keyComparator(array_[l].first, key) != 0) {
    return false;
  }
  *value = array_[l].second;
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &keyComparator) const -> int {
  auto target = std::lower_bound(array_, array_ + GetSize(), key, [&keyComparator](const auto &pair, auto k) {
    return keyComparator(pair.first, k) < 0;
  });
  return std::distance(array_, target);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  auto first_item = GetItem(0);
  std::move(array_ + 1, array_ + GetSize(), array_);
  IncreaseSize(-1);
  recipient->CopyLastFrom(first_item);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  *(array_ + GetSize()) = item;
  IncreaseSize(1);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  auto last_item = GetItem(GetSize() - 1);
  IncreaseSize(-1);
  recipient->CopyFirstFrom(last_item);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  std::move_backward(array_, array_ + GetSize(), array_ + GetSize() + 1);
  *array_ = item;
  IncreaseSize(1);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  recipient->CopyNFrom(array_, GetSize());
  recipient->SetNextPageId(GetNextPageId());
  SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) -> const MappingType & { return array_[index]; }
template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
