#include "primer/trie.h"
#include <cstddef>
#include <map>
#include <memory>
#include <string_view>
#include <utility>
#include "common/exception.h"

namespace bustub {

auto TrieNode::FindNext(const char &c) const -> std::shared_ptr<const TrieNode> {
  if (!this->children_.empty()) {
    auto it = this->children_.find(c);
    if (it == this->children_.end()) {
      return nullptr;
    }
    return it->second;
  }
  return nullptr;
}

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  auto l = key.size();
  auto cur = this->root_;
  for (std::size_t i = 0; i < l; i++) {
    if (cur) {
      cur = cur->FindNext(key[i]);
    } else {
      return nullptr;
    }
  }

  if (!cur || !cur->is_value_node_) {
    return nullptr;
  }
  auto node = dynamic_cast<const TrieNodeWithValue<T> *>(cur.get());
  if (!node) {
    return nullptr;
  }
  return node->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  auto l = key.size();

  auto cur = this->root_;

  std::shared_ptr<TrieNode> new_nodes[l+1];

  for (std::size_t i = 0; i < l; i++) {
    if (cur) {
      new_nodes[i] = cur->Clone();
      cur = cur->FindNext(key[i]);
    } else {
      new_nodes[i] = std::make_shared<TrieNode>();
    }
  }

  if (cur) {
    new_nodes[l] = std::make_unique<TrieNodeWithValue<T>>(cur->children_, std::make_shared<T>(std::move(value)));
  } else {
    new_nodes[l] = std::make_unique<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
  }

  for(std::size_t i = 0; i < l; i++) {
    new_nodes[i]->children_[key[i]] = std::shared_ptr<const TrieNode>(new_nodes[i+1]);
  }

  return Trie(std::move(new_nodes[0]));
}

auto Trie::Remove(std::string_view key) const -> Trie {
  auto l = key.size();

  auto cur = this->root_;

  std::shared_ptr<TrieNode> new_nodes[l+1];

  for (std::size_t i = 0; i < l; i++) {
    if (cur) {
      new_nodes[i] = cur->Clone();
      cur = cur->FindNext(key[i]);
    } else {
      return *this;
    }
  }

  if (!cur || !cur->is_value_node_) {
    return *this;
  }

  if (!cur->children_.empty()) {
    new_nodes[l] = std::make_shared<TrieNode>(cur->children_); //copy
  } else {
    new_nodes[l] = std::make_shared<TrieNode>();
  }

  for(std::size_t i = 0; i < l; i++) {
    new_nodes[i]->children_[key[i]] = std::shared_ptr<const TrieNode>(new_nodes[i+1]);
  }

  return Trie(std::move(new_nodes[0]));
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
