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
  throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  std::size_t l = key.size();
  if (l < 1) {
    return *this;
  }

  auto cur = this->root_;

  std::unique_ptr<TrieNode> new_nodes[l];

  for (std::size_t i = 0; i < l - 1; i++) {
    if (cur) {
      new_nodes[i] = cur->Clone();
      auto next = cur->FindNext(key[i]);
      cur = next;
    } else {
      new_nodes[i] = std::make_unique<TrieNode>();
    }
  }

  if (cur) {
    new_nodes[l-1] = std::make_unique<TrieNodeWithValue<T>>(cur->children_, std::make_shared<T>(std::move(value)));
  } else {
    new_nodes[l-1] = std::make_unique<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
  }

  for(std::size_t i = 0; i < l - 1; i++) {
    new_nodes[i]->children_[key[i]] = std::shared_ptr<const TrieNode>(std::move(new_nodes[i+1]));
  }

  return Trie(std::move(new_nodes[0]));
}

auto Trie::Remove(std::string_view key) const -> Trie {
  throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
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
