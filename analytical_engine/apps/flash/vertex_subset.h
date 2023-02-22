/** Copyright 2022 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_APP_VERTEX_SUBSET_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_APP_VERTEX_SUBSET_H_

#include "flash/flashware.h"

namespace gs {

/**
 * @brief The VertexSubset structure in Flash.
 *
 * @tparam FRAG_T
 * @tparam VALUE_T
 */
template <typename FRAG_T, class VALUE_T>
class VertexSubset {
 public:
  using fragment_t = FRAG_T;
  using value_t = VALUE_T;
  using vid_t = typename fragment_t::vid_t;
  using vset_t = VertexSubset<fragment_t, value_t>;
  static vset_t all;
  static vset_t empty;
  static FlashWare<fragment_t, value_t>* fw;

  VertexSubset() { is_dense = false; }
  VertexSubset(const VertexSubset &other) {
    s = other.s;
    d.copy(other.d);
    is_dense = other.is_dense;
  }
  VertexSubset& operator=(const VertexSubset &other) {
    s = other.s;
    d.copy(other.d);
    is_dense = other.is_dense;
    return *this;
  }
  VertexSubset& operator=(VertexSubset &&other) {
    s = std::move(other.s);
    d.move(other.d);
    is_dense = other.is_dense;
    return *this;
  }
  ~VertexSubset() { s.clear(); }

  inline int size() const {
    int cnt_local = (int) s.size(), cnt;
    fw->Sum(cnt_local, cnt);
    return cnt;
  }
  inline bool IsEmpty() const { return size() == 0; }
  inline bool IsIn(const vid_t vid) const { return d.get_bit(vid); }
  inline void AddV(const vid_t vid) { s.push_back(vid); }
  inline void ToDense();

  vset_t Union(const vset_t& x) const;
  vset_t Minus(const vset_t& x) const;
  vset_t Intersect(const vset_t& x) const;

 public:
  std::vector<vid_t> s;
  grape::Bitset d;
  bool is_dense;
};

template <typename FRAG_T, class VALUE_T>
VertexSubset<FRAG_T, VALUE_T> VertexSubset<FRAG_T, VALUE_T>::all = vset_t();
template <typename FRAG_T, class VALUE_T>
VertexSubset<FRAG_T, VALUE_T> VertexSubset<FRAG_T, VALUE_T>::empty = vset_t();
template <typename FRAG_T, class VALUE_T>
FlashWare<FRAG_T, VALUE_T>* VertexSubset<FRAG_T, VALUE_T>::fw =
    nullptr;

template <typename fragment_t, class value_t>
void VertexSubset<fragment_t, value_t>::ToDense() {
  if (is_dense)
    return;
  is_dense = true;
  if (d.get_size() == 0)
    d.init(fw->GetSize());
  else
    d.parallel_clear(fw->GetThreadPool());
  fw->ForEach(s.begin(), s.end(),
              [this](int tid, int key) { this->d.set_bit(key); });
  fw->SyncBitset(d);
}

template <typename fragment_t, class value_t>
VertexSubset<fragment_t, value_t> VertexSubset<fragment_t, value_t>::Union(
    const vset_t& x) const {
  vset_t y;
  y.s.resize(s.size() + x.s.size());
  typename std::vector<vid_t>::iterator it =
      set_union(s.begin(), s.end(), x.s.begin(), x.s.end(), y.s.begin());
  y.s.resize(it - y.s.begin());
  return y;
}

template <typename fragment_t, class value_t>
VertexSubset<fragment_t, value_t> VertexSubset<fragment_t, value_t>::Minus(
    const vset_t& x) const {
  vset_t y;
  y.s.resize(s.size());
  typename std::vector<vid_t>::iterator it =
      set_difference(s.begin(), s.end(), x.s.begin(), x.s.end(), y.s.begin());
  y.s.resize(it - y.s.begin());
  return y;
}

template <typename fragment_t, class value_t>
VertexSubset<fragment_t, value_t> VertexSubset<fragment_t, value_t>::Intersect(
    const vset_t& x) const {
  vset_t y;
  y.s.resize(min(s.size(), x.s.size()));
  typename std::vector<vid_t>::iterator it =
      set_intersection(s.begin(), s.end(), x.s.begin(), x.s.end(), y.s.begin());
  y.s.resize(it - y.s.begin());
  return y;
}

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_APP_VERTEX_SUBSET_H_
