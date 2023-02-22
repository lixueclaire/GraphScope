/** Copyright 2020 Alibaba Group Holding Limited.

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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_ARTICLE_RANK_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_ARTICLE_RANK_H_

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

namespace gs {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class ArticleRankFlash : public FlashAppBase<FRAG_T, VALUE_T> {
 public:
  using fragment_t = FRAG_T;
  using vid_t = typename fragment_t::vid_t;
  using vertex_t = typename fragment_t::vertex_t;
  using value_t = VALUE_T;
  using vdata_t = typename fragment_t::vdata_t;
  using edata_t = typename fragment_t::edata_t;
  using adj_list_t = typename fragment_t::adj_list_t;
  using vertex_subset = VertexSubset<fragment_t, value_t>;
  bool sync_all_ = false;

  float* Res(value_t* v) { return &(v->val); };

  void Run(const fragment_t& graph, int max_iters, float damping = 0.85) {
    Print("Run ArticleRank with Flash, max_iters = %d\n", max_iters);
    int n_vertex = graph.GetTotalVerticesNum();
    Print("Total vertices: %d\n", n_vertex);

    DefineMapV(init_v) {
      v.next = 0;
      v.deg = OutDeg(id);
    };
    VertexMap(All, CTrueV, init_v);

    float a = 0, tot = 0, avg = 0;
    for (auto &id: All.s)
      a += GetV(id)->deg;
    GetSum(a, tot);
    avg = tot / n_vertex;

    Print("Init complete\n");

    DefineMapE(update) { d.next += damping * s.val / (s.deg + avg); };
    DefineMapV(local) {
      v.val = v.next + 1 - damping;
      v.next = 0;
    };

    for (int i = 0; i < max_iters; i++) {
      Print("Round %d\n", i);
      EdgeMapDense(All, ED, CTrueE, update, CTrueV, false);
      VertexMap(All, CTrueV, local);
    }

    /*double loc_val = 0, tot_val = 0;
    for (auto &id: All.s)
      loc_val += GetV(id)->val;
    All.fw->Sum(loc_val, tot_val);
    std::cout << "total = " << tot_val << std::endl;*/
  }
};

}  // namespace flash
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_ARTICLE_RANK_H_
