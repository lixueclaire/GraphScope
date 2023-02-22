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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_CLOSENESS_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_CLOSENESS_H_

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

namespace gs {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class ClosenessFlash : public FlashAppBase<FRAG_T, VALUE_T> {
 public:
  using fragment_t = FRAG_T;
  using vid_t = typename fragment_t::vid_t;
  using vertex_t = typename fragment_t::vertex_t;
  using value_t = VALUE_T;
	using vdata_t = typename fragment_t::vdata_t;
  using edata_t = typename fragment_t::edata_t;
  using adj_list_t = typename fragment_t::adj_list_t;
  using vset_t = VertexSubset<fragment_t, value_t>;
  bool sync_all_ = false;

  double* Res(value_t* v) { return &(v->val); }

  void Run(const fragment_t& graph) {
    Print("Run closeness-centrality with Flash, ");
    int n_vertex = graph.GetTotalVerticesNum();
    Print("total vertices: %d\n", n_vertex);

    std::vector<int> s;
    int64_t one = 1, n_sample = 500;

    DefineMapV(init) { v.val = 0; v.cnt = 0; };
    VertexMap(All, CTrueV, init);

    DefineMapV(local1) { v.seen = 0; };
    DefineFV(filter2) { return find(s, (int)id); };
    DefineMapV(local2) { int p = locate(s, (int)id); v.seen |= one <<p; };

    for (int p = 0; p < n_sample; p += 64) {
      Print("Phase %d\n", p/64+1);
      s.clear();
		  for(int i = p; i < n_sample && i < p + 64; ++i)
        s.push_back(rand() % n_vertex);

      int l = s.size();
      vset_t C = All;
      vset_t S = VertexMap(All, CTrueV, local1);
      S = VertexMap(S, filter2, local2);

		  for(int len = VSize(S), i = 1; len > 0; len = VSize(S), ++i) {
        DefineFE(check) { return s.seen & (~d.seen); };
        DefineMapE(update) {
          int64_t b = s.seen & (~d.seen);
          d.seen |= b;
          for (int p = 0; p < l; ++p)
            if (b & (one << p)) {
              d.val += i;
              ++d.cnt;
            }
        };

			  Print("Round %d: size=%d\n", i, len);
        S = EdgeMapDense(All, EU, check, update, CTrueV);
		  }
    }

    DefineMapV(final) { if (v.cnt) v.val /= v.cnt; };
    VertexMap(All, CTrueV, final, false);

    double best_local = n_vertex * 1.0, best;
    for (auto &i : All.s) {
      if (GetV(i)->cnt && GetV(i)->val < best_local)
        best_local = GetV(i)->val;
    }
	  GetMin(best_local, best);
	  Print( "best_val=%0.5lf\n", best);
  }
};

}  // namespace flash
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_CLOSENESS_H_
