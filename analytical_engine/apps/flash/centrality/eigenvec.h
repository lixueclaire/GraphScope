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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_EIGENVEC_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_EIGENVEC_H_

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

namespace gs {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class EigenvecFlash : public FlashAppBase<FRAG_T, VALUE_T> {
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

  float* Res(value_t* v) { return &(v->val); }

  void Run(const fragment_t& graph) {
    Print("Run eigenvec-centrality with Flash, ");
    int n_vertex = graph.GetTotalVerticesNum();
    Print("total vertices: %d\n", n_vertex);

		DefineMapV(init) { v.val = 1.0 / n_vertex; };
    VertexMap(All, CTrueV, init);

    float s = 0, s_tot = 0;
		DefineMapE(update) { d.next += s.val; };
		DefineMapV(local1) { v.val = v.next; v.next = 0; };
    DefineMapV(local2) { v.val /= s_tot; };

		for (int i = 0; i < 10; i++) {
			Print("Round %d\n", i);
      s = 0;
      s_tot = 0;
			EdgeMapDense(All, EU, CTrueE, update, CTrueV, false);
			VertexMap(All, CTrueV, local1, false);
      for (auto &id: All.s)
        s += GetV(id) -> val;
      GetSum(s, s_tot);
      s_tot = sqrt(s_tot);
      VertexMap(All, CTrueV, local2);
		}
  }
};

}  // namespace flash
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_EIGENVEC_H_
