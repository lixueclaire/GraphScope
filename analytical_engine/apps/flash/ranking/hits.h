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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_HITS_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_HITS_H_

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"

namespace gs {

template <typename FRAG_T, typename VALUE_T>
class HITSFlash : public FlashAppBase<FRAG_T, VALUE_T> {
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

  float* Res(value_t* v) { return &(v->auth); };

  void Run(const fragment_t& graph, int max_iters) {
    Print("Run HITS with Flash, max_iters = %d\n", max_iters);
    int n_vertex = graph.GetTotalVerticesNum();
    Print("Total vertices: %d\n", n_vertex);

    DefineMapV(init_v) { v.auth = 1.0; v.hub = 1.0; v.auth1 = 0; v.hub1 = 0;};
    VertexMap(All, CTrueV, init_v);

    double sa = 0, sh = 0, sa_all = 0, sh_all = 0;

	  DefineMapE(update1) { d.auth1 += s.hub; };
	  DefineMapE(update2) { d.hub1 += s.auth; };
	  DefineMapV(local) {
      v.auth = v.auth1 / sa_all;
      v.hub = v.hub1 / sh_all;
      v.auth1 = 0;
      v.hub1 = 0;
    };

	  for(int i = 0; i < max_iters; ++i) {
		  Print("Round %d\n", i);
		  sa = 0, sh = 0, sa_all = 0, sh_all = 0;

		  EdgeMapDense(All, ED, CTrueE, update1, CTrueV, false);
		  EdgeMapDense(All, ER, CTrueE, update2, CTrueV, false);

		  for (auto &id: All.s) {
        float auth = GetV(id)->auth1, hub = GetV(id)->hub1;
        sa += auth * auth;
        sh += hub * hub;
      }
		  sa_all = sqrt(sa);
      sh_all = sqrt(sh);
		  VertexMap(All, CTrueV, local);
	  }
  }
};

}  // namespace flash
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_HITS_H_
