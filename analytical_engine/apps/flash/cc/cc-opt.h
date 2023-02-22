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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_CC_OPT_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_CC_OPT_H_

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"

namespace gs {

template <typename FRAG_T>
class CCOptFlash : public FlashAppBase<FRAG_T, CC_OPT_TYPE> {
 public:
  INSTALL_FLASH_WORKER(BCCFlash<FRAG_T>, CC_OPT_TYPE, FRAG_T)
  using context_t = FlashVertexDataContext<FRAG_T, CC_OPT_TYPE, int>;
  bool sync_all_ = false;

  int64_t* Res(value_t* v) { return &(v->res); }

  void Run(const fragment_t& graph) {
    int n_vertex = graph.GetTotalVerticesNum();
    Print("Run CC with Flash, total vertices: %d\n", n_vertex);
   	long long v_loc = 0, v_glb = 0;

	  DefineMapV(init) {
		  v.res = Deg(id) * (long long) n_vertex + id;
		  v_loc = std::max(v_loc, v.res);
	  };
	  DefineFV(filter) { return v.res == v_glb; };

	  VertexMap(All, CTrueV, init);
    GetMax(v_loc, v_glb);;
	  vset_t A = VertexMap(All, filter);

	  DefineFV(cond) { return v.res != v_glb; };
	  DefineMapE(update) { d.res = v_glb; };
	  DefineMapE(reduce) { d = s; };

	  for(int len = VSize(A), i = 0; len > 0; len = VSize(A), ++i) {
		  Print("Round 0.%d: size=%d\n", i, len);
		  A = EdgeMap(A, EU, CTrueE, update, cond, reduce);
	  }

	  DefineFV(filter2) { return v.res != v_glb; };
	  A = VertexMap(All, filter2);

	  DefineFE(check2) { return s.res > d.res; };
	  DefineMapE(update2) { d.res = std::max(d.res, s.res); };

	  for(int len = VSize(A), i = 0; len > 0; len = VSize(A),++i) {
		  Print("Round 1.%d: size=%d\n", i, len);
		  A = EdgeMap(A, EU, check2, update2, CTrueV, update2);
	  }
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_CC_OPT_H_
