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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_SCC_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_SCC_H_

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

namespace gs {

template <typename FRAG_T>
class SCCFlash : public FlashAppBase<FRAG_T, SCC_TYPE> {
 public:
  INSTALL_FLASH_WORKER(BCCFlash<FRAG_T>, SCC_TYPE, FRAG_T)
  using context_t = FlashVertexDataContext<FRAG_T, SCC_TYPE, int>;
  bool sync_all_ = false;

  int* Res(value_t* v) { return &(v->res); }

  void Run(const fragment_t& graph) {
    int n_vertex = graph.GetTotalVerticesNum();
    Print("Run SCC with Flash, total vertices: %d\n", n_vertex);

    DefineMapV(init) { v.scc = -1; };
	  vset_t A = VertexMap(All, CTrueV, init);

	  DefineMapV(local1) { v.fid = id; };

	  DefineFV(filter2) { return v.fid == id; };
	  DefineMapV(local2) { v.res = id; };

	  DefineFV(filter3) { return v.res == -1; };

	 for (int i = 1, vsa = VSize(A); vsa > 0; vsa = VSize(A), ++i) {
		  vset_t B = VertexMap(A, CTrueV, local1);
		  for (int vsb = VSize(B), j = 1; vsb > 0; vsb = VSize(B), ++j) {
			  Print("Round %d.1.%d: na=%d, nb=%d\n", i, j, vsa, vsb );

        DefineFE(check1) { return s.fid < d.fid; };
			  DefineMapE(update1) { d.fid = std::min(d.fid, s.fid); };
			  DefineFV(cond1) { return v.res == -1; };

			  B = EdgeMap(B, EjoinV(ED, A), check1, update1, cond1);
		  }

		  B = VertexMap(A, filter2, local2);

		  for(int vsb = VSize(B), j = 1; vsb > 0; vsb = VSize(B), ++j) {
			  Print("Round %d.2.%d: na=%d, nb=%d\n", i, j, vsa, vsb );

			  DefineFE(check2) { return s.res == d.fid; };
			  DefineMapE(update2) { d.res = d.fid; };
			  DefineFV(cond2) { return v.res == -1; };

			  B = EdgeMap(B, EjoinV(ER, A), check2, update2, cond2);
		  }

		  A = VertexMap(A, filter3);
	  }
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_SCC_H_
