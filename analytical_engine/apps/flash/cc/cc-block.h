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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_CC_BLOCK_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_CC_BLOCK_H_

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"

namespace gs {

template <typename FRAG_T>
class CCBlockFlash : public FlashAppBase<FRAG_T, CC_TYPE> {
 public:
  INSTALL_FLASH_WORKER(CCBlockFlash<FRAG_T>, CC_TYPE, FRAG_T)
  using context_t = FlashVertexDataContext<FRAG_T, CC_TYPE, int>;

  int* Res(value_t* v) { return &(v->res); }

  void Run(const fragment_t& graph) {
    int n_vertex = graph.GetTotalVerticesNum();
    Print("Run CC-Block with Flash, total vertices: %d\n", n_vertex);

		union_find f(n_vertex), cc;
		DefineMapV(local) { for_out(union_f(f, id, nb_id);); };

		VertexMapSeq(All, CTrueV, local, false);

		Block(Reduce(f, cc, for_i(union_f(cc, f[i], i)), true));

    for(int i = 0; i < n_vertex; ++i) {
		  int fi = get_f(cc, i);
		  cc[i] = fi;
	  }

		DefineMapV(local2) { v.res = cc[id]; };

		VertexMapSeq(All, CTrueV, local2, false);
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_CC_BLOCK_H_
