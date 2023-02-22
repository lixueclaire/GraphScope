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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_AB_CORE_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_AB_CORE_H_

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

#define C(ID,V) ((ID<nx && V.d<ta) || (ID>=nx && V.d<tb))

namespace gs {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class ABCoreFlash : public FlashAppBase<FRAG_T, VALUE_T> {
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

  int* Res(value_t* v) { return &(v->d); }

  void Run(const fragment_t& graph, int ta, int tb, int nx) {
    Print("Run ab-core search with Flash, ");
    int64_t n_vertex = graph.GetTotalVerticesNum();
    Print("a = %d, b = %d, total vertices: %lld\n", ta, tb, n_vertex);

		DefineMapV(init) {v.d = Deg(id); v.c = 0;};
	  vset_t A = VertexMap(All, CTrueV, init);

	  DefineMapV(local) { v.c = 0; };
	  DefineFV(check) { return C(id, v); };
	  DefineFV(check2) { return !C(id, v); };

	  DefineMapE(update) { d.c++; };
	  DefineMapE(reduce) { d.d -= s.c; };

	  for(int len = VSize(A), i = 0; len > 0; len = VSize(A),++i) {
		  Print("Round %d: size=%d\n", i, len);
		  A = VertexMap(A, CTrueV, local);
		  A = VertexMap(A, check);
		  A = EdgeMapSparse(A, EU, CTrueE, update, check2, reduce);
	  }

	  DefineFV(filter1) { return id < nx && v.d >= ta; };
	  DefineFV(filter2) { return id >= nx && v.d >= tb; };
	  int sa = VSize(VertexMap(All, filter1));
	  int sb = VSize(VertexMap(All, filter2));
	  Print( "sa=%d,sb=%d\n", sa, sb);
	}
};

}  // namespace flash
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_AB_CORE_H_
