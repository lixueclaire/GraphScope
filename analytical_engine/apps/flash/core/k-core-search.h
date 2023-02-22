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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_K_CORE_SEARCH_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_K_CORE_SEARCH_H_

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

namespace gs {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class KCoreSearchFlash : public FlashAppBase<FRAG_T, VALUE_T> {
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

  int32_t* Res(value_t* v) { return &(v->d); }

  void Run(const fragment_t& graph, int k) {
    Print("Run K-core search with Flash, ");
    int64_t n_vertex = graph.GetTotalVerticesNum();
    Print("k = %d, total vertices: %lld\n", k, n_vertex);

		DefineMapV(init) { v.d = Deg(id);};
		vset_t A = VertexMap(All, CTrueV, init);

		DefineFV(filter) { return v.d < k; };

		DefineFV(check) { return v.d >= k; };
		DefineMapE(update) { d.d--; };

		for(int len = VSize(A), i = 0; len > 0; len = VSize(A), ++i) {
			Print("Round %d: size=%d\n", i, len);
			A = VertexMap(A, filter);
		  A = EdgeMapDense(A, EU, CTrueE, update, check);
		}

		int s = VSize(VertexMap(All, check));
		Print( "k-core size=%d\n", s);
	}
};

}  // namespace flash
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_K_CORE_SEARCH_H_
