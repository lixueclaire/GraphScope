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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_MIS_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_MIS_H_

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

namespace gs {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class MISFlash : public FlashAppBase<FRAG_T, VALUE_T> {
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

  bool* Res(value_t* v) { return &(v->b); }

  void Run(const fragment_t& graph) {
    Print("Run MIS with Flash, ");
    int64_t n_vertex = graph.GetTotalVerticesNum();
    Print("total vertices: %lld\n", n_vertex);

		DefineMapV(init) { v.d = false; v.r = Deg(id) * n_vertex + id; };
		vset_t A = VertexMap(All, CTrueV, init);

		DefineMapV(local) { v.b = true; };

		DefineFE(check) {	return !s.d && s.r < d.r;	};
		DefineMapE(update) {	d.b = false;	};
		DefineFV(filter) {	return v.b;	};

		DefineMapE(update2) {	};
		DefineFV(cond) {	return !v.d; };
		DefineMapE(reduce) {	d.d = true; };

		DefineFV(filter2) {	return !v.b; };

		for(int i = 0, len = VSize(A); len > 0; ++i) {
			A = VertexMap(A, CTrueV, local);
			EdgeMapDense(All, EjoinV(EU, A), check, update, filter);

			vset_t B = VertexMap(A, filter);
			vset_t C = EdgeMapSparse(B, EU, CTrueE, update2, cond, reduce);
			A = A.Minus(C);
			A = VertexMap(A, filter2);

			int num = VSize(B); len = VSize(A);
			Print("Round %d: size=%d, selected=%d\n", i, len, num);
		}

		int n_mis = VSize(VertexMap(All, filter));
		Print( "size of max independent set = %d\n", n_mis);
	}
};

}  // namespace flash
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_MIS_H_
