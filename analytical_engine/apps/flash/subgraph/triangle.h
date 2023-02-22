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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_TRIANGLE_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_TRIANGLE_H_

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

namespace gs {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class TriangleFlash : public FlashAppBase<FRAG_T, VALUE_T> {
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

  int32_t* Res(value_t* v) { return &(v->count); }

  void Run(const fragment_t& graph) {
    Print("Run Triangle Counting with Flash, ");
    int32_t n_vertex = graph.GetTotalVerticesNum();
    Print("total vertices: %d\n", n_vertex);

		DefineMapV(init) { v.deg = Deg(id); v.count = 0; v.out.clear(); };
		VertexMap(All, CTrueV, init);

		DefineFE(check) { return (s.deg > d.deg) || (s.deg == d.deg && sid > did); };
		DefineMapE(update) { d.out.insert(sid); };

		DefineMapE(update2) {
			for (auto &x: s.out) {
				if (d.out.find(x) != d.out.end())
					d.count++;
			}
		};

		EdgeMapDense(All, EU, check, update, CTrueV);
		EdgeMapDense(All, EU, check, update2, CTrueV, false);

		int64_t cnt = 0, cnt_all = 0;
		for (auto &id: All.s) {
			cnt += GetV(id)->count;
		}
    GetSum(cnt, cnt_all);
    Print( "number of triangles=%lld\n", cnt_all);
	}
};

}  // namespace flash
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_TRIANGLE_H_
