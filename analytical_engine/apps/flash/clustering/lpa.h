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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_LPA_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_LPA_H_

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

namespace gs {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class LPAFlash : public FlashAppBase<FRAG_T, VALUE_T> {
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

  int* Res(value_t* v) { return &(v->c); }

  void Run(const fragment_t& graph) {
    Print("Run LPA with Flash, ");
    int64_t n_vertex = graph.GetTotalVerticesNum();
    Print("total vertices: %lld\n", n_vertex);

		DefineMapV(init) { v.c = VDATA(id); v.cc = -1; v.s.clear(); };
		vset_t A = VertexMap(All, CTrueV, init);

		DefineMapE(update) { d.s.push_back(s.c); };

		DefineMapV(local1) {
			std::map<int, int> cnt;
			int max_cnt = 0;
			for (auto &i:v.s) {
				cnt[i]++;
				if (cnt[i] > max_cnt) {
					max_cnt = cnt[i];
					v.cc = i;
				}
			}
			v.s.clear();
		};

		DefineFV(filter) { return v.cc != v.c; };
		DefineMapV(local2) { v.c = v.cc; };

		for	(int len = VSize(A), i = 0; i < 10 & len > 0; len = VSize(A), ++i) {
			Print("Round %d: size=%d\n", i, len);
			A = EdgeMapDense(All, EU, CTrueE, update, CTrueV, false);
			A = VertexMap(All, CTrueV, local1, false);
			A = VertexMap(All, filter, local2);
		}
	}
};

}  // namespace flash
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_LPA_H_
