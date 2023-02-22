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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_K_CLIQUE_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_K_CLIQUE_H_

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

namespace gs {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class KCliqueFlash : public FlashAppBase<FRAG_T, VALUE_T> {
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

  void Run(const fragment_t& graph, int32_t k) {
    Print("Run K-clique Counting with Flash, ");
    int32_t n_vertex = graph.GetTotalVerticesNum();
    Print("total vertices: %d, k = %d\n", n_vertex, k);

		Print("Loading\n");
		DefineMapV(init) { v.deg = Deg(id); v.count = 0; v.out.clear(); };
		VertexMap(All, CTrueV, init);

		DefineFE(check) { return (s.deg > d.deg) || (s.deg == d.deg && sid > did); };
		DefineMapE(update) { d.out.insert(sid); };
		EdgeMapDense(All, EU, check, update, CTrueV);

		Print("Computing\n");
		DefineFV(filter) { return v.out.size() >= k - 1; };
		std::function<void(std::set<int>&, int, int&)> compute=[&](std::set<int> &cand, int nowk, int &res) {
			if (nowk == k) {
				res++;
				return;
			}
			std::set<int> c;
			for(auto &u : cand) {
				int len = 0;
				c.clear();
				for (auto &o : GetV(u)->out) {
					if (cand.find(o) != cand.end()) {
						len++;
						c.insert(o);
					}
				}
				if (len < k - nowk - 1) continue;
				compute(c, nowk + 1, res);
			}
		};

		DefineMapV(local) {
			compute(v.out, 1, v.count);
		};
		VertexMapSeq(All, filter, local, false);

    int64_t cnt = 0, cnt_all = 0;
		for (auto &id: All.s) {
			cnt += GetV(id)->count;
		}
    GetSum(cnt, cnt_all);
    Print( "number of %d-cliques=%lld\n", k, cnt_all);
	}
};

}  // namespace flash
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_K_CLIQUE_H_
