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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_EGO_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_EGO_H_

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

namespace gs {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class EgoFlash : public FlashAppBase<FRAG_T, VALUE_T> {
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

 	int* Res(value_t* v) { return &(v->deg); }

  void Run(const fragment_t& graph) {
    Print("Run ego-net with Flash, ");
    int64_t n_vertex = graph.GetTotalVerticesNum();
    Print("total vertices: %lld\n", n_vertex);

		int n_itr = 10;

		DefineMapV(init) {
			v.deg = Deg(id);
			v.ego.resize(v.deg);
		};
		VertexMap(All, CTrueV, init);

		int r;
		DefineMapV(local) {
			v.out.clear();
			for_nb(
				if (nb_id % n_itr == r && (nb.deg > v.deg || (nb.deg == v.deg && nb_id > id)))
					v.out.push_back(nb_id); );
		};

		std::vector<int> p(n_vertex, -1);
		DefineMapV(update) {
			int idx = 0;
			for_nb( p[nb_id] = idx++; );
			idx = 0;
			for_nb(
				for(auto u:nb.out)
					if (p[u] >= 0) {
						v.ego[idx].push_back(p[u]);
						v.ego[p[u]].push_back(idx);
					}
				++idx; );
			idx = 0;
			for_nb(
				p[nb_id] = -1;
				sort(v.ego[idx].begin(), v.ego[idx].end());
				std::vector<int>(v.ego[idx]).swap(v.ego[idx]);
				++idx; );
		};

		for (r = 0; r < n_itr; ++r) {
			Print( "Round %d, Loading...\n", r+1 );
			VertexMap(All, CTrueV, local);
			Print( "Computing...\n" );
			VertexMapSeq(All, CTrueV, update, false);
		}
	}
};

}  // namespace flash
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_EGO_H_
