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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_CORE_2_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_CORE_2_H_

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

namespace gs {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class Core2Flash : public FlashAppBase<FRAG_T, VALUE_T> {
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

  short* Res(value_t* v) { return &(v->core); }

  void Run(const fragment_t& graph) {
    Print("Run K-core with Flash, ");
    int64_t n_vertex = graph.GetTotalVerticesNum();
    Print("total vertices: %lld\n", n_vertex);

    DefineMapV(init) { v.core = std::min(SHRT_MAX, Deg(id)); };
    vset_t A = VertexMap(All, CTrueV, init);

    DefineMapE(none) { };

    std::vector<int> cnt(SHRT_MAX);
    DefineMapV(local) {
      v.old = v.core;
		  int nowcnt = 0;
		  for_nb ( if (nb.core >= v.core) ++nowcnt; );
		  if (nowcnt >= v.core) return;
		  memset(cnt.data(), 0, sizeof(int) * (v.core + 1));
		  for_nb ( ++cnt[std::min(v.core, nb.core)]; );
		  for(int s = 0; s + cnt[v.core] < v.core; --v.core)
        s += cnt[v.core];
    };

    DefineFV(filter) { return v.old != v.core; };

	  for(int len = VSize(A), i = 0; len > 0; len = VSize(A), ++i) {
		  Print("Round %d: size=%d\n", i, len);
      if (len < THRESHOLD) {
			  A = EdgeMapSparse(A, EU, CTrueE, none, CTrueV);
        A = VertexMapSeq(A, CTrueV, local);
      }
		  else
			  A = VertexMapSeq(All, CTrueV, local);
      A = VertexMap(A, filter);
	  }

  	long long sum_core = 0, tot_sum_core = 0;
	  for (auto &i: All.s)
      sum_core += GetV(i)->core;
    GetSum(sum_core, tot_sum_core);
	  Print( "sum_core=%lld\n", tot_sum_core);
	}
};

}  // namespace flash
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_CORE_2_H_
