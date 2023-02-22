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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_MSF_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_MSF_H_

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

namespace gs {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class MSFFlash : public FlashAppBase<FRAG_T, VALUE_T> {
 public:
  using fragment_t = FRAG_T;
  using vid_t = typename fragment_t::vid_t;
  using vertex_t = typename fragment_t::vertex_t;
  using value_t = VALUE_T;
  using vdata_t = typename fragment_t::vdata_t;
  using edata_t = typename fragment_t::edata_t;
  using adj_list_t = typename fragment_t::adj_list_t;
  using vset_t = VertexSubset<fragment_t, value_t>;
  using E = std::pair<edata_t, std::pair<vid_t, vid_t> >;
  bool sync_all_ = false;
  float wt = 0;

  float* Res(value_t* v) { return &(wt); }

  void Run(const fragment_t& graph) {
    Print("Run MSF with Flash, ");
    int n_vertex = graph.GetTotalVerticesNum();
    Print("total vertices: %d\n", n_vertex);

    std::vector<E> edges, mst0(n_vertex - 1), mst;
	  for (auto &id: All.s) {
		  for_out(
        edges.push_back(std::make_pair(weight, std::make_pair(id, nb_id)));
      );
    }

	  kruskal<E>(edges, mst0.data(), n_vertex);
	  Reduce(
      mst0, mst, std::vector<E> edges;
      edges.assign(mst0, mst0 + len);
      edges.insert(edges.end(), mst, mst + len);
      kruskal<E>(edges, mst, len + 1)
    );

    for (auto &e: mst)
      wt += e.first;
	  Print("mst weight = %0.3f\n", wt);
  }
};

}  // namespace flash
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_MSF_H_
