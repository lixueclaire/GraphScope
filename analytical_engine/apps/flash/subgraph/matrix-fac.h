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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_MATRIX_FAC_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_MATRIX_FAC_H_

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

namespace gs {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class MatrixFacFlash : public FlashAppBase<FRAG_T, VALUE_T> {
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

  float* Res(value_t* v) { return &(v->val[0]); }

  void Run(const fragment_t& graph, int d) {
    Print("Run Matrix-fac with Flash, ");
    int32_t n_vertex = graph.GetTotalVerticesNum();
    Print("total vertices: %d\n", n_vertex);

		float alpha=0.0002, beta=0.02;

		DefineMapV(init) {
			v.val.resize(d);
			for	(int i = 0; i < d; ++i) v.val[i] = rand() * 1.0 / RAND_MAX;
			return v;
		};
		VertexMap(All, CTrueV, init);

		DefineMapV(update) {
			for_nb(
				float w = weight - prod(v.val, nb.val);
				mult(v.val, 1 - alpha * beta);
				add(v.val, nb.val, 2.0f * alpha * w);
			);
		};

		for(int i = 0; i < 100; ++i) {
			Print("Round %d\n", i);
			VertexMap(All, CTrueV, update);
		}
	}
};

}  // namespace flash
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_MATRIX_FAC_H_
