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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_BC_UNDIRECTED_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_BC_UNDIRECTED_H_

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "grape/fragment/immutable_edgecut_fragment.h"

namespace gs {
namespace flash {

template <typename FRAG_T, typename VALUE_T>
class BCUndirectedFlash : public FlashAppBase<FRAG_T, VALUE_T> {
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

  float* Res(value_t* v) { return &(v->b); }

  void Run(const fragment_t& graph, vid_t source) {
    Print("Run BC-Undirected with Flash, source = %d\n", source);
    int n_vertex = graph.GetTotalVerticesNum();
    Print("Total vertices: %d\n", n_vertex);
    vset_t a = All;
    vid_t gid;
    graph.GetVertexMap()->GetGid(source, gid);
    vid_t s = All.fw->Gid2Key(gid);

    int curLevel;

	  DefineMapV(init) {
		  if (id == s) {v.d = 0; v.c = 1; v.b = 0;}
		  else {v.d = -1; v.c = 0; v.b= 0;}
	  };
	  DefineFV(filter) {return id == s;};

	  DefineMapE(update1) { d.c += s.c; };
	  DefineFV(cond) { return v.d == -1; };
	  DefineMapE(reduce1) { d.c += s.c; };
	  DefineMapV(local) { v.d = curLevel; };

	  DefineMapE(update2) { d.b += d.c / s.c * (1 + s.b); };

	  std::function<void(vset_t&, int)> bn = [&](vset_t &S, int h) {
		  curLevel = h;
		  int sz = VSize(S);
		  if (sz == 0) return;
      Print("size=%d\n", sz);
		  vset_t T = EdgeMap(S, EU, CTrueE, update1, cond, reduce1);
		  T = VertexMap(T, CTrueV, local);
		  bn(T, h+1);
		  Print("-size=%d\n", sz);
		  curLevel = h;
		  EdgeMap(T, EjoinV(EU, S), CTrueE, update2, CTrueV);
	  };

	  vset_t S = VertexMap(All, CTrueV, init);
	  S = VertexMap(S, filter);

	  bn(S, 1);
  }
};

}  // namespace flash
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_BC_UNDIRECTED_H_
