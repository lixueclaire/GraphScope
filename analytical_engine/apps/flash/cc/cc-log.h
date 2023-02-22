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

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_CC_LOG_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_CC_LOG_H_

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"

#define jump(A) VertexMap(A, checkj, updatej)

#define star(A) S = VertexMap(A, CTrueV, locals); \
S = VertexMap(S, checkj, locals2);\
EdgeMapSparse(S, edges2, CTrueE, updates, CTrueV, updates);\
VertexMap(A, checks, locals2);

#define hook(A) S = VertexMap(A, filterh1); \
VertexMap(S, CTrueV, localh1);\
EdgeMapSparse(S, EU, f2, h2, CTrueV, h2);\
VertexMap(S, filterh2, localh2);


namespace gs {

template <typename FRAG_T>
class CCLogFlash : public FlashAppBase<FRAG_T, CC_LOG_TYPE> {
 public:
  INSTALL_FLASH_WORKER(CCLogFlash<FRAG_T>, CC_LOG_TYPE, FRAG_T)
  using context_t = FlashVertexDataContext<FRAG_T, CC_LOG_TYPE, int>;
  bool sync_all_ = true;

  int* Res(value_t* v) { return &(v->res); }

  void Run(const fragment_t& graph) {
    int n_vertex = graph.GetTotalVerticesNum();
    Print("Run cc-log with Flash, total vertices: %d\n", n_vertex);
		vset_t S;
		bool c = true;

		DefineMapV(init) { v.p = id; v.s = false; v.f = id; };
		VertexMap(All, CTrueV, init);

		DefineFE(check1) { return sid < d.res; };
		DefineMapE(update1) { d.res = std::min(d.res, (int)sid); };
		vset_t A = EdgeMapDense(All, EU, check1, update1, CTrueV);

		DefineOutEdges(edges) { VjoinP(p) };
		DefineMapE(update2) { d.s = true; };
		EdgeMapSparse(A, edges, CTrueE, update2, CTrueV, update2);

		DefineFV(filter1) { return v.res == id && (!v.s); };
		DefineMapV(local1) { v.res = INT_MAX; };
		A = VertexMap(All, filter1, local1);
		EdgeMapDense(All, EjoinV(EU, A), check1, update1, CTrueV);

		DefineFV(filter2) { return v.res != INT_MAX; };
		A = VertexMap(All, filter2);

		DefineFV(checkj) { return GetV(v.res)->res != v.res; };
		DefineMapV(updatej) { v.res = GetV(v.res)->res; };

		DefineOutEdges(edges2) {
			std::vector<vid_t> res;
			res.clear();
			res.push_back(GetV(v.res)->res);
			return res;
		};
		DefineMapV(locals) { v.s = true; };
		DefineMapV(locals2) { v.s = false; };
		DefineMapE(updates) { d.s = false; };
		DefineFV(checks) { return (v.s && !(GetV(v.res)->s)); };

		DefineFV(filterh1) { return v.s; };
		DefineMapV(localh1) {
			v.f = c ? v.res : INT_MAX;
			for_nb( if (nb.res != v.res) v.f = std::min(v.f, nb.res); )
		};
		DefineFE(f2) { return s.res != sid && s.f != INT_MAX && s.f != s.res && s.res == did; };
		DefineMapE(h2) { d.f = std::min(d.f, s.f); };

		DefineFV(filterh2) { return v.res == id && v.f != INT_MAX && v.f != v.res; };
		DefineMapV(localh2) { v.res = v.f; };

		for (int i = 0, len = 0; VSize(A) > 0; ++i) {
			len = VSize(jump(A));
			if (len == 0) break;
			Print("Round %d,len=%d\n", i, len);
			jump(A); jump(A);
			star(A); c = true; hook(A);
			star(A); c = false; hook(A);
		}

		DefineFV(filter3) { return v.res == INT_MAX; };
		DefineMapV(local3) { v.res = id; };
		VertexMap(All, filter3, local3);
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_CC_LOG_H_
