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

#ifndef ANALYTICAL_ENGINE_APPS_CENTRALITY_DEGREE_DEGREE_CENTRALITY_H_
#define ANALYTICAL_ENGINE_APPS_CENTRALITY_DEGREE_DEGREE_CENTRALITY_H_

#include <string>
#include <vector>

#include "grape/grape.h"

#include "apps/seal/seal_context.h"
#include "core/worker/parallel_property_worker.h"

namespace gs {
/**
 * @brief Compute the degree centrality for vertices. The degree centrality for
 * a vertex v is the fraction of vertices it is connected to.
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class SealPath
    : public ParallelPropertyAppBase<FRAG_T, SealPathContext<FRAG_T>>,
      public grape::ParallelEngine,
      public grape::Communicator {
 public:
  INSTALL_PARALLEL_PROPERTY_WORKER(SealPath<FRAG_T>,
                          SealPathContext<FRAG_T>, FRAG_T)
  using vertex_t = typename fragment_t::vertex_t;
  using label_id_t = typename fragment_t::label_id_t;

  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;

  void BFS(const fragment_t& frag, context_t& ctx, message_manager_t& messages,
           std::queue<std::pair<vid_t, path_t>>& paths) {
    auto& path_result = ctx.path_result;

    while (!paths.empty()) {
      auto& pair = paths.front();
      auto target = pair.first;
      auto& path = pair.second;

      vertex_t u;
      CHECK(frag.Gid2Vertex(path[path.size() - 1], u));
      for (label_id_t e_label; e_label < frag.edge_label_num(); e_label++) {
        auto oes = frag.GetOutgoingAdjList(u, curr_e_label);
        for (auto& e : oes) {
          auto v = e.neighbor();
          auto v_gid = frag.Vertex2Gid(v);
          if (v_gid == target) {
            if (path.size() != 1) {
              path_result.push_back(path);
              path_result.back().push_back(v_gid);
            }
          } else if (path.size() < ctx.k - 1 && ) {
            if (frag.IsInnerVertex(v)) {
              paths.push(pair);
              paths.back().second.push_back(v_gid);
            } else {
              messages.SyncStateOnOuterVertex(frag, v, pair);
            }
          }
        }
      }
      paths.pop();
    }
  }

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    BFS(frag, ctx, messages, ctx.paths);
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    auto& paths = ctx.paths;

    {
      vertex_t v;
      vid_t gid;
      std::pair<vid_t, path_t> msg;

      while (messages.GetMessage(v, msg)) {
        gid = frag.Vertex2Gid(v);
        msg.second.push_back(gid);
        paths.push(std::move(msg));
      }
    }

    BFS(frag, ctx, messages, paths);
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_CENTRALITY_DEGREE_DEGREE_CENTRALITY_H_
