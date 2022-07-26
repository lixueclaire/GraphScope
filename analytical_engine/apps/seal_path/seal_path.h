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

#include "apps/seal_path/seal_path_context.h"
// #include "core/app/app_base.h"
// #include "core/worker/default_worker.h"

namespace gs {
/**
 * @brief Compute the degree centrality for vertices. The degree centrality for
 * a vertex v is the fraction of vertices it is connected to.
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class SealPath
    : public grape::ParallelAppBase<FRAG_T, SealPathContext<FRAG_T>>,
      public grape::ParallelEngine,
      public grape::Communicator {
 public:
  INSTALL_PARALLEL_WORKER(SealPath<FRAG_T>, SealPathContext<FRAG_T>, FRAG_T)
  using vid_t = typename FRAG_T::vid_t;
  using vertex_t = typename fragment_t::vertex_t;
  using path_t = typename context_t::path_t;
  using queue_t = std::queue<std::pair<vid_t, path_t>>;

  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;

  void ParallelBFS(const fragment_t& frag, context_t& ctx, message_manager_t& messages) {
    std::vector<std::thread> threads(thread_num());
    std::atomic<int> offset(0);
    for (int i = 0; i < thread_num(); ++i) {
      threads[i] = std::thread([&]() {
        while (true) {
          int got_offset = offset.fetch_add(1);
          if (got_offset >= ctx.path_queues.size()) {
            break;
          }
          if (!ctx.path_queues[got_offset].empty()) {
            auto& paths = ctx.path_queues[got_offset];
            auto& path_result = ctx.path_results[got_offset];
            while (!paths.empty()) {
              auto& pair = paths.front();
              auto target = pair.first;
              auto& path = pair.second;

              vertex_t u;
              CHECK(frag.Gid2Vertex(path[path.size() - 1], u));
              auto oes = frag.GetOutgoingAdjList(u);
              for (auto& e : oes) {
                auto v = e.neighbor();
                auto v_gid = frag.Vertex2Gid(v);
                if (v_gid == target) {
                  if (path.size() != 1) {  // ignore the src->target path
                    path_result.push_back(path);
                    path_result.back().push_back(v_gid);
                  }
                } else if (path.size() < ctx.k - 1 && std::find(path.begin(), path.end(), v_gid) == path.end()) {
                  if (frag.IsInnerVertex(v)) {
                    paths.push(pair);
                    paths.back().second.push_back(v_gid);
                  } else {
                    path_t new_path(path);
                    new_path.push_back(got_offset);
                    auto new_pair = std::make_pair(target, new_path);
                    messages.Channels()[i].SyncStateOnOuterVertex(frag, v, new_pair);
                  }
                }
              }
              paths.pop();
            }
          }
        }
      });
    }

    for (auto& thrd : threads) {
      thrd.join();
    }
  }

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    messages.InitChannels(thread_num());
    ParallelBFS(frag, ctx, messages);
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    messages.ParallelProcess<fragment_t, std::pair<vid_t, path_t>>(
        thread_num(), frag,
        [&ctx, &frag](int tid, vertex_t v, std::pair<vid_t, path_t>& msg) {
          auto offset = msg.second.back();
          auto& last = msg.second.back();
          last = frag.Vertex2Gid(v);
          ctx.path_queues[offset].push(std::move(msg));
        });
    ParallelBFS(frag, ctx, messages);
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_CENTRALITY_DEGREE_DEGREE_CENTRALITY_H_
