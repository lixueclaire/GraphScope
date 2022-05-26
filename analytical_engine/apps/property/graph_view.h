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

#include "grape/grape.h"

#include "apps/property/graph_view_context.h"

namespace gs {
/**
 * @brief Compute the degree centrality for vertices. The degree centrality for
 * a vertex v is the fraction of vertices it is connected to.
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class GraphView
    : public grape::ParallelAppBase<FRAG_T, GraphViewContext<FRAG_T>>,
      public grape::ParallelEngine {
 public:
  INSTALL_PARALLEL_WORKER(GraphView<FRAG_T>,
                          GraphViewContext<FRAG_T>, FRAG_T)
  using vertex_t = typename fragment_t::vertex_t;
  using label_id_t = typename fragment_t::label_id_t;
  using depth_type = typename context_t::depth_type;

  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    messages.InitChannels(thread_num(), 2 * 1023 * 64, 2 * 1024 * 64);

    ctx.current_depth = 0;
    for (int i = 0; i < ctx.path_num; i++) {
      ctx.curr_inner_updated[i].Init(inner_vertices, GetThreadPool());
      ctx.next_inner_updated[i].Init(inner_vertices, GetThreadPool());
    }

    ForEach(ctx.sources,
            [&frag, &ctx](int tid, vertex_t v) {
              for (size_t i = 0; i < paths.size(); i++) {
                label_id_t e_label = paths[i][ctx.current_depth].first;
                auto es = paths[i][ctx.current_depth].second ? frag.GetIncomingAdjList(v) : frag.GetOutgoingAdjList(v);
                if (es.empty()) {
                  return;
                }
                if (path[i].size() == ctx.current_depth + 1) {
                  for (auto& e : es) {
                    auto u = e.get_neighbor();
                    ctx.coloring.Insert(u);
                    if (frag.IsOuterVertex(u)) {
                      int msg = -1;
                      messages.SyncStateOnOuterVertex(frag, u, msg);
                    }
                  }
                } else {
                  for (auto& e : es) {
                    auto u = e.get_neighbor();
                    if (frag.IsInnerVertex(u)) {
                      ctx.curr_inner_updated[i].Insert(u);
                    } else {
                      messages.SyncStateOnOuterVertex(frag, u, i);
                    }
                  }
                }
              }
            });
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    auto& channels = messages.Channels();
    ctx.current_depth += 1;

    int thrd_num = thread_num();
    for (int i = 0; i < ctx.path_num; i++) {
      ctx.next_inner_updated[i].ParallelClear(GetThreadPool());
    }

    // process received messages and update depth
    messages.ParallelProcess<fragment_t, int>(
        thrd_num, frag, [&ctx](int tid, vertex_t v, int msg) {
          if (msg == -1) {
            ctx.coloring.Insert(v);
          } else {
            ctx.curr_inner_updated[msg].Insert(v);
          }
        });

    for (size_t i = 0; i < ctx.path_num; i++) {
      ForEach(ctx.curr_inner_updated[i], [&frag, &ctx, &channels](
                                       int tid, vertex_t v) {
        label_id_t e_label = paths[i][ctx.current_depth].first;
        auto es = paths[i][ctx.current_depth].second ? frag.GetIncomingAdjList(v) : frag.GetOutgoingAdjList(v);
        if (es.empty()) {
          return;
        }
        if (path[i].size() == ctx.current_depth + 1) {
          for (auto& e : es) {
            auto u = e.get_neighbor();
            if (!ctx.coloring.Exist(u)) {
              ctx.coloring.Insert(u);
              if (frag.IsOuterVertex(u)) {
                int msg = -1;
                channels[tid].SyncStateOnOuterVertex(frag, u, msg);
              }
            }
          }
        } else {
          for (auto& e : es) {
            auto u = e.get_neighbor();
            if (frag.IsInnerVertex(u)) {
              ctx.next_inner_updated[i].Insert(u);
            } else {
              channels[tid].SyncStateOnOuterVertex(frag, u, i);
            }
          }
        }
      });
    }

    for (int i = 0; i < ctx.path_num; i++) {
      if (!ctx.next_inner_updated[i].Empty()) {
        message.ForceContinue();
      }
      ctx.next_inner_updated[i].Swap(ctx.curr_inner_updated[i]);
    }
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_CENTRALITY_DEGREE_DEGREE_CENTRALITY_H_
