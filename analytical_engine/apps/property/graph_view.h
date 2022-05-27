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
#include "core/worker/parallel_property_worker.h"

namespace gs {
/**
 * @brief Compute the degree centrality for vertices. The degree centrality for
 * a vertex v is the fraction of vertices it is connected to.
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class GraphView
    : public ParallelPropertyAppBase<FRAG_T, GraphViewContext<FRAG_T>>,
      public grape::ParallelEngine {
 public:
  INSTALL_PARALLEL_PROPERTY_WORKER(GraphView<FRAG_T>,
                          GraphViewContext<FRAG_T>, FRAG_T)
  using vertex_t = typename fragment_t::vertex_t;
  using label_id_t = typename fragment_t::label_id_t;
  using depth_type = typename context_t::depth_type;

  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    LOG(INFO) << "Start PEval";
    messages.InitChannels(thread_num(), 2 * 1023 * 64, 2 * 1024 * 64);
    auto& channels = messages.Channels();

    ctx.current_depth = 0;
    auto inner_vertices = frag.InnerVertices(0);
    for (int i = 0; i < ctx.path_num; i++) {
      ctx.curr_inner_updated[i].Init(inner_vertices, GetThreadPool());
      ctx.next_inner_updated[i].Init(inner_vertices, GetThreadPool());
    }
    LOG(INFO) << "start iterate sources";
    ForEach(ctx.sources,
            [&frag, &ctx, &channels](int tid, vertex_t v) {
              for (int i = 0; i < ctx.path_num; i++) {
                LOG(INFO) << "start path " << i << " vertex " << frag.GetId(v);
                label_id_t e_label = ctx.paths[i][ctx.current_depth].first;
                auto es = ctx.paths[i][ctx.current_depth].second ? frag.GetIncomingAdjList(v, e_label) : frag.GetOutgoingAdjList(v, e_label);
                if (es.Empty()) {
                  return;
                }
                if (ctx.paths[i].size() == ctx.current_depth + 1) {
                  for (auto& e : es) {
                    auto u = e.get_neighbor();
                    ctx.coloring.Insert(u);
                    if (frag.IsOuterVertex(u)) {
                      int msg = -1;
                      channels[tid].SyncStateOnOuterVertex(frag, u, msg);
                    }
                  }
                } else {
                  for (auto& e : es) {
                    auto u = e.get_neighbor();
                    if (frag.IsInnerVertex(u)) {
                      ctx.curr_inner_updated[i].Insert(u);
                    } else {
                      channels[tid].SyncStateOnOuterVertex(frag, u, i);
                    }
                  }
                }
              }
            });

    messages.ForceContinue();
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    LOG(INFO) << "Start IncEval";
    auto& channels = messages.Channels();
    ctx.current_depth += 1;

    int thrd_num = thread_num();
    for (int i = 0; i < ctx.path_num; i++) {
      ctx.next_inner_updated[i].ParallelClear(GetThreadPool());
    }
    LOG(INFO) << "Clear next inner updated.";

    // process received messages and update depth
    messages.ParallelProcess<fragment_t, int>(
        thrd_num, frag, [&frag, &ctx](int tid, vertex_t v, int msg) {
          if (msg == -1) {
            ctx.coloring.Insert(v);
          } else {
            ctx.curr_inner_updated[msg].Insert(v);
          }
        });
    LOG(INFO) << "Process messages";

    for (int i = 0; i < ctx.path_num; i++) {
      LOG(INFO) << "Process path " << i;
      ForEach(ctx.curr_inner_updated[i], [&frag, &ctx, &channels, i](
                                       int tid, vertex_t v) {
        label_id_t e_label = ctx.paths[i][ctx.current_depth].first;
        auto es = ctx.paths[i][ctx.current_depth].second ? frag.GetIncomingAdjList(v, e_label) : frag.GetOutgoingAdjList(v, e_label);
        if (es.Empty()) {
          return;
        }
        if (ctx.paths[i].size() == ctx.current_depth + 1) {
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
      LOG(INFO) << "Done Process path " << i;
    }

    for (int i = 0; i < ctx.path_num; i++) {
      if (!ctx.next_inner_updated[i].Empty()) {
        messages.ForceContinue();
      }
      ctx.next_inner_updated[i].Swap(ctx.curr_inner_updated[i]);
    }
    LOG(INFO) << "Done Swap updated ";
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_CENTRALITY_DEGREE_DEGREE_CENTRALITY_H_
