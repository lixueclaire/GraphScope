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
  using msg_t = std::pair<vid_t, path_t>;

  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;

  std::string PrintPath(const fragment_t& frag, const path_t& path) {
    std::string buf;

    for (auto gid : path) {
      buf += std::to_string(frag.Gid2Oid(gid)) + " ";
    }
    return buf;
  }

  void ParallelBFS(const fragment_t& frag, context_t& ctx, message_manager_t& messages) {
    int thrd_num = thread_num();
    auto& channels = messages.Channels();
    std::vector<std::thread> threads(thrd_num);
    std::atomic<int> offset(0);
    for (int i = 0; i < thrd_num; ++i) {
      threads[i] = std::thread([&](int tid) {
        while (true) {
          int got_offset = offset.fetch_add(1);
          if (got_offset >= ctx.path_queues.size()) {
            break;
          }
          auto& paths = ctx.path_queues[got_offset];
          auto& path_result = ctx.path_results[got_offset];
          std::set<vid_t> filter_set;
          while (!paths.empty()) {
            auto& pair = paths.front();
            auto target = pair.first;
            auto& path = pair.second;

            vertex_t u;
            CHECK(frag.Gid2Vertex(path[path.size() - 1], u));
            auto oes = frag.GetOutgoingAdjList(u);
            filter_set.clear();
            for (auto& e : oes) {
              auto v = e.neighbor();
              if (filter_set.find(v.GetValue()) != filter_set.end()) {
                continue;
              }
              filter_set.insert(v.GetValue());
              auto v_gid = frag.Vertex2Gid(v);
              if (v_gid == target) {
                if (path.size() != 1) {  // ignore the src->target path
                  path_result.push_back(path);
                  path_result.back().push_back(v_gid);
                }
                if (path_result.size() >= ctx.n) {
                  // the result num is enough, clear the path queue.
                  queue_t empty;
                  std::swap(paths, empty );
                  break;
                }
              } else if (path.size() < ctx.k - 1 && std::find(path.begin(), path.end(), v_gid) == path.end()) {
                if (frag.IsInnerVertex(v)) {
                  paths.push(pair);
                  paths.back().second.push_back(v_gid);
                } else {
                  path_t new_path(path);
                  new_path.push_back(got_offset);
                  auto new_pair = std::make_pair(target, new_path);
                  channels[tid].SyncStateOnOuterVertex(frag, v, new_pair);
                }
              }
            }
            if (!paths.empty()) {
              paths.pop();
            }
          }
        }
      },
      i);
    }

    for (auto& thrd : threads) {
      thrd.join();
    }
  }

  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    messages.InitChannels(thread_num());
    ParallelBFS(frag, ctx, messages);
    messages.ForceContinue();
  }

  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    messages.ParallelProcess<fragment_t, std::pair<vid_t, path_t>>(
        1, frag,
        [&ctx, &frag](int tid, vertex_t v, std::pair<vid_t, path_t>& msg) {
          auto offset = msg.second.back();
          auto& last = msg.second.back();
          last = frag.Vertex2Gid(v);
          ctx.path_queues[offset].push(msg);
        });
    pruningQueue(ctx);
    if (checkToContinue(ctx)) {
      ParallelBFS(frag, ctx, messages);
      messages.ForceContinue();
    } else {
      writeToCtx(frag, ctx);
    }
  }

 private:
  void pruningQueue(context_t& ctx) {
    for (size_t i = 0; i < ctx.path_results.size(); ++i) {
      size_t total_path_num = 0;
      Sum(ctx.path_results[i].size(), total_path_num);
      if (total_path_num >= ctx.n) {
        // the path num of pair-i is enough, cleaning the queue
        queue_t empty;
        std::swap(ctx.path_queues[i], empty);
      }
    }
  }

  bool checkToContinue(context_t& ctx) {
    int to_continue = 0;
    for (auto& queue : ctx.path_queues) {
      if (!queue.empty()) {
        to_continue = 1;
        break;
      }
    }
    int continue_num = 0;
    Sum(to_continue, continue_num);
    return continue_num;
  }

  void writeToCtx(const fragment_t& frag, context_t& ctx) {
    size_t path_num = 0;
    for (auto& result : ctx.path_results) {
      path_num += result.size();
    }
    std::vector<std::string> data;
    data.reserve(path_num);
    for (auto& result : ctx.path_results) {
      for (auto& path : result) {
        std::string path_str = std::to_string(frag.Gid2Oid(path[0])) + "," + std::to_string(frag.Gid2Oid(path.back())) + ":";
        for (size_t i = 1; i < path.size() - 2; ++i) {
          path_str += std::to_string(frag.Gid2Oid(path[i])) + ",";
        }
        path_str += std::to_string(frag.Gid2Oid(path[path.size() - 2])) + ":" + std::to_string(path.size() - 1);
        data.push_back(std::move(path_str));
      }
    }
    std::vector<size_t> shape{path_num};
    ctx.assign(data, shape);
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_CENTRALITY_DEGREE_DEGREE_CENTRALITY_H_
