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

#ifndef ANALYTICAL_ENGINE_APPS_SEAL_SEAL_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_SEAL_SEAL_CONTEXT_H_

#include <limits>
#include <queue>
#include <string>

#include "nlohmann/json.hpp"

#include "grape/grape.h"

#include "core/context/tensor_context.h"

using json = nlohmann::json;

namespace gs {

template <typename FRAG_T>
class SealPathContext : public TensorContext<FRAG_T, typename FRAG_T::oid_t> {
 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using vertex_t = typename FRAG_T::vertex_t;
  using path_t = std::vector<vid_t>;
  using label_t = typename FRAG_T::label_id_t;

  explicit SealPathContext(const FRAG_T& fragment)
      : TensorContext<FRAG_T, typename FRAG_T::oid_t>(fragment) {}

  void Init(grape::DefaultMessageManager& messages, std::string pairs,
            int k, int n) {
    auto& frag = this->fragment();
    fid_t fid = frag.fid();
    auto vm_ptr = frag.GetVertexMap();
    auto vertices = frag.Vertices();
    vid_t src, dst;

    auto pair_json = json::parse(pairs.c_str());
    if (!pair_json.empty()) {
      for (auto& pair : pairs_json) {
        if (pair.is_array() && pair.size == 2) {
          if (vm_ptr->GetGid(fid, pair[0].get<oid_t>(), src) &&
              vm_ptr->GetGid(pair[1].get<oid_t>(), dst)) {
            auto new_pair = std::make_pair(dst, {src});
            this->paths.push(new_pair);
          }
        } else {
          LOG(ERROR) << "Invalid pair: " << pair.dump();
        }
      }
    }
    visited.Init(vertices, false);

    this->k = k;
    this->n = n;

#ifdef PROFILING
    preprocess_time = 0;
    exec_time = 0;
    postprocess_time = 0;
#endif
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();

    for (auto& path : path_result) {
      std::string buf;

      for (auto gid : path) {
        buf += std::to_string(frag.Gid2Oid(gid)) + " ";
      }
      if (!buf.empty()) {
        buf[buf.size() - 1] = '\n';
        os << buf;
      }
    }

#ifdef PROFILING
    VLOG(2) << "preprocess_time: " << preprocess_time << "s.";
    VLOG(2) << "exec_time: " << exec_time << "s.";
    VLOG(2) << "postprocess_time: " << postprocess_time << "s.";
#endif
  }

  // std::vector<std::pair<vid_t, vid_t>> pairs;
  std::queue<std::pair<vid_t, path_t> paths;
  int k, n;
  std::vector<path_t> path_result;

#ifdef PROFILING
  double preprocess_time = 0;
  double exec_time = 0;
  double postprocess_time = 0;
#endif

};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_SEAL_SEAL_CONTEXT_H_
