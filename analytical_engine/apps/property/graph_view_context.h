/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef ANALYTICAL_ENGINE_APPS_GRAPH_VIEW_GRAPH_VIEW_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_GRAPH_VIEW_GRAPH_VIEW_CONTEXT_H_

#include "rapidjson/document.h"
#include "grape/grape.h"

#include "apps/boundary/utils.h"
#include "core/app/parallel_property_app_base.h"
#include "core/context/tensor_context.h"

namespace gs {
template <typename FRAG_T>
class GraphViewContext
    : public TensorContext<FRAG_T, typename FRAG_T::oid_t> {
 public:
  using label_id_t = int;
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using vertex_t = typename FRAG_T::vertex_t;
  using depth_type = int64_t;

  explicit GraphViewContext(const FRAG_T& fragment)
      : TensorContext<FRAG_T, oid_t>(fragment) {}

  void Init(ParallelPropertyMessageManager& messages, const std::string& source_str,
      const std::string& path_str) {
    auto& frag = this->fragment();
    rapidjson::Document source_array, path_array;
    source_array.Parse(source_str);
    vertex_t v;
    for (auto& s : source_array.GetArray()) {
      if (frag.GetVertex(0, dynamic_to_oid<oid_t>(s), v) && frag.IsInnerVertex(v)) {
        sources.push_back(v);
      }
    }
    path_array.Parse(path_str);
    path_num = path_array.Size();
    paths.resize(path_array.Size());
    int i = 0;
    for (auto& path : path_array.GetArray()) {
      for (auto& relation : path.GetArray()) {
        std::string relation_name = relation.GetString();
        if (relation_name[0] == '^') {
          label_id_t label_id = frag.schema().GetEdgeLabelId(relation_name.substr(1));
          paths[i].emplace_back(label_id, true);
        } else {
          label_id_t label_id = frag.schema().GetEdgeLabelId(relation_name);
          paths[i].emplace_back(label_id, false);
        }
      }
      ++i;
    }
    curr_inner_updated.resize(path_num);
    next_inner_updated.resize(path_num);
    coloring.Init(frag.Vertices(0));
  }

  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices(0);

    for (auto& v : inner_vertices) {
      if (coloring.Exist(v)) {
        os << frag.GetId(v) << std::endl;
      }
    }
  }

  std::vector<vertex_t> sources;
  std::vector<std::vector<std::pair<label_id_t, bool>>> paths;
  grape::DenseVertexSet<typename FRAG_T::vertices_t> coloring;
  std::vector<grape::DenseVertexSet<typename FRAG_T::inner_vertices_t>> curr_inner_updated,
      next_inner_updated;
  depth_type current_depth;
  size_t path_num;
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_GRAPH_VIEW_GRAPH_VIEW_CONTEXT_H_
