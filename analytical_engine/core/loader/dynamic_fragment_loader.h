
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

#ifndef ANALYTICAL_ENGINE_CORE_LOADER_DYNAMIC_FRAGMENT_LOADER_H_
#define ANALYTICAL_ENGINE_CORE_LOADER_DYNAMIC_FRAGMENT_LOADER_H_

#include <memory>
#include <string>
#include <vector>

#include "grape/fragment/basic_fragment_loader.h"
#include "grape/grape.h"
#include "grape/io/local_io_adaptor.h"

#include "core/fragment/dynamic_fragment.h"

namespace gs {

class DynamicFragmentLoader {
  using fragment_t = DynamicFragment;
  using oid_t = typename fragment_t::oid_t;
  using vid_t = typename fragment_t::vid_t;
  using vdata_t = typename fragment_t::vdata_t;
  using edata_t = typename fragment_t::edata_t;
  using vertex_t = typename fragment_t::internal_vertex_t;
  using edge_t = typename fragment_t::edge_t;

  using vertex_map_t = typename fragment_t::vertex_map_t;
  using partitioner_t = typename fragment_t::partitioner_t;
  using line_parser_t = DynamicLineParser;
  using io_adaper_t = grape::LocalIOAdaptor;

 public:
  explicit DynamicFragmentLoader(const grape::CommSpec& comm_spec)
      : comm_spec_(comm_spec) {}

  std::shared_ptr<fragment_t> LoadFragment(const std::string& efile,
                                           const grape::LoadGraphSpec& spec) {
    std::shared_ptr<fragment_t> fragment(nullptr);
    auto vm_ptr = std::shared_ptr<vertex_map_t>(new vertex_map_t(comm_spec_));
    vm_ptr->Init();
    fragment = std::shared_ptr<fragment_t>(new fragment_t(vm_ptr));
    std::vector<vertex_t> vertices;
    std::vector<edge_t> edges;
    partitioner_t partitioner;
    partitioner.Init(comm_spec_.fnum());
    {
      auto io_adaptor =
          std::unique_ptr<io_adaper_t>(new io_adaper_t(std::string(efile)));
      io_adaptor->Open();
      std::string line;
      oid_t src, dst;
      edata_t e_data;
      vid_t src_gid, dst_gid;
      fid_t src_fid, dst_fid;
      vdata_t vdata;
      size_t lineNo = 0;
      while (io_adaptor->ReadLine(line)) {
        ++lineNo;
        if (lineNo % 1000000 == 0) {
          VLOG(10) << "[worker-" << comm_spec_.worker_id() << "][efile] "
                   << lineNo;
        }
        if (line.empty() || line[0] == '#')
          continue;

        try {
          line_parser_.LineParserForELine(line, src, dst, e_data);
        } catch (std::exception& e) {
          VLOG(1) << e.what();
          continue;
        }
        src_fid = partitioner.GetPartitionId(src);
        dst_fid = partitioner.GetPartitionId(dst);
        if (vm_ptr->AddVertex(src_fid, src, src_gid)) {
          vertices.emplace_back(src_gid, vdata);
        }
        if (vm_ptr->AddVertex(dst_fid, dst, dst_gid)) {
          vertices.emplace_back(dst_gid, vdata);
        }
        edges.emplace_back(src_gid, dst_gid, e_data);
        if (!spec.directed && src_gid != dst_gid) {
          edges.emplace_back(dst_gid, src_gid, e_data);
        }
      }
    }
    fragment->Init(comm_spec_.worker_id(), vertices, edges, spec.directed,
                   true);
    return fragment;
  }

 private:
  grape::CommSpec comm_spec_;
  line_parser_t line_parser_;
};
}  // namespace gs
#endif  // ANALYTICAL_ENGINE_CORE_LOADER_DYNAMIC_FRAGMENT_LOADER_H_
