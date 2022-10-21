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

#include <cstdio>
#include <fstream>
#include <string>

#include "boost/process.hpp"
#include "boost/lexical_cast.hpp"
#include "boost/leaf.hpp"
#include "glog/logging.h"


#include "grape/grape.h"
#include "grape/util.h"
#include "vineyard/client/client.h"
#include "vineyard/graph/fragment/arrow_fragment.h"
#include "vineyard/graph/loader/arrow_fragment_writer.h"

#include "core/loader/arrow_fragment_loader.h"

namespace bl = boost::leaf;
namespace bp = boost::process;

using GraphType =
      vineyard::ArrowFragment<vineyard::property_graph_types::OID_TYPE,
                              vineyard::property_graph_types::VID_TYPE>;

void Run(vineyard::Client& client, const grape::CommSpec& comm_spec,
         vineyard::ObjectID id, const std::string& prefix) {
  std::shared_ptr<GraphType> fragment =
      std::dynamic_pointer_cast<GraphType>(client.GetObject(id));
  vineyard::WriterConfig config;
  config.prefix = prefix;
  config.vertex_chunk_size = 2050262;
  config.edge_chunk_size = 33554432;
  // config.vertex_chunk_size = 100;
  // config.edge_chunk_size = 1024;
  config.vertex_chunk_file_type = gsf::FileType::PARQUET;
  config.edge_chunk_file_type = gsf::FileType::PARQUET;
  config.adj_list_types = {gsf::AdjListType::ordered_by_source,
                           gsf::AdjListType::ordered_by_dest};
  config.yaml_output_path = config.prefix;

  auto writer = std::make_unique<
          vineyard::ArrowFragmentWriter<vineyard::property_graph_types::OID_TYPE,
                                   vineyard::property_graph_types::VID_TYPE>>(
              fragment, comm_spec, "cf", config, true);
  writer->Write();
}

bl::result<vineyard::ObjectID> Deserialize(vineyard::Client& client,
                           const std::string& serialize_path,
                           const std::string& hosts) {
  std::string deserialize_cmd = "python3 -m utils deserialize " + serialize_path + " " + client.IPCSocket() + " " + client.RPCEndpoint() + " " + hosts;
  bp::ipstream is;  // reading pipe-stream
  bp::child c(deserialize_cmd.c_str(), bp::std_out > is);

  std::string output, line;
  while (c.running() && std::getline(is, line) && !line.empty()) {
    output += line;
  }
  c.wait();
  LOG(INFO) << "All output: " << output;
  int return_value = c.exit_code();
  if (return_value != 0) {
    RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidOperationError, output.c_str());
  }
  return boost::lexical_cast<vineyard::ObjectID>(output);
}

bl::result<void>
LoadGraphFromVineyardId(const grape::CommSpec &comm_spec, vineyard::Client &client,
                        vineyard::ObjectID frag_group_id) {
  LOG(INFO) << "[worker-" << comm_spec.worker_id() << "] loaded graph "
            << frag_group_id << " to vineyard.";

  MPI_Barrier(comm_spec.comm());
  VINEYARD_DISCARD(client.SyncMetaData());

  auto fg = std::dynamic_pointer_cast<vineyard::ArrowFragmentGroup>(
      client.GetObject(frag_group_id));
  auto fid = comm_spec.WorkerToFrag(comm_spec.worker_id());
  auto frag_id = fg->Fragments().at(fid);
  auto frag = std::static_pointer_cast<GraphType>(client.GetObject(frag_id));

  // MPI_Barrier(comm_spec.comm());
  // VINEYARD_DISCARD(client.SyncMetaData());
  LOG(INFO) << "Graph Schema: " << frag->schema().ToJSONString();
  return {};
}


int main(int argc, char** argv) {
  if (argc < 6) {
    printf(
        "usage: ./test_convert <ipc_socket> <e_label_num> <efiles...> "
        "<v_label_num> <vfiles...> [directed]\n");
    return 1;
  }
  int index = 1;
  std::string ipc_socket = std::string(argv[index++]);

  int edge_label_num = atoi(argv[index++]);
  std::vector<std::string> efiles;
  for (int i = 0; i < edge_label_num; ++i) {
    efiles.push_back(argv[index++]);
  }

  int vertex_label_num = atoi(argv[index++]);
  std::vector<std::string> vfiles;
  for (int i = 0; i < vertex_label_num; ++i) {
    vfiles.push_back(argv[index++]);
  }

  int directed = 1;
  if (argc > index) {
    directed = atoi(argv[index++]);
  }
  std::string serialize_path = argv[index++];
  std::string hosts = argv[index++];

  grape::InitMPIComm();
  {
    grape::CommSpec comm_spec;
    comm_spec.Init(MPI_COMM_WORLD);

    vineyard::Client client;
    VINEYARD_CHECK_OK(client.Connect(ipc_socket));

    // using oid_t = int64_t;
    // using vid_t = vineyard::property_graph_types::VID_TYPE;

    // gs::ArrowFragmentLoader<oid_t, vid_t> loader(client, comm_spec, efiles,
    //                                              vfiles, directed != 0);
    vineyard::ObjectID vineyard_id;
    if (comm_spec.worker_id() == grape::kCoordinatorRank) {
      // BOOST_LEAF_ASSIGN(vineyard_id, Deserialize(client, serialize_path, hosts));
      vineyard_id =
          bl::try_handle_all([&]() { return Deserialize(client, serialize_path, hosts); },
                             [](const vineyard::GSError& e) {
                               LOG(FATAL) << e.error_msg;
                               return 0;
                             },
                             [](const bl::error_info& unmatched) {
                               LOG(FATAL) << "Unmatched error " << unmatched;
                               return 0;
                             });
    }
    grape::sync_comm::Bcast(vineyard_id, grape::kCoordinatorRank, comm_spec.comm());
    LOG(INFO) << "worker-" << comm_spec.worker_id() << "to vineyard id: " << " " << vineyard_id;
    LoadGraphFromVineyardId(comm_spec, client, vineyard_id);


    LOG(INFO) << "[worker-" << comm_spec.worker_id()
              << "] loaded graph to vineyard ...";

    MPI_Barrier(comm_spec.comm());

  }

  grape::FinalizeMPIComm();
  return 0;
}
