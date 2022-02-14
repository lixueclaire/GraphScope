#ifndef ANALYTICAL_ENGINE_CORE_FRAGMENT_DYNAMIC_FRAGMENT_POC_H_
#define ANALYTICAL_ENGINE_CORE_FRAGMENT_DYNAMIC_FRAGMENT_POC_H_

#include <memory>

#include "grape/fragment/basic_fragment_mutator.h"
#include "grape/fragment/mutable_edgecut_fragment.h"

#include "core/object/dynamic.h"
#include "core/utils/partitioner.h"

namespace gs {

class DynamicFragmentPoc {
 public:
  using oid_t = dynamic::Value;
  using vid_t = vineyard::property_graph_types::VID_TYPE;
  using edata_t = dynamic::Value;
  using vdata_t = dynamic::Value;
  using vertex_map_t = grape::GlobalVertexMap<oid_t, vid_t>;
  using fragment_t = grape::MutableEdgecutFragment<oid_t, vid_t, vdata_t, edata_t, grape::LoadStrategy::kOnlyOut, vertex_map_t>;
  using internal_vertex_t = typename fragment_t::internal_vertex_t;
  using vertex_t = typename fragment_t::vertex_t;
  using edge_t = typename fragment_t::edge_t;
  //  using fid_t = typename fragment_t::fid_t;
  using fid_t = int;
  using vertices_t = typename fragment_t::vertices_t;
  using adj_list_t = typename fragment_t::adj_list_t;
  using const_adj_list_t = typename fragment_t::const_adj_list_t;
  using inner_vertices_t = typename fragment_t::inner_vertices_t;
  using outer_vertices_t = typename fragment_t::outer_vertices_t;
  using mutation_t = grape::Mutation<vid_t, vdata_t, edata_t>;
  using partitioner_t = typename vertex_map_t::partitioner_t;

  explicit DynamicFragmentPoc(std::shared_ptr<vertex_map_t> vm_ptr) : vm_ptr_(vm_ptr) {
      frag_ptr_ = std::make_shared<fragment_t>(vm_ptr);
  }
  ~DynamicFragmentPoc() = default;

  void Init(fid_t fid, std::vector<internal_vertex_t>& vertices,
            std::vector<edge_t>& edges, bool directed) {
    directed ? frag_ptr_->Init(fid, directed, vertices, edges, grape::LoadStrategy::kBothOutIn)
             : frag_ptr_->Init(fid, directed, vertices, edges, grape::LoadStrategy::kOnlyOut);
  }

  void Init(fid_t fid, bool directed) {
    std::vector<internal_vertex_t> empty_vertices;
    std::vector<edge_t> empty_edges;

    Init(fid, empty_vertices, empty_edges, directed);
  }

  fid_t fid() const { return frag_ptr_->fid(); }

  fid_t fnum() const { return frag_ptr_->fnum(); }

  bool directed() const { return frag_ptr_->directed(); }

  vid_t GetEdgeNum() const { return frag_ptr_->GetEdgeNum(); }

  vid_t GetVerticesNum() const { return frag_ptr_->GetVerticesNum(); }

  size_t GetTotalVerticesNum() const { return frag_ptr_->GetTotalVerticesNum(); }

  vid_t GetInnerVerticesNum() const { return frag_ptr_->GetInnerVerticesNum(); }

  vid_t GetOuterVerticesNum() const { return frag_ptr_->GetOuterVerticesNum(); }

  const vertices_t& Vertices() const { return frag_ptr_->Vertices(); }

  const inner_vertices_t& InnerVertices() const { return frag_ptr_->InnerVertices(); }

  const outer_vertices_t& OuterVertices() const { return frag_ptr_->OuterVertices(); }

  bool IsInnerVertex(const vertex_t& v) const { return frag_ptr_->IsInnerVertex(v); }

  bool IsOuterVertex(const vertex_t& v) const { return frag_ptr_->IsOuterVertex(v); }

  bool GetVertex(const oid_t& oid, vertex_t& v) const { return frag_ptr_->GetVertex(oid, v); }

  bool GetInnerVertex(const oid_t& oid, vertex_t& v) const {
    return frag_ptr_->GetInnerVertex(oid, v);
  }

  oid_t GetId(const vertex_t&v ) const {
    auto gid = frag_ptr_->Vertex2Gid(v);
    oid_t oid;
    vm_ptr_->GetOid(gid, oid);
    return oid;
  }

  oid_t Gid2Oid(vid_t gid) const {
    oid_t oid;
    vm_ptr_->GetOid(gid, oid);
    return oid;
  }

  fid_t GetFragId(const vertex_t& v) const {
    return frag_ptr_->GetFragId(v);
  }

  const vdata_t& GetData(const vertex_t& v) const {
    return frag_ptr_->GetData(v);
  }

  void SetData(const vertex_t&v, const vdata_t& val) {
    frag_ptr_->SetData(v, val);
  }

  int GetLocalOutDegree(const vertex_t& v) const {
    return frag_ptr_->GetLocalOutDegree(v);
  }

  int GetLocalInDegree(const vertex_t& v) const {
    return frag_ptr_->GetLocalInDegree(v);
  }

  inline adj_list_t GetIncomingAdjList(const vertex_t& v) {
    return frag_ptr_->GetIncomingAdjList(v);
  }

  inline adj_list_t GetIncomingAdjList(const vertex_t& v) const {
    return frag_ptr_->GetIncomingAdjList(v);
  }

  inline adj_list_t GetOutgoingAdjList(const vertex_t& v) {
    return frag_ptr_->GetOutgoingAdjList(v);
  }

  inline adj_list_t GetOutgoingAdjList(const vertex_t& v) const {
    return frag_ptr_->GetOutgoingAdjList(v);
  }

  bool Gid2Vertex(const vid_t& gid, vertex_t& v) const {
    return frag_ptr_->Gid2Vertex(gid, v);
  }

  void ModifyEdges(dynamic::Value& edges_to_modify,
                   const dynamic::Value& common_attrs,
                   const rpc::ModifyType modify_type,
                   const std::string weight) {
    {
      edata_t e_data;
      vdata_t fake_data(rapidjson::kObjecteType);
      oid_t src, dst;
      vid_t src_gid, dst_gid;
      fid_t src_fid, dst_fid;
      // auto& partitioner = vm_ptr_->GetPartitioner();
      partitioner_t partitioner(fnum());
      mutation_t mutation;
      for (auto& e : edges_to_modify) {
        // the edge could be [src, dst] or [srs, dst, value] or [src, dst,
        // {"key": val}]
        e_data = common_attrs;
        if (e.Size() == 3) {
          if (weight.empty()) {
            e_data.Update(edata_t(e[2]));
          } else {
            e_data.Insert(weight, edata_t(e[2]));
          }
        }
        src = std::move(e[0]);
        dst = std::move(e[1]);
        src_fid = partitioner.GetPartitionId(src);
        dst_fid = partitioner.GetPartitionId(dst);
        if (modify_type == rpc::NX_ADD_EDGES) {
          bool src_existed = vm_ptr_->AddVertex(src, src_gid);
          bool dst_existed = vm_ptr_->AddVertex(dst, dst_gid);
          if (src_fid == fid() && !src_existed) {
            mutation.vertices_to_add.emplace_back(src_gid);
          }
          if (dst_fid == fid() && !dst_existed) {
            mutation.vertices_to_add.emplace_back(dst_gid, fake_data);
          }
        } else {
          if (!vm_ptr_->GetGid(src_fid, src, src_gid) ||
              !vm_ptr_->GetGid(dst_fid, dst, dst_gid)) {
            continue;
          }
        }
        if (modify_type == rpc::NX_ADD_EDGES) {
          if (src_fid == fid() || dst_fid == fid()) {
            mutation.edges_to_add.emplace_back(src_gid, dst_gid, e_data);
            if (!frag_ptr_->directed()) {
              mutation.edges_to_add.emplace_back(dst_gid, src_gid, e_data);
            }
          }
        } else if (modify_type == rpc::NX_DEL_EDGES) {
          if (src_fid == fid() || dst_fid == fid()) {
            mutation.edges_to_remove.emplace_back(src_gid, dst_gid);
            if (!frag_ptr_->directed()) {
              mutation.edges_to_remove.emplace_back(dst_gid, src_gid);
            }
          }
        } else if (modify_type == rpc::NX_UPDATE_EDGES) {
          if (src_fid == fid() || dst_fid == fid()) {
            mutation.edges_to_update.emplace_back(src_gid, dst_gid, e_data);
            if (!frag_ptr_->directed()) {
              mutation.edges_to_update.emplace_back(dst_gid, src_gid, e_data);
            }
          }
        }
      }
      frag_ptr_->Mutate(mutation);
    }
  }

  void ModifyVertices(dynamic::Value& vertices_to_modify,
                      const dynamic::Value& common_attrs,
                      const rpc::ModifyType& modify_type) {
    {
      mutation_t mutation;
      // auto& partitioner = vm_ptr_->GetPartitioner();
      partitioner_t partitioner(fnum());
      oid_t oid;
      vid_t gid;
      vdata_t v_data;
      fid_t v_fid;
      for (auto& v : vertices_to_modify) {
        v_data = common_attrs;
        // v could be [id, attrs] or id
        if (v.IsArray() && v.Size() == 2 && v[1].IsObject()) {
          oid = std::move(v[0]);
          v_data.Update(vdata_t(v[1]));
        } else {
          oid = std::move(v);
        }
        v_fid = partitioner.GetPartitionId(oid);
        if (modify_type == rpc::NX_ADD_NODES) {
          bool v_existed = vm_ptr_->AddVertex(oid, gid);
          if (v_fid == fid() && !v_existed) {
            mutation.vertices_to_add.emplace_back(gid, std::move(v_data));
          }
        } else {
          // UPDATE or DELETE, if not exist the node, continue.
          if (!vm_ptr_->GetGid(v_fid, oid, gid)) {
            continue;
          }
        }
        if (modify_type == rpc::NX_UPDATE_NODES && v_fid == fid()) {
          mutation.vertices_to_update.emplace_back(gid, v_data);
        }
        if (modify_type == rpc::NX_DEL_NODES && v_fid == fid()) {
          mutation.vertices_to_remove.emplace_back(gid);
        }
      }
      frag_ptr_->Mutate(mutation);
    }
  }

 protected:
   std::shared_ptr<vertex_map_t> vm_ptr_;
   std::shared_ptr<fragment_t> frag_ptr_;
};

} // namespace gs

#endif