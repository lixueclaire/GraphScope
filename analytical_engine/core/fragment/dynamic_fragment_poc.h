#ifndef ANALYTICAL_ENGINE_CORE_FRAGMENT_DYNAMIC_FRAGMENT_POC_H_
#define ANALYTICAL_ENGINE_CORE_FRAGMENT_DYNAMIC_FRAGMENT_POC_H_

#include <memory>

#include "grape/communication/communicator.h"
#include "grape/fragment/basic_fragment_mutator.h"
#include "grape/fragment/csr_edgecut_fragment_base.h"

#include "core/fragment/de_mutable_csr.h"
#include "core/object/dynamic.h"
#include "core/utils/partitioner.h"
#include "core/utils/vertex_array.h"

namespace grape {

struct DynamicFragmentTraits {
  using OID_T = gs::dynamic::Value;
  using VID_T = vineyard::property_graph_types::VID_TYPE;
  using VDATA_T = gs::dynamic::Value;
  using EDATA_T = gs::dynamic::Value;
  using VERTEX_MAP_T = GlobalVertexMap<OID_T, VID_T>;
  using inner_vertices_t = DynamicVertexRange<VID_T>;
  using outer_vertices_t = DynamicVertexRange<VID_T>;
  using vertices_t = DynamicDualVertexRange<VID_T>;
  using sub_vertices_t = VertexVector<VID_T>;

  using fragment_adj_list_t =
      FilterAdjList<VID_T, EDATA_T,
                    std::function<bool(const Nbr<VID_T, EDATA_T>&)>>;
  using fragment_const_adj_list_t =
      FilterConstAdjList<VID_T, EDATA_T,
                         std::function<bool(const Nbr<VID_T, EDATA_T>&)>>;

  using csr_t = DeMutableCSR<VID_T, Nbr<VID_T, EDATA_T>>;
  using csr_builder_t = DeMutableCSRBuilder<VID_T, Nbr<VID_T, EDATA_T>>;
  using vertex_map_t = VERTEX_MAP_T;
  using mirror_vertices_t = std::vector<Vertex<VID_T>>;
};

class DynamicFragmentPoc
    : public CSREdgecutFragmentBase<
          gs::dynamic::Value, vineyard::property_graph_types::VID_TYPE,
          gs::dynamic::Value, gs::dynamic::Value, DynamicFragmentTraits> {
 public:
  using traits_t = DynamicFragmentTraits;
  using OID_T = gs::dynamic::Value;
  using VID_T = vineyard::property_graph_types::VID_TYPE;
  using VDATA_T = gs::dynamic::Value;
  using EDATA_T = gs::dynamic::Value;
  using oid_t = gs::dynamic::Value;
  using vid_t = vineyard::property_graph_types::VID_TYPE;
  using vdata_t = gs::dynamic::Value;
  using edata_t = gs::dynamic::Value;
  using base_t =
      CSREdgecutFragmentBase<oid_t, vid_t, vdata_t, edata_t, traits_t>;
  using internal_vertex_t = internal::Vertex<vid_t, vdata_t>;
  using edge_t = Edge<vid_t, edata_t>;
  using nbr_t = Nbr<vid_t, edata_t>;
  using vertex_t = Vertex<vid_t>;

  static constexpr LoadStrategy load_strategy = LoadStrategy::kOnlyOut;

  using vertex_map_t = typename traits_t::vertex_map_t;
  using partitioner_t = typename vertex_map_t::partitioner_t;
  using mutation_t = Mutation<vid_t, vdata_t, edata_t>;

  using IsEdgeCut = std::true_type;
  using IsVertexCut = std::false_type;

  using inner_vertices_t = typename traits_t::inner_vertices_t;
  using outer_vertices_t = typename traits_t::outer_vertices_t;
  using vertices_t = typename traits_t::vertices_t;
  using fragment_adj_list_t = typename traits_t::fragment_adj_list_t;
  using fragment_const_adj_list_t =
      typename traits_t::fragment_const_adj_list_t;

  template <typename T>
  using inner_vertex_array_t = VertexArray<inner_vertices_t, T>;

  template <typename T>
  using outer_vertex_array_t = VertexArray<outer_vertices_t, T>;

  template <typename T>
  using vertex_array_t = VertexArray<vertices_t, T>;

  using vertex_range_t = inner_vertices_t;

  DynamicFragmentPoc()
      : FragmentBase<OID_T, VID_T, VDATA_T, EDATA_T, traits_t>(NULL) {}
  explicit DynamicFragmentPoc(std::shared_ptr<vertex_map_t> vm_ptr)
      : FragmentBase<OID_T, VID_T, VDATA_T, EDATA_T, traits_t>(vm_ptr) {}
  virtual ~DynamicFragmentPoc() = default;

  using base_t::buildCSR;
  using base_t::init;
  using base_t::IsInnerVertexGid;
  void Init(fid_t fid, bool directed, std::vector<internal_vertex_t>& vertices,
            std::vector<edge_t>& edges,
            LoadStrategy load_strategy = LoadStrategy::kOnlyOut) override {
    init(fid, directed);

    ovnum_ = 0;
    load_strategy_ = load_strategy;
    static constexpr VID_T invalid_vid = std::numeric_limits<VID_T>::max();
    if (load_strategy_ == LoadStrategy::kOnlyIn) {
      for (auto& e : edges) {
        if (IsInnerVertexGid(e.dst)) {
          if (!IsInnerVertexGid(e.src)) {
            parseOrAddOuterVertexGid(e.src);
          }
        } else {
          e.src = invalid_vid;
        }
      }
    } else if (load_strategy_ == LoadStrategy::kOnlyOut) {
      for (auto& e : edges) {
        if (IsInnerVertexGid(e.src)) {
          if (!IsInnerVertexGid(e.dst)) {
            parseOrAddOuterVertexGid(e.dst);
          }
        } else {
          e.src = invalid_vid;
        }
      }
    } else if (load_strategy_ == LoadStrategy::kBothOutIn) {
      for (auto& e : edges) {
        if (IsInnerVertexGid(e.src)) {
          if (!IsInnerVertexGid(e.dst)) {
            parseOrAddOuterVertexGid(e.dst);
          }
        } else {
          if (IsInnerVertexGid(e.dst)) {
            parseOrAddOuterVertexGid(e.src);
          } else {
            e.src = invalid_vid;
          }
        }
      }
    } else {
      LOG(FATAL) << "Invalid load strategy";
    }

    iv_alive_.clear();
    iv_alive_.resize(ivnum_, true);
    ov_alive_.clear();
    ov_alive_.resize(ovnum_, true);
    alive_ivnum_ = ivnum_;
    alive_ovnum_ = ovnum_;
    selfloops_num_ = 0;
    selfloops_vertices_.clear();

    this->inner_vertices_.SetRange(0, ivnum_, alive_ivnum_, &iv_alive_);
    this->outer_vertices_.SetRange(id_parser_.max_local_id() - ovnum_,
                                   id_parser_.max_local_id(), alive_ovnum_,
                                   &ov_alive_, true);
    this->vertices_.SetRange(0, ivnum_, id_parser_.max_local_id() - ovnum_,
                             id_parser_.max_local_id(), &iv_alive_, &ov_alive_);
    initOuterVerticesOfFragment();

    buildCSR(edges, load_strategy_);

    ivdata_.clear();
    ivdata_.resize(ivnum_);
    ovdata_.clear();
    ovdata_.resize(ovnum_);
    if (sizeof(internal_vertex_t) > sizeof(VID_T)) {
      for (auto& v : vertices) {
        VID_T gid = v.vid;
        if (id_parser_.get_fragment_id(gid) == fid_) {
          ivdata_[id_parser_.get_local_id(gid)] = v.vdata;
        } else {
          auto iter = ovg2i_.find(gid);
          if (iter != ovg2i_.end()) {
            ovdata_[outerVertexLidToIndex(iter->second)] = v.vdata;
          }
        }
      }
    }
  }

  void Init(fid_t fid, bool directed) {
    std::vector<internal_vertex_t> empty_vertices;
    std::vector<edge_t> empty_edges;
    directed ? Init(fid, directed, empty_vertices, empty_edges,
                    grape::LoadStrategy::kBothOutIn)
             : Init(fid, directed, empty_vertices, empty_edges,
                    grape::LoadStrategy::kOnlyOut);
  }

  using base_t::Gid2Lid;
  using base_t::ie_;
  using base_t::oe_;
  using base_t::vm_ptr_;
  void Mutate(Mutation<vid_t, vdata_t, edata_t>& mutation) {
    vertex_t v;
    if (mutation.vertices_to_remove.empty() &&
        static_cast<double>(mutation.vertices_to_remove.size()) /
            static_cast<double>(this->GetVerticesNum()) <
        0.1) {
      std::set<vertex_t> sparse_set;
      for (auto gid : mutation.vertices_to_remove) {
        if (Gid2Vertex(gid, v)) {
          if (load_strategy_ == LoadStrategy::kBothOutIn) {
            ie_.remove_vertex(v.GetValue());
          }
          oe_.remove_vertex(v.GetValue());
          sparse_set.insert(v);

          // remove vertex
          iv_alive_[v.GetValue()] = false;
          --alive_ivnum_;
        }
      }
      if (!sparse_set.empty()) {
        auto func = [&sparse_set](vid_t i, const nbr_t& e) {
          return sparse_set.find(e.neighbor) != sparse_set.end();
        };
        if (load_strategy_ == LoadStrategy::kBothOutIn) {
          ie_.remove_if(func);
        }
        oe_.remove_if(func);
      }
    } else if (!mutation.vertices_to_remove.empty()) {
      vertex_array_t<bool> dense_bitset;
      dense_bitset.Init(Vertices(), false);
      for (auto gid : mutation.vertices_to_remove) {
        if (Gid2Vertex(gid, v)) {
          if (load_strategy_ == LoadStrategy::kBothOutIn) {
            ie_.remove_vertex(v.GetValue());
          }
          oe_.remove_vertex(v.GetValue());
          dense_bitset[v] = true;

          // remove vertex
          iv_alive_[v.GetValue()] = false;
          --alive_ivnum_;

          if (selfloops_vertices_.find(v.GetValue()) !=
              selfloops_vertices_.end()) {
            selfloops_vertices_.erase(v.GetValue());
            --selfloops_num_;
          }
        }
      }
      auto func = [&dense_bitset](vid_t i, const nbr_t& e) {
        return dense_bitset[e.neighbor];
      };
      if (load_strategy_ == LoadStrategy::kBothOutIn) {
        ie_.remove_if(func);
      }
      oe_.remove_if(func);
    }
    {
      static constexpr vid_t sentinel = std::numeric_limits<vid_t>::max();
      for (auto& e : mutation.edges_to_remove) {
        if (!(Gid2Lid(e.first, e.first) && Gid2Lid(e.second, e.second))) {
          e.first = sentinel;
        }
      }
      if (load_strategy_ == LoadStrategy::kBothOutIn) {
        ie_.remove_reversed_edges(mutation.edges_to_remove);
      }
      oe_.remove_edges(mutation.edges_to_remove);
    }
    {
      static constexpr vid_t sentinel = std::numeric_limits<vid_t>::max();
      for (auto& e : mutation.edges_to_update) {
        if (!(Gid2Lid(e.src, e.src) && Gid2Lid(e.dst, e.dst))) {
          e.src = sentinel;
        }
      }
      ie_.update_reversed_edges(mutation.edges_to_update);
      oe_.update_edges(mutation.edges_to_update);
    }
    {
      // vid_t ivnum = this->GetInnerVerticesNum();
      // vid_t ovnum = this->GetOuterVerticesNum();
      vid_t ivnum = this->inner_vertices_.end_value() -
                    this->inner_vertices_.begin_value();
      vid_t ovnum = this->outer_vertices_.end_value() -
                    this->outer_vertices_.begin_value();
      auto& edges_to_add = mutation.edges_to_add;
      static constexpr VID_T invalid_vid = std::numeric_limits<VID_T>::max();
      if (load_strategy_ == LoadStrategy::kOnlyIn) {
        for (auto& e : edges_to_add) {
          if (IsInnerVertexGid(e.dst)) {
            e.dst = id_parser_.get_local_id(e.dst);
            if (!IsInnerVertexGid(e.src)) {
              e.src = parseOrAddOuterVertexGid(e.src);
            } else {
              e.src = id_parser_.get_local_id(e.src);
            }
          } else {
            e.src = invalid_vid;
          }
        }
      } else if (load_strategy_ == LoadStrategy::kOnlyOut) {
        for (auto& e : edges_to_add) {
          if (IsInnerVertexGid(e.src)) {
            e.src = id_parser_.get_local_id(e.src);
            if (!IsInnerVertexGid(e.dst)) {
              e.dst = parseOrAddOuterVertexGid(e.dst);
            } else {
              e.dst = id_parser_.get_local_id(e.dst);
            }
          } else {
            e.src = invalid_vid;
          }
        }
      } else if (load_strategy_ == LoadStrategy::kBothOutIn) {
        for (auto& e : edges_to_add) {
          if (IsInnerVertexGid(e.src)) {
            e.src = id_parser_.get_local_id(e.src);
            if (IsInnerVertexGid(e.dst)) {
              e.dst = id_parser_.get_local_id(e.dst);
            } else {
              e.dst = parseOrAddOuterVertexGid(e.dst);
            }
          } else {
            if (IsInnerVertexGid(e.dst)) {
              e.src = parseOrAddOuterVertexGid(e.src);
              e.dst = id_parser_.get_local_id(e.dst);
            } else {
              e.src = invalid_vid;
            }
          }
        }
      } else {
        LOG(FATAL) << "Invalid load strategy";
      }
      vid_t new_ivnum = vm_ptr_->GetInnerVertexSize(fid_);
      vid_t new_ovnum = ovgid_.size();
      // add edges first, then update ivnum_ and ovnum;
      // reserve edges
      oe_.add_vertices(new_ivnum - ivnum, new_ovnum - ovnum);
      if (load_strategy_ == LoadStrategy::kBothOutIn) {
        ie_.add_vertices(new_ivnum - ivnum, new_ovnum - ovnum);
        oe_.reserve_forward_edges(edges_to_add);
        ie_.reserve_reversed_edges(edges_to_add);
      } else {
        oe_.reserve_edges(edges_to_add);
      }
      double rate = 0;
      if (directed_) {
        rate = static_cast<double>(edges_to_add.size()) / static_cast<double>(oe_.edge_num());
      } else {
        rate = 2.0 * static_cast<double>(edges_to_add.size()) / static_cast<double>(oe_.edge_num());
      }
      if (rate < oe_.dense_threshold) {
        addEdgesSparse(edges_to_add);
      } else {
        addEdgesDense(edges_to_add);
      }
      // add or update edge
      //  for (auto& e : edges_to_add) {
      //  addOrUpdateEdge(e);
      // }

      this->ivnum_ = new_ivnum;
      if (ovnum_ != new_ovnum) {
        ovnum_ = new_ovnum;
        initOuterVerticesOfFragment();
      }
    }
    ivdata_.resize(this->ivnum_);
    ovdata_.resize(this->ovnum_);
    iv_alive_.resize(this->ivnum_, true);
    ov_alive_.resize(this->ovnum_, true);
    for (auto& v : mutation.vertices_to_add) {
      vid_t lid;
      if (IsInnerVertexGid(v.vid)) {
        this->InnerVertexGid2Lid(v.vid, lid);
        ivdata_[lid] = std::move(v.vdata);
        iv_alive_[lid] = true;
        ++alive_ivnum_;
      } else {
        if (this->OuterVertexGid2Lid(v.vid, lid)) {
          ovdata_[outerVertexLidToIndex(lid)] = std::move(v.vdata);
          ov_alive_[outerVertexLidToIndex(lid)] = true;
          ++alive_ovnum_;
        }
      }
    }
    for (auto& v : mutation.vertices_to_update) {
      vid_t lid;
      if (IsInnerVertexGid(v.vid)) {
        this->InnerVertexGid2Lid(v.vid, lid);
        ivdata_[lid] = std::move(v.vdata);
      } else {
        if (this->OuterVertexGid2Lid(v.vid, lid)) {
          ovdata_[outerVertexLidToIndex(lid)] = std::move(v.vdata);
        }
      }
    }

    // The ranges must be set after alive_ivnum_ and alive_ovnum_
    this->inner_vertices_.SetRange(0, this->ivnum_, alive_ivnum_, &iv_alive_);
    this->outer_vertices_.SetRange(id_parser_.max_local_id() - this->ovnum_,
                                   id_parser_.max_local_id(), alive_ovnum_,
                                   &ov_alive_, true);
    this->vertices_.SetRange(0, this->ivnum_,
                             id_parser_.max_local_id() - this->ovnum_,
                             id_parser_.max_local_id(), &iv_alive_, &ov_alive_);
  }

  template <typename IOADAPTOR_T>
  void Serialize(const std::string& prefix) {
    char fbuf[1024];
    snprintf(fbuf, sizeof(fbuf), kSerializationFilenameFormat, prefix.c_str(),
             fid_);

    auto io_adaptor =
        std::unique_ptr<IOADAPTOR_T>(new IOADAPTOR_T(std::string(fbuf)));

    io_adaptor->Open("wb");

    base_t::serialize(io_adaptor);

    InArchive ia;
    ia << ovnum_;
    CHECK(io_adaptor->WriteArchive(ia));
    ia.Clear();

    if (ovnum_ > 0) {
      CHECK(io_adaptor->Write(&ovgid_[0], ovnum_ * sizeof(VID_T)));
    }

    ia << ivdata_ << ovdata_;
    CHECK(io_adaptor->WriteArchive(ia));
    ia.Clear();

    io_adaptor->Close();
  }

  template <typename IOADAPTOR_T>
  void Deserialize(const std::string& prefix, const fid_t fid) {
    char fbuf[1024];
    snprintf(fbuf, sizeof(fbuf), kSerializationFilenameFormat, prefix.c_str(),
             fid);
    auto io_adaptor =
        std::unique_ptr<IOADAPTOR_T>(new IOADAPTOR_T(std::string(fbuf)));
    io_adaptor->Open();

    base_t::deserialize(io_adaptor);

    OutArchive oa;
    CHECK(io_adaptor->ReadArchive(oa));
    oa >> ovnum_;

    oa.Clear();

    ovgid_.clear();
    ovgid_.resize(ovnum_);
    if (ovnum_ > 0) {
      CHECK(io_adaptor->Read(&ovgid_[0], ovnum_ * sizeof(VID_T)));
    }

    initOuterVerticesOfFragment();

    {
      ovg2i_.clear();
      VID_T ovlid = id_parser_.max_local_id();
      for (auto gid : ovgid_) {
        ovg2i_.emplace(gid, --ovlid);
      }
    }

    CHECK(io_adaptor->ReadArchive(oa));
    oa >> ivdata_ >> ovdata_;

    io_adaptor->Close();
  }

  void PrepareToRunApp(const CommSpec& comm_spec, PrepareConf conf) override {
    base_t::PrepareToRunApp(comm_spec, conf);
    if (conf.need_split_edges_by_fragment) {
      LOG(FATAL) << "MutableEdgecutFragment cannot split edges by fragment";
    } else if (conf.need_split_edges) {
      // splitEdges();
    }
  }

  using base_t::InnerVertices;
  using base_t::IsInnerVertex;
  using base_t::OuterVertices;

  inline const VDATA_T& GetData(const vertex_t& v) const override {
    return IsInnerVertex(v) ? ivdata_[v.GetValue()]
                            : ovdata_[outerVertexLidToIndex(v.GetValue())];
  }

  inline VDATA_T& GetRefData(const vertex_t& v) {
    return IsInnerVertex(v) ? ivdata_[v.GetValue()]
                            : ovdata_[outerVertexLidToIndex(v.GetValue())];
  }

  inline void SetData(const vertex_t& v, const VDATA_T& val) override {
    if (IsInnerVertex(v)) {
      ivdata_[v.GetValue()] = val;
    } else {
      ovdata_[outerVertexLidToIndex(v.GetValue())] = val;
    }
  }

  inline void UpdateData(const vertex_t& v, VDATA_T&& val) {
    if (IsInnerVertex(v)) {
      ivdata_[v.GetValue()].Update(val);
    } else {
      ovdata_[outerVertexLidToIndex(v.GetValue())].Update(val);
    }
  }

  bool OuterVertexGid2Lid(VID_T gid, VID_T& lid) const override {
    auto iter = ovg2i_.find(gid);
    if (iter != ovg2i_.end()) {
      lid = iter->second;
      return true;
    } else {
      return false;
    }
  }

  VID_T GetOuterVertexGid(vertex_t v) const override {
    return ovgid_[outerVertexLidToIndex(v.GetValue())];
  }

  inline bool Gid2Vertex(const VID_T& gid, vertex_t& v) const override {
    fid_t fid = id_parser_.get_fragment_id(gid);
    if (fid == fid_) {
      v.SetValue(id_parser_.get_local_id(gid));
      return true;
    } else {
      auto iter = ovg2i_.find(gid);
      if (iter != ovg2i_.end()) {
        v.SetValue(iter->second);
        return true;
      } else {
        return false;
      }
    }
  }

  inline VID_T Vertex2Gid(const vertex_t& v) const override {
    if (IsInnerVertex(v)) {
      return id_parser_.generate_global_id(fid_, v.GetValue());
    } else {
      return ovgid_[outerVertexLidToIndex(v.GetValue())];
    }
  }

  void ModifyVertices(gs::dynamic::Value& vertices_to_modify,
                      const gs::dynamic::Value& common_attrs,
                      const gs::rpc::ModifyType& modify_type) {
    {
      LOG(INFO) << "begin modify nodes.";
      double start = GetCurrentTime();
      mutation_t mutation;
      auto& partitioner = vm_ptr_->GetPartitioner();
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
        if (modify_type == gs::rpc::NX_ADD_NODES) {
          bool added = vm_ptr_->AddVertex(oid, gid);
          if (v_fid == fid()) {
            if (!added) {
              vertex_t vertex;
              Gid2Vertex(gid, vertex);
              UpdateData(vertex, std::move(v_data));
            } else {
              mutation.vertices_to_add.emplace_back(gid, std::move(v_data));
            }
          }
        } else {
          // UPDATE or DELETE, if not exist the node, continue.
          if (!vm_ptr_->GetGid(v_fid, oid, gid)) {
            continue;
          }
        }
        if (modify_type == gs::rpc::NX_UPDATE_NODES && v_fid == fid()) {
          mutation.vertices_to_update.emplace_back(gid, std::move(v_data));
        }
        if (modify_type == gs::rpc::NX_DEL_NODES && v_fid == fid()) {
          mutation.vertices_to_remove.emplace_back(gid);
        }
      }
      LOG(INFO) << "Poc processing vertices time: " << GetCurrentTime() - start;
      Mutate(mutation);
      LOG(INFO) << "Poc modify vertices time: " << GetCurrentTime() - start;
    }
  }


  void ModifyEdges(gs::dynamic::Value& edges_to_modify,
                   const gs::dynamic::Value& common_attrs,
                   const gs::rpc::ModifyType modify_type,
                   const std::string weight) {
    {
      LOG(INFO) << "begin adding edge.";
      double start = GetCurrentTime();
      edata_t e_data;
      oid_t src, dst;
      vid_t src_gid, dst_gid;
      fid_t src_fid, dst_fid;
      auto& partitioner = vm_ptr_->GetPartitioner();
      mutation_t mutation;
      mutation.edges_to_add.reserve(edges_to_modify.Size());
      mutation.vertices_to_add.reserve(edges_to_modify.Size() * 2);
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
        if (modify_type == gs::rpc::NX_ADD_EDGES) {
          bool src_added = vm_ptr_->AddVertex(src, src_gid);
          bool dst_added = vm_ptr_->AddVertex(dst, dst_gid);
          if (src_fid == fid() && src_added) {
            vdata_t empty_data(rapidjson::kObjectType);
            mutation.vertices_to_add.emplace_back(src_gid,
                                                  std::move(empty_data));
          }
          if (dst_fid == fid() && dst_added) {
            vdata_t empty_data(rapidjson::kObjectType);
            mutation.vertices_to_add.emplace_back(dst_gid,
                                                  std::move(empty_data));
          }
        } else {
          if (!vm_ptr_->GetGid(src_fid, src, src_gid) ||
              !vm_ptr_->GetGid(dst_fid, dst, dst_gid)) {
            continue;
          }
        }
        if (modify_type == gs::rpc::NX_ADD_EDGES) {
          if (src_fid == fid() || dst_fid == fid()) {
            mutation.edges_to_add.emplace_back(src_gid, dst_gid, std::move(e_data));
            // if (!directed()) {
            //   mutation.edges_to_add.emplace_back(dst_gid, src_gid, e_data);
            // }
          }
        } else if (modify_type == gs::rpc::NX_DEL_EDGES) {
          if (src_fid == fid() || dst_fid == fid()) {
            mutation.edges_to_remove.emplace_back(src_gid, dst_gid);
            if (!directed()) {
              mutation.edges_to_remove.emplace_back(dst_gid, src_gid);
            }

            if (src_gid == dst_gid) {
              // delete selflooops
              vid_t lid;
              CHECK(InnerVertexGid2Lid(src_gid, lid));
              if (selfloops_vertices_.find(lid) != selfloops_vertices_.end()) {
                selfloops_vertices_.erase(lid);
                --selfloops_num_;
              }
            }
          }
        } else if (modify_type == gs::rpc::NX_UPDATE_EDGES) {
          if (src_fid == fid() || dst_fid == fid()) {
            mutation.edges_to_update.emplace_back(src_gid, dst_gid, e_data);
            if (!directed()) {
              mutation.edges_to_update.emplace_back(dst_gid, src_gid, e_data);
            }
          }
        }
      }
      LOG(INFO) << "Poc processing edges time: " << GetCurrentTime() - start;
      start = GetCurrentTime();
      Mutate(mutation);
      LOG(INFO) << "Poc insert edges time: " << GetCurrentTime() - start;
    }
  }

  // Methods that belongs to NetworkX
  void ClearGraph(std::shared_ptr<vertex_map_t> vm_ptr) {
    vm_ptr_.reset();
    vm_ptr_ = vm_ptr;
    Init(fid_, directed_);
  }

  void ClearEdges() {
    selfloops_vertices_.clear();
    selfloops_num_ = 0;

    if (load_strategy_ == LoadStrategy::kBothOutIn) {
      ie_.clear_edges();
    }
    oe_.clear_edges();

    // clear outer vertices map
    ovgid_.clear();
    ovg2i_.clear();
    ov_alive_.clear();
    this->ovnum_ = 0;
    this->alive_ovnum_ = 0;
  }

  void CopyFrom(std::shared_ptr<DynamicFragmentPoc> source,
                const std::string& copy_type = "identical") {
    directed_ = source->directed_;
    load_strategy_ = source->load_strategy_;
    copyVertices(source);
  }

  // generate directed graph from orignal undirected graph.
  void ToDirectedFrom(std::shared_ptr<DynamicFragmentPoc> origin) { return; }

  // generate undirected graph from original directed graph.
  void ToUndirectedFrom(std::shared_ptr<DynamicFragmentPoc> origin) { return; }

  // induce a subgraph that contains the induced_vertices and the edges between
  // those vertices or a edge subgraph that contains the induced_edges and the
  // nodes incident to induced_edges.
  void InduceSubgraph(
      std::shared_ptr<DynamicFragmentPoc> origin,
      const std::vector<oid_t>& induced_vertices,
      const std::vector<std::pair<oid_t, oid_t>>& induced_edges) {
    return;
  }

  inline bool Oid2Gid(const oid_t& oid, vid_t& gid) const {
    return vm_ptr_->GetGid(oid, gid);
  }

  inline virtual size_t selfloops_num() const {
    return selfloops_vertices_.size();
  }

  inline virtual bool HasNode(const oid_t& node) const {
    vid_t gid;
    return this->vm_ptr_->GetGid(fid_, node, gid) &&
           iv_alive_[id_parser_.get_local_id(gid)];
  }

  inline virtual bool HasEdge(const oid_t& u, const oid_t& v) const {
    vid_t uid, vid;
    if (vm_ptr_->GetGid(u, uid) && vm_ptr_->GetGid(v, vid)) {
      vid_t ulid, vlid;
      if (IsInnerVertexGid(uid) && InnerVertexGid2Lid(uid, ulid) &&
          Gid2Lid(vid, vlid) && iv_alive_[ulid]) {
        auto begin = oe_.get_begin(ulid);
        auto end = oe_.get_end(ulid);
        // TODO: binary search
        auto iter = std::find_if(begin, end, [&vlid](const auto& val) {
          return val.neighbor.GetValue() == vlid;
        });
        if (iter != end) {
          return true;
        }
      }
    }
    return false;
  }

  inline virtual bool GetEdgeData(const oid_t& u_oid, const oid_t& v_oid,
                                  edata_t& data) const {
    vid_t uid, vid;
    if (vm_ptr_->GetGid(u_oid, uid) && vm_ptr_->GetGid(v_oid, vid)) {
      vid_t ulid, vlid;
      if (IsInnerVertexGid(uid) && InnerVertexGid2Lid(uid, ulid) &&
          Gid2Lid(vid, vlid) && iv_alive_[ulid]) {
        auto begin = oe_.get_begin(ulid);
        auto end = oe_.get_end(ulid);
        // TODO: binary search
        auto iter = std::find_if(begin, end, [&vlid](const auto& val) {
          return val.neighbor.GetValue() == vlid;
        });
        if (iter != end) {
          LOG(INFO) << "Get Edge Data: " << iter->data;
          // data = iter->data;
          return true;
        }
      }
    }
    return false;
  }

  inline virtual bool IsAliveInnerVertex(const vertex_t& v) const {
    return iv_alive_[v.GetValue()];
  }

  auto CollectPropertyKeysOnVertices()
      -> gs::bl::result<std::map<std::string, gs::dynamic::Type>> {
    std::map<std::string, gs::dynamic::Type> prop_keys;

    for (const auto& v : InnerVertices()) {
      auto& data = ivdata_[v.GetValue()];

      for (auto member = data.MemberBegin(); member != data.MemberEnd();
           ++member) {
        std::string s_k = member->name.GetString();

        if (prop_keys.find(s_k) == prop_keys.end()) {
          prop_keys[s_k] = gs::dynamic::GetType(member->value);
        } else {
          auto seen_type = prop_keys[s_k];
          auto curr_type = gs::dynamic::GetType(member->value);

          if (seen_type != curr_type) {
            std::stringstream ss;
            ss << "OID: " << GetId(v) << " has key " << s_k << " with type "
               << curr_type << " but previous type is: " << seen_type;
            RETURN_GS_ERROR(vineyard::ErrorCode::kDataTypeError, ss.str());
          }
        }
      }
    }

    return prop_keys;
  }

  auto CollectPropertyKeysOnEdges()
      -> gs::bl::result<std::map<std::string, gs::dynamic::Type>> {
    std::map<std::string, gs::dynamic::Type> prop_keys;

    auto extract_keys = [this, &prop_keys](
                            const vertex_t& u,
                            const adj_list_t& es) -> gs::bl::result<void> {
      for (auto& e : es) {
        auto& data = e.data;

        for (auto member = data.MemberBegin(); member != data.MemberEnd();
             ++member) {
          std::string s_k = member->name.GetString();

          if (prop_keys.find(s_k) == prop_keys.end()) {
            prop_keys[s_k] = gs::dynamic::GetType(member->value);
          } else {
            auto seen_type = prop_keys[s_k];
            auto curr_type = gs::dynamic::GetType(member->value);

            if (seen_type != curr_type) {
              std::stringstream ss;
              ss << "Edge (OID): " << GetId(u) << " " << GetId(e.neighbor)
                 << " has key " << s_k << " with type " << curr_type
                 << " but previous type is: " << seen_type;
              RETURN_GS_ERROR(vineyard::ErrorCode::kDataTypeError, ss.str());
            }
          }
        }
      }
      return {};
    };

    for (const auto& v : InnerVertices()) {
      if (load_strategy_ == grape::LoadStrategy::kOnlyIn ||
          load_strategy_ == grape::LoadStrategy::kBothOutIn) {
        auto es = this->GetIncomingAdjList(v);
        if (es.NotEmpty()) {
          BOOST_LEAF_CHECK(extract_keys(v, es));
        }
      }

      if (load_strategy_ == grape::LoadStrategy::kOnlyOut ||
          load_strategy_ == grape::LoadStrategy::kBothOutIn) {
        auto es = this->GetOutgoingAdjList(v);
        if (es.NotEmpty()) {
          BOOST_LEAF_CHECK(extract_keys(v, es));
        }
      }
    }

    return prop_keys;
  }

  virtual gs::bl::result<gs::dynamic::Type> GetOidType(
      const grape::CommSpec& comm_spec) const {
    auto oid_type = gs::dynamic::Type::kNullType;
    if (this->alive_ivnum_ > 0) {
      // Get first alive vertex oid type.
      for (vid_t lid = 0; lid < ivnum_; ++lid) {
        if (iv_alive_[lid]) {
          oid_t oid;
          vm_ptr_->GetOid(fid_, lid, oid);
          oid_type = gs::dynamic::GetType(oid);
        }
      }
    }
    grape::Communicator comm;
    gs::dynamic::Type max_type;
    comm.InitCommunicator(comm_spec.comm());
    comm.Max(oid_type, max_type);

    if (max_type != gs::dynamic::Type::kInt64Type &&
        max_type != gs::dynamic::Type::kDoubleType &&
        max_type != gs::dynamic::Type::kStringType &&
        max_type != gs::dynamic::Type::kNullType) {
      LOG(FATAL) << "Unsupported oid type.";
    }
    return max_type;
  }

 public:
  using base_t::GetIncomingAdjList;
  using base_t::GetOutgoingAdjList;

  fragment_adj_list_t GetOutgoingAdjList(const vertex_t& v,
                                         fid_t dst_fid) override {
    return fragment_adj_list_t(
        get_oe_begin(v), get_oe_end(v), [this, dst_fid](const nbr_t& nbr) {
          return this->GetFragId(nbr.get_neighbor()) == dst_fid;
        });
  }

  fragment_const_adj_list_t GetOutgoingAdjList(const vertex_t& v,
                                               fid_t dst_fid) const override {
    return fragment_const_adj_list_t(
        get_oe_begin(v), get_oe_end(v), [this, dst_fid](const nbr_t& nbr) {
          return this->GetFragId(nbr.get_neighbor()) == dst_fid;
        });
  }

  fragment_adj_list_t GetIncomingAdjList(const vertex_t& v,
                                         fid_t dst_fid) override {
    return fragment_adj_list_t(
        get_ie_begin(v), get_ie_end(v), [this, dst_fid](const nbr_t& nbr) {
          return this->GetFragId(nbr.get_neighbor()) == dst_fid;
        });
  }

  fragment_const_adj_list_t GetIncomingAdjList(const vertex_t& v,
                                               fid_t dst_fid) const override {
    return fragment_const_adj_list_t(
        get_ie_begin(v), get_ie_end(v), [this, dst_fid](const nbr_t& nbr) {
          return this->GetFragId(nbr.get_neighbor()) == dst_fid;
        });
  }

 public:
  using base_t::get_ie_begin;
  using base_t::get_ie_end;
  using base_t::get_oe_begin;
  using base_t::get_oe_end;

 public:
  using adj_list_t = typename base_t::adj_list_t;
  using const_adj_list_t = typename base_t::const_adj_list_t;
  inline adj_list_t GetIncomingInnerVertexAdjList(const vertex_t& v) override {
    assert(IsInnerVertex(v));
    return adj_list_t(get_ie_begin(v), iespliter_[v]);
  }

  inline const_adj_list_t GetIncomingInnerVertexAdjList(
      const vertex_t& v) const override {
    assert(IsInnerVertex(v));
    return const_adj_list_t(get_ie_begin(v), iespliter_[v]);
  }

  inline adj_list_t GetIncomingOuterVertexAdjList(const vertex_t& v) override {
    assert(IsInnerVertex(v));
    return adj_list_t(iespliter_[v], get_ie_end(v));
  }

  inline const_adj_list_t GetIncomingOuterVertexAdjList(
      const vertex_t& v) const override {
    assert(IsInnerVertex(v));
    return const_adj_list_t(iespliter_[v], get_ie_end(v));
  }

  inline adj_list_t GetOutgoingInnerVertexAdjList(const vertex_t& v) override {
    assert(IsInnerVertex(v));
    return adj_list_t(get_oe_begin(v), oespliter_[v]);
  }

  inline const_adj_list_t GetOutgoingInnerVertexAdjList(
      const vertex_t& v) const override {
    assert(IsInnerVertex(v));
    return const_adj_list_t(get_oe_begin(v), oespliter_[v]);
  }

  inline adj_list_t GetOutgoingOuterVertexAdjList(const vertex_t& v) override {
    assert(IsInnerVertex(v));
    return adj_list_t(oespliter_[v], get_oe_end(v));
  }

  inline const_adj_list_t GetOutgoingOuterVertexAdjList(
      const vertex_t& v) const override {
    assert(IsInnerVertex(v));
    return const_adj_list_t(oespliter_[v], get_oe_end(v));
  }

 private:
  inline VID_T outerVertexLidToIndex(VID_T lid) const {
    return id_parser_.max_local_id() - lid - 1;
  }

  inline VID_T outerVertexIndexToLid(VID_T index) const {
    return id_parser_.max_local_id() - index - 1;
  }

  void splitEdges() {
    auto inner_vertices = InnerVertices();
    iespliter_.Init(inner_vertices);
    oespliter_.Init(inner_vertices);
    int inner_neighbor_count = 0;
    for (auto& v : inner_vertices) {
      inner_neighbor_count = 0;
      auto ie = GetIncomingAdjList(v);
      for (auto& e : ie) {
        if (IsInnerVertex(e.neighbor)) {
          ++inner_neighbor_count;
        }
      }
      iespliter_[v] = get_ie_begin(v) + inner_neighbor_count;

      inner_neighbor_count = 0;
      auto oe = GetOutgoingAdjList(v);
      for (auto& e : oe) {
        if (IsInnerVertex(e.neighbor)) {
          ++inner_neighbor_count;
        }
      }
      oespliter_[v] = get_oe_begin(v) + inner_neighbor_count;
    }
  }

  VID_T parseOrAddOuterVertexGid(VID_T gid) {
    auto iter = ovg2i_.find(gid);
    if (iter != ovg2i_.end()) {
      return iter->second;
    } else {
      ++ovnum_;
      VID_T lid = id_parser_.max_local_id() - ovnum_;
      ovgid_.push_back(gid);
      ovg2i_.emplace(gid, lid);
      return lid;
    }
  }

  void initOuterVerticesOfFragment() {
    outer_vertices_of_frag_.resize(fnum_);
    for (auto& vec : outer_vertices_of_frag_) {
      vec.clear();
    }
    for (VID_T i = 0; i < ovnum_; ++i) {
      fid_t fid = id_parser_.get_fragment_id(ovgid_[i]);
      outer_vertices_of_frag_[fid].push_back(
          vertex_t(outerVertexIndexToLid(i)));
    }
  }

  bool addOrUpdateEdge(edge_t& e) {
    // TODO(weibin): revise the method.

    bool ret = true;
    if (load_strategy_ == LoadStrategy::kBothOutIn) {
      if (e.src < ivnum_) {
        // src is inner vertex;
        auto begin = oe_.get_begin(e.src);
        auto end = oe_.get_end(e.src);
        auto iter = std::find_if(begin, end, [&e](const auto& val) {
          return val.neighbor.GetValue() == e.dst;
        });
        if (iter != end) {
          iter->data.Update(e.edata);
          ret = false;
        } else {
          oe_.add_edge(e);
          if (e.src == e.dst) {
            selfloops_vertices_.insert(e.src);
          }
        }
      } else {
        oe_.add_edge(e);
        ivnum_++;
      }

      if (e.dst < ivnum_) {
        // dst is the inner vertex;
        auto begin = ie_.get_begin(e.dst);
        auto end = ie_.get_end(e.dst);
        auto iter = std::find_if(begin, end, [&e](const auto& val) {
          return val.neighbor.GetValue() == e.src;
        });
        if (iter != end) {
          iter->data.Update(e.edata);
          ret = false;
        } else {
          ie_.add_reversed_edge(e);
        }
      } else if (e.dst < vm_ptr_->GetInnerVertexSize(fid_)) {
        ie_.add_reversed_edge(e);
        ivnum_++;
      }
    } else {
      if (e.src < ivnum_) {
        auto begin = oe_.get_begin(e.src);
        auto end = oe_.get_end(e.src);
        auto iter = std::find_if(begin, end, [&e](const auto& val) {
          return val.neighbor.GetValue() == e.dst;
        });
        if (iter != end) {
          iter->data.Update(e.edata);
          ret = false;
        } else {
          oe_.add_edge(e);
          if (e.src == e.dst) {
            selfloops_vertices_.insert(e.src);
            return ret;
          }
        }
      } else {
        oe_.add_edge(e);
        ivnum_++;
      }

      if (e.dst < ivnum_) {
        auto begin = oe_.get_begin(e.dst);
        auto end = oe_.get_end(e.dst);
        auto iter = std::find_if(begin, end, [&e](const auto& val) {
          return val.neighbor.GetValue() == e.src;
        });
        if (iter != end) {
          iter->data.Update(e.edata);
          ret = false;
        } else {
          oe_.add_reversed_edge(e);
        }
      } else if (e.dst < vm_ptr_->GetInnerVertexSize(fid_)) {
        oe_.add_reversed_edge(e);
        ivnum_++;
      }
    }
    return ret;
  }

  void addEdgesDense(std::vector<edge_t>& edges) {
    LOG(INFO) << "addEdgesDense";
    if (directed_) {
      std::vector<int> oe_head_degree_to_add(oe_.head_vertex_num(), 0), ie_head_degree_to_add(ie_.head_vertex_num(), 0), dummy_tail_degree;
      for (auto& e : edges) {
        if (addOrUpdateEdge(e)) {
          if (e.src < ivnum_) {
            ++oe_head_degree_to_add[oe_.head_index(e.src)];
          }
          if (e.dst < ivnum_) {
            ++ie_head_degree_to_add[ie_.head_index(e.dst)];
          }
        }
      }
      oe_.dedup_or_sort_neighbors_dense(oe_head_degree_to_add, dummy_tail_degree);
      ie_.dedup_or_sort_neighbors_dense(ie_head_degree_to_add, dummy_tail_degree);
    } else {
      std::vector<int> oe_head_degree_to_add(oe_.head_vertex_num(), 0), dummy_tail_degree;
      for (auto& e : edges) {
        if (addOrUpdateEdge(e)) {
          if (e.src < ivnum_) {
            ++oe_head_degree_to_add[oe_.head_index(e.src)];
          }
          if (e.dst < ivnum_ && e.src != e.dst) {
            ++oe_head_degree_to_add[oe_.head_index(e.dst)];
          }
        }
      }
      oe_.dedup_or_sort_neighbors_dense(oe_head_degree_to_add, dummy_tail_degree);
    }
  }

  void addEdgesSparse(std::vector<edge_t>& edges) {
    LOG(INFO) << "addEdgesSparse";
    if (directed_) {
      std::map<vid_t, int> oe_head_degree_to_add, ie_head_degree_to_add, dummy_tail_degree;
      for (auto& e : edges) {
        if (addOrUpdateEdge(e)) {
          if (e.src < ivnum_) {
            ++oe_head_degree_to_add[oe_.head_index(e.src)];
          }
          if (e.dst < ivnum_) {
            ++ie_head_degree_to_add[ie_.head_index(e.dst)];
          }
        }
      }
      oe_.dedup_or_sort_neighbors_sparse(oe_head_degree_to_add, dummy_tail_degree);
      ie_.dedup_or_sort_neighbors_sparse(ie_head_degree_to_add, dummy_tail_degree);
    } else {
      std::map<vid_t, int> oe_head_degree_to_add, dummy_tail_degree;
      for (auto& e : edges) {
        if (addOrUpdateEdge(e)) {
          if (e.src < ivnum_) {
            LOG(INFO) << e.src << " has degree to add.";
            ++oe_head_degree_to_add[oe_.head_index(e.src)];
          }
          if (e.dst < ivnum_ && e.src != e.dst) {
            LOG(INFO) << e.dst << " has degree to add.";
            ++oe_head_degree_to_add[oe_.head_index(e.dst)];
          }
        }
      }
      oe_.dedup_or_sort_neighbors_sparse(oe_head_degree_to_add, dummy_tail_degree);
    }
  }

  void copyVertices(std::shared_ptr<DynamicFragmentPoc>& source) {
    this->ivnum_ = source->ivnum_;
    this->ovnum_ = source->ovnum_;
    this->alive_ivnum_ = source->alive_ivnum_;
    this->alive_ovnum_ = source->alive_ovnum_;
    this->fid_ = source->fid_;
    this->fnum_ = source->fnum_;
    this->selfloops_num_ = source->selfloops_num_;
    this->selfloops_vertices_ = source->selfloops_vertices_;

    ovg2i_ = source->ovg2i_;
    ovgid_.resize(ovnum_);
    memcpy(&ovgid_[0], &(source->ovgid_[0]), ovnum_ * sizeof(vid_t));

    ivdata_.clear();
    ivdata_.resize(ivnum_);
    for (size_t i = 0; i < ivnum_; ++i) {
      ivdata_[i] = source->ivdata_[i];
    }

    iv_alive_.resize(ivnum_);
    ov_alive_.resize(ovnum_);
    memcpy(&iv_alive_[0], &(source->iv_alive_[0]), ivnum_ * sizeof(bool));
    memcpy(&ov_alive_[0], &(source->ov_alive_[0]), ovnum_ * sizeof(bool));
  }

  using base_t::ivnum_;
  VID_T ovnum_;
  VID_T alive_ivnum_, alive_ovnum_;
  using base_t::directed_;
  using base_t::fid_;
  using base_t::fnum_;
  using base_t::id_parser_;
  LoadStrategy load_strategy_;

  ska::flat_hash_map<VID_T, VID_T> ovg2i_;
  std::vector<VID_T> ovgid_;
  Array<VDATA_T, Allocator<VDATA_T>> ivdata_;
  Array<VDATA_T, Allocator<VDATA_T>> ovdata_;
  Array<bool> iv_alive_;
  Array<bool> ov_alive_;

  VertexArray<inner_vertices_t, nbr_t*> iespliter_, oespliter_;

  using base_t::outer_vertices_of_frag_;

  VID_T selfloops_num_;
  std::set<vid_t> selfloops_vertices_;

  template <typename _VDATA_T, typename _EDATA_T>
  friend class DynamicProjectedFragmentPoc;
};

}  // namespace grape

#endif