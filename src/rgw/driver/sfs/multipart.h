// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t
// vim: ts=8 sw=2 smarttab ft=cpp
/*
 * Ceph - scalable distributed file system
 * SFS SAL implementation
 *
 * Copyright (C) 2022 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 */
#ifndef RGW_STORE_SFS_MULTIPART_H
#define RGW_STORE_SFS_MULTIPART_H

#include "rgw/driver/sfs/bucket.h"
#include "rgw/driver/sfs/multipart_types.h"
#include "rgw/driver/sfs/object.h"
#include "rgw/driver/sfs/sqlite/buckets/multipart_definitions.h"
#include "rgw/driver/sfs/sqlite/sqlite_multipart.h"
#include "rgw/driver/sfs/types.h"
#include "rgw/driver/sfs/uuid_path.h"
#include "rgw_sal.h"
#include "rgw_sal_store.h"

namespace rgw::sal::sfs {

class SFStore;

/* This class comes as a hack to circumvent the SAL layer's
 * requirement/expectation to for a meta object for a given multipart upload.
 * Unlike other backends, we do not rely on a meta object during our process.
 * Instead of returning an actual object, which entails meeting several
 * criteria, we choose instead to extend the SFSObject class and overriding the
 * bits that are relevant for the SAL layer's expected path.
 *
 * For reference, check 'rgw_op.cc', RGWCompleteMultipart::execute().
 *
 * Note that during MP uploads, before completing them, the object is
 * actually a promise of a future object. To make this promise
 * accessible to certain queries (e.g get_attr) initialize the
 * underlying objref from MP data rather than object / versioned
 * object data.
 */
struct SFSMultipartMetaObject : public rgw::sal::SFSObject {
  SFSMultipartMetaObject(SFSMultipartMetaObject&) = delete;
  SFSMultipartMetaObject(
      rgw::sal::SFStore* _st, const rgw_obj_key& _k, rgw::sal::Bucket* _b,
      BucketRef _bucket, const rgw::sal::Attrs& attrs
  )
      : rgw::sal::SFSObject(_st, _k, _b, _bucket, false) {
    // Note: objref points to a object that does not actually exists.
    objref.reset(sfs::Object::create_from_obj_key(_k));
    objref->update_attrs(attrs);
  }

  struct SFSMetaObjReadOp : public ReadOp {
   private:
    const sfs::Object& obj;

   public:
    SFSMetaObjReadOp() = delete;
    SFSMetaObjReadOp(const sfs::Object& _obj) : obj(_obj) {}
    virtual int prepare(optional_yield, const DoutPrefixProvider*) override {
      return 0;
    }
    virtual int
    read(int64_t, int64_t, bufferlist&, optional_yield, const DoutPrefixProvider*)
        override {
      return -ENOTSUP;
    }
    virtual int iterate(
        const DoutPrefixProvider*, int64_t, int64_t, RGWGetDataCB*,
        optional_yield
    ) override {
      return -ENOTSUP;
    }
    virtual int get_attr(
        const DoutPrefixProvider*, const char* name, bufferlist& dest,
        optional_yield
    ) override {
      return obj.get_attr(name, dest);
    }
    const std::string get_cls_name() { return "mp_meta_obj_read"; }
  };

  struct SFSMetaObjDeleteOp : public DeleteOp {
    SFSMetaObjDeleteOp() = default;
    virtual int
    delete_obj(const DoutPrefixProvider* /*dpp*/, optional_yield /*y*/)
        override {
      return 0;
    }
    const std::string get_cls_name() { return "mp_meta_obj_delete"; }
  };

  virtual std::unique_ptr<Object> clone() override { return nullptr; }
  SFSMultipartMetaObject& operator=(const SFSMultipartMetaObject&) = delete;

  virtual std::unique_ptr<DeleteOp> get_delete_op() override {
    return std::make_unique<SFSMetaObjDeleteOp>();
  }

  virtual int delete_object(
      const DoutPrefixProvider* /*dpp*/, optional_yield /*y*/,
      bool /*prevent_versioning*/
  ) override {
    return 0;
  }

  virtual std::unique_ptr<ReadOp> get_read_op() override {
    return std::make_unique<SFSMetaObjReadOp>(*objref);
  }
};

class SFSMultipartPartV2 : public StoreMultipartPart {
  const std::string upload_id;
  uint32_t part_num;
  uint64_t len;
  const std::string etag;
  ceph::real_time mtime;

 public:
  SFSMultipartPartV2(const sqlite::DBMultipartPart& part)
      : upload_id(part.upload_id),
        part_num(part.part_num),
        len(part.size),
        etag(part.etag.value()),
        mtime(part.mtime.value()) {}

  virtual ~SFSMultipartPartV2() = default;

  virtual uint32_t get_num() override { return part_num; }
  virtual uint64_t get_size() override { return len; }
  virtual const std::string& get_etag() override { return etag; }
  virtual ceph::real_time& get_mtime() override { return mtime; }
};

class SFSMultipartUploadV2 : public StoreMultipartUpload {
  rgw::sal::SFStore* store;
  BucketRef bucketref;
  const std::string upload_id;
  const std::string oid;
  ACLOwner owner;
  ceph::real_time mtime;
  rgw_placement_rule placement;

  const std::string meta_str;

 public:
  SFSMultipartUploadV2(
      rgw::sal::SFStore* _store, SFSBucket* _bucket, sfs::BucketRef _bucketref,
      const std::string& _upload_id, const std::string& _oid, ACLOwner _owner,
      ceph::real_time _mtime
  );

  virtual ~SFSMultipartUploadV2() = default;

  virtual const std::string& get_meta() const override { return meta_str; }
  virtual const std::string& get_key() const override { return oid; }
  virtual const std::string& get_upload_id() const override {
    return upload_id;
  }
  virtual const ACLOwner& get_owner() const override { return owner; }
  virtual ceph::real_time& get_mtime() override { return mtime; }
  virtual std::unique_ptr<rgw::sal::Object> get_meta_obj() override;

  /* Called from `rgw_op.cc`'s `RGWInitMultipart::execute()`, initiates a
   * multipart upload. The multipart upload is first obtained by calling on the
   * bucket's `get_multipart_upload()`, which returns the multipart upload
   * unique_ptr, and then initiates the upload.
   *
   * We expect that the multipart doesn't yet exist at this point. If it does,
   * then we return an error.
  */
  virtual int init(
      const DoutPrefixProvider* dpp, optional_yield y, ACLOwner& owner,
      rgw_placement_rule& dest_placement, rgw::sal::Attrs& attrs
  ) override;

  /**
   * @brief List this Multipart Upload's parts, according to the specified
   * parameters. This function does not return the parts list, but instead
   * populates the parent class' `parts` map; it's the SAL's responsibility to
   * obtain parts from said map.
   *
   * @param num_parts Number of parts to obtain.
   * @param marker Obtain parts with ID greater or equal to this value.
   * @param next_marker Output value, next part ID if any.
   * @param truncated  Whether there are more parts available.
   * @param assume_unsorted Ignored parameter.
   * @return int
   */
  virtual int list_parts(
      const DoutPrefixProvider* dpp, CephContext* cct, int num_parts,
      int marker, int* next_marker, bool* truncated,
      bool assume_unsorted = false
  ) override;

  /**
   * @brief Abort this Multipart Upload.
   *
   * @return 0 if successful, negative value otherwise.
   */
  virtual int abort(const DoutPrefixProvider* dpp, CephContext* cct) override;

  /**
   * @brief Mark this Multipart Upload as complete.
   *
   * @param part_etags Map of client-provided part num to part etag
   * @param remove_objs ??
   * @param accounted_size ??
   * @param compressed ??
   * @param cs_info ??
   * @param ofs ??
   * @param tag ??
   * @param owner expected bucket owner
   * @param olh_epoch ??
   * @param target_obj
   * @return int
   */
  virtual int complete(
      const DoutPrefixProvider* dpp, optional_yield y, CephContext* cct,
      std::map<int, std::string>& part_etags,
      std::list<rgw_obj_index_key>& remove_objs, uint64_t& accounted_size,
      bool& compressed, RGWCompressionInfo& cs_info, off_t& ofs,
      std::string& tag, ACLOwner& owner, uint64_t olh_epoch,
      rgw::sal::Object* target_obj
  ) override;

  /**
   * @brief Obtain information on the current multipart upload.
   *
   * @param rule Output pointer to the placement rule.
   * @param attrs Output point to the attributes map.
   * @return int 0 on success, negative otherwise.
   */
  virtual int get_info(
      const DoutPrefixProvider* dpp, optional_yield y,
      rgw_placement_rule** rule, rgw::sal::Attrs* attrs = nullptr
  ) override;

  /**
   * @brief Get Writer for this multipart.
   *
   * @param head_obj
   * @param owner
   * @param ptail_placement_rule
   * @param part_num
   * @param part_num_str
   * @return std::unique_ptr<Writer>
   */
  virtual std::unique_ptr<Writer> get_writer(
      const DoutPrefixProvider* dpp, optional_yield y,
      rgw::sal::Object* head_obj, const rgw_user& owner,
      const rgw_placement_rule* ptail_placement_rule, uint64_t part_num,
      const std::string& part_num_str
  ) override;

  /**
   * @brief Obtain a list of multipart uploads for a given bucket.
   *
   * @param bucket The bucket for which we will obtain a list of multipart
   * uploads.
   * @param prefix The prefix, on object name, to use as a filter.
   * @param marker Pagination marker, marks the first entry to obtain.
   * @param delim ???
   * @param max_uploads Maximum number of multipart uploads to obtain, per page.
   * @param uploads Vector of resulting multipart uploads.
   * @param common_prefixes  ???
   * @param is_truncated Whether the results have been truncated (i.e., there
   * are more on following pages).
   * @return 0 on success, negative otherwise.
   */
  static int list_multiparts(
      const DoutPrefixProvider* dpp, rgw::sal::SFStore* store,
      rgw::sal::SFSBucket* bucket, BucketRef bucketref,
      const std::string& prefix, std::string& marker, const std::string& delim,
      const int& max_uploads,
      std::vector<std::unique_ptr<MultipartUpload>>& uploads,
      std::map<std::string, bool>* common_prefixes, bool* is_truncated
  );

  /**
   * @brief Abort all on-going multiparts for a given bucket.
   *
   * @param bucket The bucket for which we will abort on-going multipart
   * uploads.
   * @return 0 on success, negative otherwise.
   */
  static int abort_multiparts(
      const DoutPrefixProvider* dpp, rgw::sal::SFStore* store,
      rgw::sal::SFSBucket* bucket
  );

  static inline std::string get_cls_name() { return "multipart_upload_v2"; }
};

struct SFSMultipartSerializer : public StoreMPSerializer {
  SFSMultipartSerializer() = default;

  virtual int try_lock(
      const DoutPrefixProvider* /*dpp*/, utime_t /*dur*/, optional_yield /*y*/
  ) override {
    return 0;
  }

  virtual int unlock() override { return 0; }
};

}  // namespace rgw::sal::sfs

#endif  // RGW_STORE_SFS_MULTIPART_H
