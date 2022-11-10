// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_op_type.h"

const char *rgw_op_type_str(RGWOpType t) {
  switch (t) {
  case RGW_OP_UNKNOWN:
    return "unknown";

  case RGW_OP_GET_OBJ:
    return "get_obj";

  case RGW_OP_LIST_BUCKETS:
    return "list_buckets";

  case RGW_OP_STAT_ACCOUNT:
    return "stat_account";

  case RGW_OP_LIST_BUCKET:
    return "list_bucket";

  case RGW_OP_GET_BUCKET_LOGGING:
    return "get_bucket_logging";

  case RGW_OP_GET_BUCKET_LOCATION:
    return "get_bucket_location";

  case RGW_OP_GET_BUCKET_VERSIONING:
    return "get_bucket_versioning";

  case RGW_OP_SET_BUCKET_VERSIONING:
    return "set_bucket_versioning";

  case RGW_OP_GET_BUCKET_WEBSITE:
    return "get_bucket_website";

  case RGW_OP_SET_BUCKET_WEBSITE:
    return "set_bucket_website";

  case RGW_OP_STAT_BUCKET:
    return "stat_bucket";

  case RGW_OP_CREATE_BUCKET:
    return "create_bucket";

  case RGW_OP_DELETE_BUCKET:
    return "delete_bucket";

  case RGW_OP_PUT_OBJ:
    return "put_obj";

  case RGW_OP_STAT_OBJ:
    return "stat_obj";

  case RGW_OP_POST_OBJ:
    return "post_obj";

  case RGW_OP_PUT_METADATA_ACCOUNT:
    return "put_metadata_account";

  case RGW_OP_PUT_METADATA_BUCKET:
    return "put_metadata_bucket";

  case RGW_OP_PUT_METADATA_OBJECT:
    return "put_metadata_object";

  case RGW_OP_SET_TEMPURL:
    return "set_tempurl";

  case RGW_OP_DELETE_OBJ:
    return "delete_obj";

  case RGW_OP_COPY_OBJ:
    return "copy_obj";

  case RGW_OP_GET_ACLS:
    return "get_acls";

  case RGW_OP_PUT_ACLS:
    return "put_acls";

  case RGW_OP_GET_CORS:
    return "get_cors";

  case RGW_OP_PUT_CORS:
    return "put_cors";

  case RGW_OP_DELETE_CORS:
    return "delete_cors";

  case RGW_OP_OPTIONS_CORS:
    return "options_cors";

  case RGW_OP_GET_BUCKET_ENCRYPTION:
    return "get_bucket_encryption";

  case RGW_OP_PUT_BUCKET_ENCRYPTION:
    return "put_bucket_encryption";

  case RGW_OP_DELETE_BUCKET_ENCRYPTION:
    return "delete_bucket_encryption";

  case RGW_OP_GET_REQUEST_PAYMENT:
    return "get_request_payment";

  case RGW_OP_SET_REQUEST_PAYMENT:
    return "set_request_payment";

  case RGW_OP_INIT_MULTIPART:
    return "init_multipart";

  case RGW_OP_COMPLETE_MULTIPART:
    return "complete_multipart";

  case RGW_OP_ABORT_MULTIPART:
    return "abort_multipart";

  case RGW_OP_LIST_MULTIPART:
    return "list_multipart";

  case RGW_OP_LIST_BUCKET_MULTIPARTS:
    return "list_bucket_multiparts";

  case RGW_OP_DELETE_MULTI_OBJ:
    return "delete_multi_obj";

  case RGW_OP_BULK_DELETE:
    return "bulk_delete";

  case RGW_OP_GET_KEYS:
    return "get_keys";

  case RGW_OP_GET_ATTRS:
    return "get_attrs";

  case RGW_OP_DELETE_ATTRS:
    return "delete_attrs";

  case RGW_OP_SET_ATTRS:
    return "set_attrs";

  case RGW_OP_GET_CROSS_DOMAIN_POLICY:
    return "get_cross_domain_policy";

  case RGW_OP_GET_HEALTH_CHECK:
    return "get_health_check";

  case RGW_OP_GET_INFO:
    return "get_info";

  case RGW_OP_CREATE_ROLE:
    return "create_role";

  case RGW_OP_DELETE_ROLE:
    return "delete_role";

  case RGW_OP_GET_ROLE:
    return "get_role";

  case RGW_OP_MODIFY_ROLE_TRUST_POLICY:
    return "modify_role_trust_policy";

  case RGW_OP_LIST_ROLES:
    return "list_roles";

  case RGW_OP_PUT_ROLE_POLICY:
    return "put_role_policy";

  case RGW_OP_GET_ROLE_POLICY:
    return "get_role_policy";

  case RGW_OP_LIST_ROLE_POLICIES:
    return "list_role_policies";

  case RGW_OP_DELETE_ROLE_POLICY:
    return "delete_role_policy";

  case RGW_OP_TAG_ROLE:
    return "tag_role";

  case RGW_OP_LIST_ROLE_TAGS:
    return "list_role_tags";

  case RGW_OP_UNTAG_ROLE:
    return "untag_role";

  case RGW_OP_UPDATE_ROLE:
    return "update_role";

  case RGW_OP_PUT_BUCKET_POLICY:
    return "put_bucket_policy";

  case RGW_OP_GET_BUCKET_POLICY:
    return "get_bucket_policy";

  case RGW_OP_DELETE_BUCKET_POLICY:
    return "delete_bucket_policy";

  case RGW_OP_PUT_OBJ_TAGGING:
    return "put_obj_tagging";

  case RGW_OP_GET_OBJ_TAGGING:
    return "get_obj_tagging";

  case RGW_OP_DELETE_OBJ_TAGGING:
    return "delete_obj_tagging";

  case RGW_OP_PUT_LC:
    return "put_lc";

  case RGW_OP_GET_LC:
    return "get_lc";

  case RGW_OP_DELETE_LC:
    return "delete_lc";

  case RGW_OP_PUT_USER_POLICY:
    return "put_user_policy";

  case RGW_OP_GET_USER_POLICY:
    return "get_user_policy";

  case RGW_OP_LIST_USER_POLICIES:
    return "list_user_policies";

  case RGW_OP_DELETE_USER_POLICY:
    return "delete_user_policy";

  case RGW_OP_PUT_BUCKET_OBJ_LOCK:
    return "put_bucket_obj_lock";

  case RGW_OP_GET_BUCKET_OBJ_LOCK:
    return "get_bucket_obj_lock";

  case RGW_OP_PUT_OBJ_RETENTION:
    return "put_obj_retention";

  case RGW_OP_GET_OBJ_RETENTION:
    return "get_obj_retention";

  case RGW_OP_PUT_OBJ_LEGAL_HOLD:
    return "put_obj_legal_hold";

  case RGW_OP_GET_OBJ_LEGAL_HOLD:
    return "get_obj_legal_hold";

  case RGW_OP_ADMIN_SET_METADATA:
    return "admin_set_metadata";

  case RGW_OP_GET_OBJ_LAYOUT:
    return "get_obj_layout";

  case RGW_OP_BULK_UPLOAD:
    return "bulk_upload";

  case RGW_OP_METADATA_SEARCH:
    return "metadata_search";

  case RGW_OP_CONFIG_BUCKET_META_SEARCH:
    return "config_bucket_meta_search";

  case RGW_OP_GET_BUCKET_META_SEARCH:
    return "get_bucket_meta_search";

  case RGW_OP_DEL_BUCKET_META_SEARCH:
    return "del_bucket_meta_search";

  case RGW_OP_SYNC_DATALOG_NOTIFY:
    return "sync_datalog_notify";

  case RGW_OP_SYNC_DATALOG_NOTIFY2:
    return "sync_datalog_notify2";

  case RGW_OP_SYNC_MDLOG_NOTIFY:
    return "sync_mdlog_notify";

  case RGW_OP_PERIOD_POST:
    return "period_post";

  case RGW_STS_ASSUME_ROLE:
    return "sts_assume_role";

  case RGW_STS_GET_SESSION_TOKEN:
    return "sts_get_session_token";

  case RGW_STS_ASSUME_ROLE_WEB_IDENTITY:
    return "sts_assume_role_web_identity";

  case RGW_OP_PUBSUB_TOPIC_CREATE:
    return "pubsub_topic_create";

  case RGW_OP_PUBSUB_TOPICS_LIST:
    return "pubsub_topics_list";

  case RGW_OP_PUBSUB_TOPIC_GET:
    return "pubsub_topic_get";

  case RGW_OP_PUBSUB_TOPIC_DELETE:
    return "pubsub_topic_delete";

  case RGW_OP_PUBSUB_SUB_CREATE:
    return "pubsub_sub_create";

  case RGW_OP_PUBSUB_SUB_GET:
    return "pubsub_sub_get";

  case RGW_OP_PUBSUB_SUB_DELETE:
    return "pubsub_sub_delete";

  case RGW_OP_PUBSUB_SUB_PULL:
    return "pubsub_sub_pull";

  case RGW_OP_PUBSUB_SUB_ACK:
    return "pubsub_sub_ack";

  case RGW_OP_PUBSUB_NOTIF_CREATE:
    return "pubsub_notif_create";

  case RGW_OP_PUBSUB_NOTIF_DELETE:
    return "pubsub_notif_delete";

  case RGW_OP_PUBSUB_NOTIF_LIST:
    return "pubsub_notif_list";

  case RGW_OP_GET_BUCKET_TAGGING:
    return "get_bucket_tagging";

  case RGW_OP_PUT_BUCKET_TAGGING:
    return "put_bucket_tagging";

  case RGW_OP_DELETE_BUCKET_TAGGING:
    return "delete_bucket_tagging";

  case RGW_OP_GET_BUCKET_REPLICATION:
    return "get_bucket_replication";

  case RGW_OP_PUT_BUCKET_REPLICATION:
    return "put_bucket_replication";

  case RGW_OP_DELETE_BUCKET_REPLICATION:
    return "delete_bucket_replication";

  case RGW_OP_GET_BUCKET_POLICY_STATUS:
    return "get_bucket_policy_status";

  case RGW_OP_PUT_BUCKET_PUBLIC_ACCESS_BLOCK:
    return "put_bucket_public_access_block";

  case RGW_OP_GET_BUCKET_PUBLIC_ACCESS_BLOCK:
    return "get_bucket_public_access_block";

  case RGW_OP_DELETE_BUCKET_PUBLIC_ACCESS_BLOCK:
    return "delete_bucket_public_access_block";

  case RGW_OP_CREATE_OIDC_PROVIDER:
    return "create_oidc_provider";

  case RGW_OP_DELETE_OIDC_PROVIDER:
    return "delete_oidc_provider";

  case RGW_OP_GET_OIDC_PROVIDER:
    return "get_oidc_provider";

  case RGW_OP_LIST_OIDC_PROVIDERS:
    return "list_oidc_providers";

  default:
    return "?";
  }
}

std::ostream &operator<<(std::ostream &os, RGWOpType t) {
  os << rgw_op_type_str(t);
  return os;
}
