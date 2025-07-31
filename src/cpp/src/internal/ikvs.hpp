/********************************************************************************
* Copyright (c) 2025 Contributors to the Eclipse Foundation
*
* See the NOTICE file(s) distributed with this work for additional
* information regarding copyright ownership.
*
* This program and the accompanying materials are made available under the
* terms of the Apache License Version 2.0 which is available at
* https://www.apache.org/licenses/LICENSE-2.0
*
* SPDX-License-Identifier: Apache-2.0
********************************************************************************/
#ifndef SCORE_LIB_KVS_INTERNAL_IKVS_HPP
#define SCORE_LIB_KVS_INTERNAL_IKVS_HPP

#include <atomic>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>
#include "kvsvalue.hpp"
#include "internal/error.hpp"
#include "score/filesystem/filesystem.h"
#include "score/json/json_parser.h"
#include "score/json/json_writer.h"
#include "score/result/result.h"

namespace score 
{
namespace mw 
{
namespace pers 
{
namespace kvs 
{

struct InstanceId {
    size_t id;
    
    /* Constructor to initialize 'id' */
    /* Not explicit to allow implicit construction e.g. function(0) instead function(InstanceId(0)) */
    InstanceId(size_t id) { this->id = id; }
};

struct SnapshotId {
    size_t id;

    /* Constructor to initialize 'id'*/
    /* Not explicit to allow implicit construction e.g. function(0) instead function(SnapshotId(0)) */
    SnapshotId(size_t id) { this->id = id; }
};

/* Need-Defaults flag*/
enum class OpenNeedDefaults{
    Optional = 0, /* Optional: Use an empty defaults Storage if not available*/
    Required = 1 /* Required: Defaults must be available*/
};

/* Need-KVS flag*/
enum class OpenNeedKvs {
    Optional = 0, /* Optional: Use an empty KVS if no KVS is available*/
    Required = 1 /* Required: KVS must be already exist*/
};

/* Need-File flag */
enum class OpenJsonNeedFile {
    Optional = 0, /* Optional: If the file doesn't exist, start with empty data */
    Required = 1 /* Required: The file must already exist */
};


class IKvs {
public:
    virtual ~IKvs() = default;

    virtual void set_flush_on_exit(bool flush) = 0;
    virtual score::ResultBlank reset() = 0;
    virtual score::Result<std::vector<std::string_view>> get_all_keys() = 0;
    virtual score::Result<bool> key_exists(const std::string_view key) = 0;
    virtual score::Result<KvsValue> get_value(const std::string_view key) = 0;
    virtual score::Result<KvsValue> get_default_value(const std::string_view key) = 0;
    virtual score::ResultBlank reset_key(const std::string_view key) = 0;
    virtual score::Result<bool> has_default_value(const std::string_view key) = 0;
    virtual score::ResultBlank set_value(const std::string_view key, const KvsValue& value) = 0;
    virtual score::ResultBlank remove_key(const std::string_view key) = 0;
    virtual score::ResultBlank flush() = 0;
    virtual score::Result<size_t> snapshot_count() const = 0;
    virtual size_t snapshot_max_count() const = 0;
    virtual score::ResultBlank snapshot_restore(const SnapshotId& snapshot_id) = 0;
    virtual score::Result<score::filesystem::Path> get_kvs_filename(const SnapshotId& snapshot_id) const = 0;
    virtual score::Result<score::filesystem::Path> get_hash_filename(const SnapshotId& snapshot_id) const = 0;
};

} /* namespace kvs */
} /* namespace pers */
} /* namespace mw */
} /* namespace score */

#endif /* SCORE_LIB_KVS_INTERNAL_IKVS_HPP */
