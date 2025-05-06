#include "Kvs.hpp"
#include <iostream>
#include <expected>
#include <utility>
#include "kvs_rust_ffi.h"

using namespace std;
void* Kvs::kvshandle = nullptr;

Kvs::~Kvs() {
    if (kvshandle != nullptr) {
        drop_kvs(kvshandle);  
        kvshandle = nullptr;
    }
}

Result<Kvs> Kvs::open(const InstanceId& id, OpenNeedDefaults need_defaults, OpenNeedKvs need_kvs) {
  
    void* handle = nullptr;
    FFIErrorCode code = open_ffi(
        id.id,
        static_cast<uint32_t>(need_defaults),
        static_cast<uint32_t>(need_kvs),
        &handle
    );

    if (code != FFIErrorCode::Ok) {
        return std::unexpected(static_cast<ErrorCode>(code));
    }

    Kvs kvs_instance;
    kvs_instance.kvshandle = handle;

    return Result<Kvs>{ std::in_place, std::move(kvs_instance)};
}

// Set flush on exit flag
void Kvs::set_flush_on_exit(bool flush) {
    // Empty implementation
    return;
}

// Reset KVS to initial state
Result<void> Kvs::reset() {
    // Empty implementation
    return {}; 
}

// Retrieve all keys in the KVS
Result<std::vector<std::string_view>> Kvs::get_all_keys() {
    // Empty implementation
    return std::vector<std::string_view>{}; 
}

// Check if a key exists
Result<bool> Kvs::key_exists(const std::string_view key) {
    // Empty implementation
    return false; 
}

// Retrieve the value associated with a key
Result<KvsValue> Kvs::get_value(const std::string_view key) {
    // Empty implementation
    return KvsValue{nullptr};
}

// Retrieve the default value associated with a key
Result<KvsValue> Kvs::get_default_value(const std::string_view key) {
    // Empty implementation
    return KvsValue{nullptr}; 
}

// Check if a key has a default value
Result<bool> Kvs::has_default_value(const std::string_view key) {
    // Empty implementation
    return false; 
}

// Set the value for a key
Result<bool> Kvs::set_value(const std::string_view key, const KvsValue& value) {
    // Empty implementation
    return true;
}

// Remove a key-value pair
Result<void> Kvs::remove_key(const std::string_view key) {
    // Empty implementation
    return {}; 
}

// Flush the key-value store
Result<void> Kvs::flush() {
    // Empty implementation
    return {}; 
}

// Retrieve the snapshot count
size_t Kvs::snapshot_count() const {
    // Empty implementation
    return 0; 
}

// Retrieve the max snapshot count
size_t Kvs::max_snapshot_count() const {
    // Empty implementation
    return 10;
}

// Restore the key-value store from a snapshot
Result<void> Kvs::snapshot_restore(const SnapshotId& snapshot_id) {
    // Empty implementation
    return {}; 
}

// Get the filename for a snapshot
const std::string_view Kvs::get_kvs_filename(const SnapshotId& snapshot_id) const {
    // Empty implementation
    return ""; // Return an empty string as a placeholder
}

// Get the hash filename for a snapshot
const std::string_view Kvs::get_kvs_hash_filename(const SnapshotId& snapshot_id) const {
    // Empty implementation
    return "";
}
