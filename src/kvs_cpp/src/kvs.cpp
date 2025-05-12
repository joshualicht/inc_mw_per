#include "Kvs.hpp"
#include <iostream>
#include <expected>
#include "kvs_rust_ffi.h"
#include <cstring>

using namespace std;

// === Implementation of Key class ===

Key::Key() = default;

Key::Key(Key&& other) noexcept
    : id_ptr(std::exchange(other.id_ptr, std::nullopt)),
      id_len(std::exchange(other.id_len, std::nullopt)),
      keyvalue(std::move(other.keyvalue)) {}

Key& Key::operator=(Key&& other) noexcept {
    if (this != &other) {
        id_ptr = std::exchange(other.id_ptr, std::nullopt);
        id_len = std::exchange(other.id_len, std::nullopt);
        keyvalue = std::move(other.keyvalue);
    }
    return *this;
}

void Key::set_key(const char* string, size_t len) {
    if (id_ptr.has_value()) {
        throw std::runtime_error("Key already initialized");
    }
    id_ptr = string;
    id_len = len;
}

void Key::init_value(KvsValue&& value) {
    if (keyvalue.has_value()) {
        throw std::runtime_error("Value already initialized");
    }
    keyvalue = std::move(value);
}

std::string_view Key::get_id() const {
    if (id_ptr.has_value()) {
        return std::string_view(id_ptr.value(), id_len.value_or(0));
    }
    return {}; 
}

const KvsValue* Key::get_value() const {
    return keyvalue ? &(*keyvalue) : nullptr;
}

void Key::dealloc() {
    if (id_ptr.has_value()) {
        free_rust_cstring(const_cast<char*>(id_ptr.value()));
        id_ptr = std::nullopt;
        id_len = std::nullopt;
    }
}

Key::~Key() {
    dealloc();
}


// === Implementation of Filename class ===

Filename::Filename() = default;

Filename::Filename(Filename&& other) noexcept
    : id_ptr(std::exchange(other.id_ptr, std::nullopt)),
      id_len(std::exchange(other.id_len, std::nullopt)) {}

Filename& Filename::operator=(Filename&& other) noexcept {
    if (this != &other) {
        id_ptr = std::exchange(other.id_ptr, std::nullopt);
        id_len = std::exchange(other.id_len, std::nullopt);
    }
    return *this;
}

void Filename::set(const char* string, size_t len) {
    if (id_ptr.has_value()) {
        throw std::runtime_error("Filename already initialized");
    }
    id_ptr = string;
    id_len = len;
}

std::string_view Filename::get() const {
    if (id_ptr.has_value()) {
        return std::string_view(id_ptr.value(), id_len.value_or(0));
    }
    return {}; 
}

void Filename::dealloc() {
    if (id_ptr.has_value()) {
        free_rust_cstring(const_cast<char*>(id_ptr.value()));
        id_ptr = std::nullopt;
        id_len = std::nullopt;
    }
}

Filename::~Filename() {
    dealloc();
}



// === Implementation of KVS class ===

Kvs::Kvs(Kvs&& other) noexcept
  : kvshandle(other.kvshandle)
{
    other.kvshandle = nullptr;
}
Kvs& Kvs::operator=(Kvs&& other) noexcept {
    if (this != &other) {
        if (kvshandle) {
            drop_kvs(kvshandle);
        }
        kvshandle = other.kvshandle;
        other.kvshandle = nullptr;
    }
    return *this;
}

Kvs::~Kvs() {
    if (kvshandle != nullptr) {
        drop_kvs(kvshandle);  
        kvshandle = nullptr;
    }
}

Result<Kvs> Kvs::open(const InstanceId& id, OpenNeedDefaults need_defaults, OpenNeedKvs need_kvs) {
  
    void* kvshandle = nullptr;
    FFIErrorCode code = open_ffi(
        id.id,
        static_cast<uint32_t>(need_defaults),
        static_cast<uint32_t>(need_kvs),
        &kvshandle
    );

    if (code != FFIErrorCode::Ok) {
        return std::unexpected(static_cast<ErrorCode>(code));
    }

    Kvs kvs_instance;
    kvs_instance.kvshandle = kvshandle;

    return Result<Kvs>{ std::in_place, std::move(kvs_instance)};
}

// Set flush on exit flag
void Kvs::set_flush_on_exit(bool flush) {
    // Empty implementation
    return;
}

// Reset KVS to initial state
Result<void> Kvs::reset() {
    FFIErrorCode code = reset_ffi(kvshandle);
    if (code != FFIErrorCode::Ok) {
        return std::unexpected(static_cast<ErrorCode>(code));
    }
    else {
        return {};
    }
}

// Retrieve all keys in the KVS
Result<vector<Key>> Kvs::get_all_keys() {

    const char** keys_ptr = nullptr;
    size_t len = 0;
    FFIErrorCode code = get_all_keys_ffi(kvshandle, &keys_ptr, &len);
    if (code != FFIErrorCode::Ok) {
        return unexpected(static_cast<ErrorCode>(code));
    }

    vector<Key> result;
    result.reserve(len);
    for (size_t i = 0; i < len; ++i) {
        Key k;
        size_t slen = std::strlen(keys_ptr[i]);
        k.set_key(keys_ptr[i], slen);
        result.push_back(std::move(k));
    }
    free_all_keys_vec_ffi(keys_ptr, len);

    return result;
}

// Check if a key exists
Result<bool> Kvs::key_exists(const string_view key) {
    uint8_t exists = 0;
    FFIErrorCode code = key_exists_ffi(kvshandle, key.data(), &exists);
    if (code != FFIErrorCode::Ok) {
        return unexpected(static_cast<ErrorCode>(code));
    }
    return static_cast<bool>(exists);
}

// Retrieve the value associated with a key
Result<KvsValue> Kvs::get_value(const std::string_view key) {
    // Empty implementation
    //TODO implementation + refactor to key class
    return KvsValue{nullptr};
}

// Retrieve the default value associated with a key
Result<KvsValue> Kvs::get_default_value(const std::string_view key) {
    // Empty implementation
    //TODO
    return KvsValue{nullptr}; 
}

// Check if a key has a default value
Result<bool> Kvs::is_value_default(const std::string_view key) {
    uint8_t is_default = 0;
    FFIErrorCode code = is_value_default_ffi(kvshandle, key.data(), &is_default);
    if (code != FFIErrorCode::Ok) {
        return unexpected(static_cast<ErrorCode>(code));
    }
    return static_cast<bool>(is_default);
}

// Set the value for a key
Result<bool> Kvs::set_value(const std::string_view key, const KvsValue& value) {
    // Empty implementation
    //TODO
    return true;
}

// Remove a key-value pair
Result<void> Kvs::remove_key(const string_view key) {
    FFIErrorCode code = remove_key_ffi(kvshandle, key.data());
    if (code != FFIErrorCode::Ok) {
        return unexpected(static_cast<ErrorCode>(code));
    }
    return {};
}

// Flush the key-value store
Result<void> Kvs::flush() {
    FFIErrorCode code = flush_ffi(kvshandle);
    if (code != FFIErrorCode::Ok) {
        return unexpected(static_cast<ErrorCode>(code));
    }
    return {};
}

// Retrieve the snapshot count
size_t Kvs::snapshot_count() const {
    size_t count = 0;
    FFIErrorCode code = snapshot_count_ffi(kvshandle, &count);
    if (code != FFIErrorCode::Ok) {
        return 0;
    }
    return count;
}

// Retrieve the max snapshot count
size_t Kvs::max_snapshot_count() const {
    size_t max = 0;
    FFIErrorCode code = snapshot_max_count_ffi(&max);
    if (code != FFIErrorCode::Ok) {
        return 0;
    }
    return max;
}

// Restore the key-value store from a snapshot
Result<void> Kvs::snapshot_restore(const SnapshotId& snapshot_id) {
    FFIErrorCode code = snapshot_restore_ffi(kvshandle, snapshot_id.id);
    if (code != FFIErrorCode::Ok) {
        return unexpected(static_cast<ErrorCode>(code));
    }
    return {};
}

// Get the filename for a snapshot
Result<Filename> Kvs::get_kvs_filename(const SnapshotId& snapshot_id) const {

    const char*  keys_ptr = nullptr;
    FFIErrorCode code = get_kvs_filename_ffi(kvshandle, snapshot_id.id, &keys_ptr);
    if (code != FFIErrorCode::Ok) {
        return unexpected(static_cast<ErrorCode>(code));
    }
    Filename fname;
    size_t slen = std::strlen(keys_ptr);
    fname.set(keys_ptr, slen);

    return fname;
}

// Get the hash filename for a snapshot
Result<Filename> Kvs::get_kvs_hash_filename(const SnapshotId& snapshot_id) const {

    const char*  keys_ptr = nullptr;
        FFIErrorCode code = get_hash_filename_ffi(kvshandle, snapshot_id.id, &keys_ptr);
    if (code != FFIErrorCode::Ok) {
        return unexpected(static_cast<ErrorCode>(code));
    }
    Filename fname;
    size_t slen = std::strlen(keys_ptr);
    fname.set(keys_ptr, slen);

    return fname;
}