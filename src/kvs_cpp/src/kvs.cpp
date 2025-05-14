#include "Kvs.hpp"
#include <iostream>
#include <expected>
#include "kvs_rust_ffi.h"
#include <cstring>

using namespace std;


/* --------------------------------------------------------------------*/
/* Helper functions to convert between KvsValue and FFI_KvsValue*/

static KvsValue kvsvalue_conversion_rust_to_cpp(const FFI_KvsValue& value) {
    KvsValue result = KvsValue(nullptr);

    switch (value.type_) {
      case FFI_KvsValueType::Number:
        result = KvsValue(value.number);
        break;

      case FFI_KvsValueType::Boolean:
        result = KvsValue(static_cast<bool>(value.boolean));
        break;

      case FFI_KvsValueType::String:
        result = KvsValue(std::string(value.string));
        break;

      case FFI_KvsValueType::Null:
        result = KvsValue(nullptr);
        break;

      case FFI_KvsValueType::Array: {
        std::vector<KvsValue> arr;
        arr.reserve(value.array_len);
        for (size_t i = 0; i < value.array_len; ++i) {
          arr.push_back(kvsvalue_conversion_rust_to_cpp(value.array_ptr[i]));
        }
        result = KvsValue(arr);
        break;
      }

      case FFI_KvsValueType::Object: {
        std::unordered_map<std::string, KvsValue> obj;
        obj.reserve(value.obj_len);
        for (size_t i = 0; i < value.obj_len; ++i) {
          const char* key = value.obj_keys[i];
          obj.emplace(
            std::string(key),
            kvsvalue_conversion_rust_to_cpp(value.obj_values[i])
          );
        }
        result = KvsValue(obj);
        break;
      }
    }

    return result;
}

static void kvsvalue_conversion_cpp_to_rust(const KvsValue& value, FFI_KvsValue* out) {
    switch (value.getType()) {
        case KvsValue::Type::Number:
            out->type_     = FFI_KvsValueType::Number;
            out->number    = std::get<double>(value.getValue());
            break;

        case KvsValue::Type::Boolean:
            out->type_     = FFI_KvsValueType::Boolean;
            out->boolean   = static_cast<uint8_t>(std::get<bool>(value.getValue()));
            break;

        case KvsValue::Type::String: {
            out->type_     = FFI_KvsValueType::String;
            auto& s        = std::get<std::string>(value.getValue());
            /* strdup allocates with malloc */
            out->string    = strdup(s.c_str());
            break;
        }

        case KvsValue::Type::Null:
            out->type_     = FFI_KvsValueType::Null;
            break;

        case KvsValue::Type::Array: {
            out->type_     = FFI_KvsValueType::Array;
            auto& arr      = std::get<KvsValue::Array>(value.getValue());
            size_t len     = arr.size();
            /* malloc*/
            out->array_ptr = (FFI_KvsValue*)std::malloc(len * sizeof(FFI_KvsValue));
            out->array_len = len;
            for (size_t i = 0; i < len; ++i) {
                kvsvalue_conversion_cpp_to_rust(arr[i], &out->array_ptr[i]);
            }
            break;
        }

        case KvsValue::Type::Object: {
            out->type_     = FFI_KvsValueType::Object;
            auto& obj      = std::get<KvsValue::Object>(value.getValue());
            size_t len     = obj.size();
            out->obj_keys  = (const char**)std::malloc(len * sizeof(const char*));
            out->obj_values  = (FFI_KvsValue*)std::malloc(len * sizeof(FFI_KvsValue));
            out->obj_len   = len;
            size_t idx = 0;
            for (auto& [k, vv] : obj) {
                out->obj_keys[idx] = strdup(k.c_str());
                kvsvalue_conversion_cpp_to_rust(vv, &out->obj_values[idx]);
                ++idx;
            }
            break;
        }
    }
}

/*Free FFI_KvsValue created in CPP */
static void free_ffi_kvsvalue_cpp(FFI_KvsValue* value) {
    switch (value->type_) {
      case FFI_KvsValueType::String:
        std::free((void*)value->string);
        break;
      case FFI_KvsValueType::Array:
        for (size_t i = 0; i < value->array_len; ++i) {
          free_ffi_kvsvalue_cpp(&value->array_ptr[i]);
        }
        std::free(value->array_ptr);
        break;
      case FFI_KvsValueType::Object:
        for (size_t i = 0; i < value->obj_len; ++i) {
          std::free((void*)value->obj_keys[i]);
          free_ffi_kvsvalue_cpp(&value->obj_values[i]);
        }
        std::free(value->obj_keys);
        std::free(value->obj_values);
        break;
      default:
        break;
    }
}

/* === Implementation of Key class ===*/

Key::Key() = default;

/* Constructor to initialize Key object */
/* Make sure, that the move constructor and move assignment operator reset the
 * id_ptr and id_len to nullptr, so that the destructor does not free the
 * memory twice. */
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
    std::string_view result = {};  

    if (id_ptr.has_value()) {
        result = std::string_view(id_ptr.value(), id_len.value_or(0));
    }

    return result;
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
    /*Freeing the KVSValue will be done automatically by the destructor of KVSValue*/
}

Key::~Key() {
    dealloc();
}


/* === Implementation of Filename class ===*/

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
    std::string_view result = {}; 

    if (id_ptr.has_value()) {
        result = std::string_view(id_ptr.value(), id_len.value_or(0));
    }

    return result;
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



/* === Implementation of KVS class ===*/
/* Constructor to initialize KVS object */
/* Make sure, that the move constructor and move assignment operator reset the
 * kvshandle to nullptr, so that the destructor does not free the
 * kvs instance twice. */
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

    Result<Kvs> result = std::unexpected(ErrorCode::UnmappedError); /* Needs default value because of std::expected in combination with kvs object*/
    if (code != FFIErrorCode::Ok) {
        return std::unexpected(static_cast<ErrorCode>(code));
    }
    else{
        Kvs kvs_instance;
        kvs_instance.kvshandle = kvshandle;
        result = Result<Kvs>{ std::in_place, std::move(kvs_instance)};
    }

    return result;
}

/* Set flush on exit flag*/
void Kvs::set_flush_on_exit(bool flush) {
    /* Empty implementation*/
    return;
}

/* Reset KVS to initial state*/
Result<void> Kvs::reset() {
    FFIErrorCode code = reset_ffi(kvshandle);

    Result<void> result;
    if (code == FFIErrorCode::Ok) {
        result = {};
    }
    else {
        result = std::unexpected(static_cast<ErrorCode>(code));
    }

    return result;
}

/* Retrieve all keys in the KVS*/
Result<std::vector<Key>> Kvs::get_all_keys() {
    const char** keys_ptr = nullptr;
    size_t len = 0;
    FFIErrorCode code = get_all_keys_ffi(kvshandle, &keys_ptr, &len);

    Result<std::vector<Key>> result;
    if (code == FFIErrorCode::Ok) {
        std::vector<Key> temp_result;
        temp_result.reserve(len);
        for (size_t i = 0; i < len; ++i) {
            Key k;
            size_t slen = std::strlen(keys_ptr[i]);
            k.set_key(keys_ptr[i], slen);
            temp_result.push_back(std::move(k));
        }
        free_all_keys_vec_ffi(keys_ptr, len);

        result = std::move(temp_result);
    }
    else {
        result = std::unexpected(static_cast<ErrorCode>(code));
    }

    return result;
}

/* Check if a key exists*/
Result<bool> Kvs::key_exists(const string_view key) {
    uint8_t exists = 0;
    FFIErrorCode code = key_exists_ffi(kvshandle, key.data(), &exists);
    
    Result<bool> result;
    if (code == FFIErrorCode::Ok) {
        result = static_cast<bool>(exists);
    }
    else {
        result = std::unexpected(static_cast<ErrorCode>(code));
    }
    
    return result;
}

/* Retrieve the value associated with a key*/
/*Result<Key> Kvs::get_value(const std::string_view key) {
    Empty implementation: TODO implementation + refactor to key class
}/*

/*Retrieve the default value associated with a key*/
Result<Key> Kvs::get_default_value(const std::string_view key) {
    FFI_KvsValue value;
    FFIErrorCode code = get_default_value_ffi(
        kvshandle,
        key.data(),
        &value
    );
    Result<Key> result;
    if (code == FFIErrorCode::Ok) {
        KvsValue cpp = kvsvalue_conversion_rust_to_cpp(value);
        Key k;
        k.init_value(std::move(cpp));
        free_ffi_kvsvalue_rust(&value);
        result = std::move(k);
    }
    else {
        result = std::unexpected(static_cast<ErrorCode>(code));
    }
    
    return result;
}

/* Check if a key has a default value*/
Result<bool> Kvs::is_value_default(const std::string_view key) {
    uint8_t is_default = 0;
    FFIErrorCode code = is_value_default_ffi(kvshandle, key.data(), &is_default);

    Result<bool> result;
    if (code == FFIErrorCode::Ok) {
        result = is_default;
    }
    else{
        result = std::unexpected(static_cast<ErrorCode>(code));
    }
    
    return result;
}

/* Set the value for a key*/
Result<bool> Kvs::set_value(const std::string_view key,
                            const KvsValue& value) {
    FFI_KvsValue ffi;
    kvsvalue_conversion_cpp_to_rust(value, &ffi);

    FFIErrorCode code = set_value_ffi(kvshandle, key.data(), &ffi);
    free_ffi_kvsvalue_cpp(&ffi);

    Result<bool> result;
    if (code == FFIErrorCode::Ok) {
        result = true;
    }
    else{
        result = std::unexpected(static_cast<ErrorCode>(code));
    }

    return result;
}

/* Remove a key-value pair*/
Result<void> Kvs::remove_key(const string_view key) {
    FFIErrorCode code = remove_key_ffi(kvshandle, key.data());

    Result<void> result;
    if (code == FFIErrorCode::Ok) {
        result = {};
    }
    else{
        result = std::unexpected(static_cast<ErrorCode>(code));
    }

    return result;
}

/* Flush the key-value store*/
Result<void> Kvs::flush() {
    FFIErrorCode code = flush_ffi(kvshandle);

    Result<void> result;
    if (code == FFIErrorCode::Ok) {
        result = {};
    }
    else{
        result = std::unexpected(static_cast<ErrorCode>(code));
    }

    return result;
}

/* Retrieve the snapshot count*/
size_t Kvs::snapshot_count() const {
    size_t count = 0;
    FFIErrorCode code = snapshot_count_ffi(kvshandle, &count);

    return (code == FFIErrorCode::Ok) ? count : 0;
}

/* Retrieve the max snapshot count*/
size_t Kvs::max_snapshot_count() const {
    size_t max = 0;
    FFIErrorCode code = snapshot_max_count_ffi(&max);

    return (code == FFIErrorCode::Ok) ? max : 0;
}

/* Restore the key-value store from a snapshot*/
Result<void> Kvs::snapshot_restore(const SnapshotId& snapshot_id) {
    FFIErrorCode code = snapshot_restore_ffi(kvshandle, snapshot_id.id);

    Result<void> result;
    if (code == FFIErrorCode::Ok) {
        result = {};
    }else{
        result = std::unexpected(static_cast<ErrorCode>(code));
    }

    return result;
}

/* Get the filename for a snapshot*/
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

/* Get the hash filename for a snapshot*/
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