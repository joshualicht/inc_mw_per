#ifndef SCORE_LIB_KVS_IKVS_HPP
#define SCORE_LIB_KVS_IKVS_HPP

#include <atomic>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>
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


/* @brief */
enum class MyErrorCode : score::result::ErrorCode {
    /* Error that was not yet mapped*/
    UnmappedError,

    /* File not found*/
    FileNotFound,

    /* KVS file read error*/
    KvsFileReadError,

    /* KVS hash file read error*/
    KvsHashFileReadError,

    /* JSON parser error*/
    JsonParserError,

    /* JSON generator error*/
    JsonGeneratorError,

    /* Physical storage failure*/
    PhysicalStorageFailure,

    /* Integrity corrupted*/
    IntegrityCorrupted,

    /* Validation failed*/
    ValidationFailed,

    /* Encryption failed*/
    EncryptionFailed,

    /* Resource is busy*/
    ResourceBusy,

    /* Out of storage space*/
    OutOfStorageSpace,

    /* Quota exceeded*/
    QuotaExceeded,

    /* Authentication failed*/
    AuthenticationFailed,

    /* Key not found*/
    KeyNotFound,
    
    /* Key default value not found*/
    KeyDefaultNotFound,

    /* Serialization failed*/
    SerializationFailed,

    /* Invalid snapshot ID*/
    InvalidSnapshotId,

    /* Conversion failed*/
    ConversionFailed,

    /* Mutex failed*/
    MutexLockFailed,

    /* Invalid value type*/
    InvalidValueType,

    /* Invalid argument*/
    InvalidArgument
};

class MyErrorDomain final : public score::result::ErrorDomain
{
public:
    std::string_view MessageFor(score::result::ErrorCode const& code) const noexcept override;
};

constexpr MyErrorDomain my_error_domain;
score::result::Error MakeError(MyErrorCode code, std::string_view user_message = "") noexcept;


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

/* Define the KvsValue class*/
/**
 * @class KvsValue
 * @brief Represents a flexible value type that can hold various data types, 
 *        including numbers, booleans, strings, null, arrays, and objects.
 * 
 * The KvsValue class provides a type-safe way to store and retrieve values of 
 * different types. It uses a std::variant to hold the underlying value and an 
 * enum to track the type of the value.
 * 
 * ## Supported Types:
 * - Number (double)
 * - Boolean (bool)
 * - String (std::string)
 * - Null (std::nullptr_t)
 * - Array (std::vector<KvsValue>)
 * - Object (std::unordered_map<std::string, KvsValue>)
 * 
 * ## Public Methods:
 * - `KvsValue(double number)`: Constructs a KvsValue holding a number.
 * - `KvsValue(bool boolean)`:
 * - Access the underlying value using `getValue()` and `std::get`.
 *
 * ## Example:
 * @code
 * KvsValue numberValue(42.0);
 * KvsValue stringValue("Hello, World!");
 * KvsValue arrayValue(KvsValue::Array{numberValue, stringValue});
 *
 * if (numberValue.getType() == KvsValue::Type::Number) {
 *     double number = std::get<double>(numberValue.getValue());
 * }
 * @endcode
 */

class KvsValue final{
public:
    /* Define the possible types for KvsValue*/
    using Array = std::vector<KvsValue>;
    using Object = std::unordered_map<std::string, KvsValue>;

    /* Enum to represent the type of the value*/
    enum class Type {
        i32,
        u32,
        i64,
        u64,
        f64,
        Boolean,
        String,
        Null,
        Array,
        Object
    };

    /* Constructors for each type*/
    explicit KvsValue(int32_t number) : value(number), type(Type::i32) {}
    explicit KvsValue(uint32_t number) : value(number), type(Type::u32) {}
    explicit KvsValue(int64_t number) : value(number), type(Type::i64) {}
    explicit KvsValue(uint64_t number) : value(number), type(Type::u64) {}
    explicit KvsValue(double number) : value(number), type(Type::f64) {}
    explicit KvsValue(bool boolean) : value(boolean), type(Type::Boolean) {}
    explicit KvsValue(const std::string& str) : value(str), type(Type::String) {}
    explicit KvsValue(std::nullptr_t) : value(nullptr), type(Type::Null) {}
    explicit KvsValue(const Array& array) : value(array), type(Type::Array) {}
    explicit KvsValue(const Object& object) : value(object), type(Type::Object) {}

    /* Get the type of the value*/
    Type getType() const { return type; }

    /* Access the underlying value (use std::get to retrieve the value)*/
    const std::variant<int32_t, uint32_t, int64_t, uint64_t, double, bool, std::string, std::nullptr_t, Array, Object>& getValue() const {
        return value;
    }

private:
    /* The underlying value*/
    std::variant<int32_t, uint32_t, int64_t, uint64_t, double, bool, std::string, std::nullptr_t, Array, Object> value;

    /* The type of the value*/
    Type type;
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

class IKvsBuilder {
public:
    virtual ~IKvsBuilder() = default;

    virtual IKvsBuilder& need_defaults_flag(bool flag) = 0;
    virtual IKvsBuilder& need_kvs_flag(bool flag) = 0;
    virtual IKvsBuilder& dir(std::string&& dir_path) = 0;
    virtual score::Result<std::unique_ptr<IKvs>> build() = 0;
};

} /* namespace kvs */
} /* namespace pers */
} /* namespace mw */
} /* namespace score */

#endif /* SCORE_LIB_KVS_IKVS_HPP */
