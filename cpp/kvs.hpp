// Copyright (c) 2025 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Apache License Version 2.0 which is available at
// <https://www.apache.org/licenses/LICENSE-2.0>
//
// SPDX-License-Identifier: Apache-2.0
//
// Example code for a Key-Value Store (KVS) in C++
//

#include <string>
#include <unordered_map>
#include <mutex>
#include <atomic>
#include <variant>
#include <utility>
#include <vector>
#include "score/result/result.h"
#include "score/result/details/expected/expected.h"
#include "score/result/details/expected/extensions.h"
#include "score/result/error.h"
#include "score/json/json_parser.h"
#include "score/json/json_writer.h"
#include <optional>
#include <stdexcept>

#define KVS_MAX_SNAPSHOTS 3

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

    /* Serialization failed*/
    SerializationFailed,

    /* Invalid snapshot ID*/
    InvalidSnapshotId,

    /* Conversion failed*/
    ConversionFailed,

    /* Mutex failed*/
    MutexLockFailed
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
    
    /* Constructor to initialize 'id'*/
    explicit InstanceId(size_t id) { this->id = id; }
};

struct SnapshotId {
    size_t id;

    /* Constructor to initialize 'id'*/
    explicit SnapshotId(size_t id) { this->id = id; }
};

/* Need-Defaults flag*/
enum class OpenNeedDefaults{
    Optional = 0, /* Optional: Open defaults only if available*/
    Required = 1 /* Required: Defaults must be available*/
};

/* Need-KVS flag*/
enum class OpenNeedKvs {
    Optional = 0, /* Optional: Use an empty KVS if no KVS is available*/
    Required = 1 /* Required: KVS must be already exist*/
};

/* Need-File flag */
enum class OpenJsonNeedFile {
    Optional = 0, /* Optional: Use an empty KVS if no KVS is available*/
    Required = 1 /* Required: KVS must be already exist*/
};
/* Verify-Hash flag */
enum class OpenJsonVerifyHash {
    No = 0, /* Optional: Use an empty KVS if no KVS is available*/
    Yes = 1 /* Required: KVS must be already exist*/
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

class KvsValue {
public:
    /* Define the possible types for KvsValue*/
    using Array = std::vector<KvsValue>;
    using Object = std::unordered_map<std::string, KvsValue>;

    /* Enum to represent the type of the value*/
    enum class Type {
        Number,
        Boolean,
        String,
        Null,
        Array,
        Object
    };

    /* Constructors for each type*/
    explicit KvsValue(double number) : value(number), type(Type::Number) {}
    explicit KvsValue(bool boolean) : value(boolean), type(Type::Boolean) {}
    explicit KvsValue(const std::string& str) : value(str), type(Type::String) {}
    explicit KvsValue(std::nullptr_t) : value(nullptr), type(Type::Null) {}
    explicit KvsValue(const Array& array) : value(array), type(Type::Array) {}
    explicit KvsValue(const Object& object) : value(object), type(Type::Object) {}

    /* Get the type of the value*/
    Type getType() const { return type; }

    /* Access the underlying value (use std::get to retrieve the value)*/
    const std::variant<double, bool, std::string, std::nullptr_t, Array, Object>& getValue() const {
        return value;
    }

private:
    /* The underlying value*/
    std::variant<double, bool, std::string, std::nullptr_t, Array, Object> value;

    /* The type of the value*/
    Type type;
};


/**
 * @class Kvs
 * @brief A thread-safe key-value store (KVS) CPP Wrapper for the Rust KVS implementation with support for default values, snapshots, and persistence.
 * 
 * The Kvs class provides an interface for managing a key-value store with features such as:
 * - Support for default values.
 * - Snapshot management for persistence and restoration.
 * - Configurable flush-on-exit behavior.
 * 
 * Features: (-> Done by Rust KVS)
 * - `FEAT_REQ__KVS__thread_safety`: Ensures thread safety using a mutex.
 * - `FEAT_REQ__KVS__default_values`: Allows optional default values for keys.
 * 
 * Public Methods:
 * - `open`: Opens the KVS with a specified instance ID and flags.
 * - `set_flush_on_exit`: Configures whether the KVS should flush to storage on exit.
 * - `get_all_keys`: Retrieves all keys stored in the KVS.
 * - `key_exists`: Checks if a specific key exists in the KVS.
 * - `get_value`: Retrieves the value associated with a specific key.
 * - `get_default_value`: Retrieves the default value associated with a specific key.
 * - `has_default_value`: Checks if a default value exists for a specific key.
 * - `set_value`: Sets the value for a specific key in the KVS.
 * - `remove_key`: Removes a specific key from the KVS.
 * - `flush`: Flushes the KVS to storage.
 * - `snapshot_count`: Retrieves the number of available snapshots.
 * - `max_snapshot_count`: Retrieves the maximum number of snapshots allowed.
 * - `snapshot_restore`: Restores the KVS from a specified snapshot.
 * - `get_kvs_filename`: Retrieves the filename associated with a snapshot.
 * - `get_kvs_hash_filename`: Retrieves the hash filename associated with a snapshot.
 * 
 * Private Members:
 * - `kvs_mutex`: A mutex for ensuring thread safety.
 * - `kvs`: An unordered map for storing key-value pairs.
 * - `default_values`: An unordered map for storing optional default values.
 * - `filename_prefix`: A string prefix for filenames associated with snapshots.
 * - `flush_on_exit`: An atomic boolean flag indicating whether to flush on exit.
 * 
 * ----------------Notice----------------
 * Blank should be used instead of void for Result class
 * Refer: "Blank and score::ResultBlank shall be used for `T` instead of `void`" in result.h
 */
class Kvs {
    public:

        ~Kvs();
        
        // Deleted copy constructor and assignment operator to prevent copying
        Kvs(const Kvs&) = delete;
        Kvs& operator=(const Kvs&) = delete;

        // Default move constructor and assignment operator
        Kvs(Kvs&& other) noexcept;
        Kvs& operator=(Kvs&& other) noexcept;

        /**
         * @brief Opens the key-value store with the specified instance ID and flags.
         * 
         * This function initializes and opens the key-value store (KVS) for a given instance ID. 
         * It allows the caller to specify whether default values and an existing KVS are required 
         * or optional during the opening process.
         * 
         * @param id The instance ID of the KVS. This uniquely identifies the KVS instance.
         * @param need_defaults A flag of type OpenNeedDefaults indicating whether default values 
         *                      are required or optional.
         *                      - OpenNeedDefaults::Required: Default values must be available.
         *                      - OpenNeedDefaults::Optional: Default values are optional.
         * @param need_kvs A flag of type OpenNeedKvs indicating whether the KVS is required or optional.
         *                 - OpenNeedKvs::Required: The KVS must already exist.
         *                 - OpenNeedKvs::Optional: An empty KVS will be used if no KVS exists.
         * 
         * @return A Result object containing either:
         *         - A Kvs object if the operation is successful.
         *         - An ErrorCode if an error occurs during the operation.
         * 
         * Possible Error Codes:
         * - ErrorCode::FileNotFound: The KVS file was not found.
         * - ErrorCode::KvsFileReadError: An error occurred while reading the KVS file.
         * - ErrorCode::IntegrityCorrupted: The KVS integrity is corrupted.
         * - ErrorCode::ValidationFailed: Validation of the KVS data failed.
         * - ErrorCode::ResourceBusy: The KVS resource is currently in use.
         */
        static score::Result<Kvs> open(const InstanceId& id, OpenNeedDefaults need_defaults, OpenNeedKvs need_kvs, const std::optional<std::string>& dir);

        /**
         * @brief Sets whether the key-value store should flush its contents to
         *        persistent storage upon program exit.
         * 
         * @param flush A boolean value indicating whether to enable or disable
         *              flushing on exit. If true, the store will flush its
         *              contents to persistent storage when the program exits.
         *              If false, the store will not perform this operation.
         */
        void set_flush_on_exit(bool flush);

        /**
         * @brief Resets a key-value-storage to its initial state
         * 
         */
        score::ResultBlank reset();

    
        /**
         * @brief Retrieves all keys stored in the key-value store.
         * 
         * @return score::Result<std::vector<std::Key>, ErrorCode> 
         *         A score::Result object containing a vector of all keys on success, 
         *         or an error code on failure.
         */
        score::Result<std::vector<std::string>> get_all_keys();


        /**
         * @brief Checks if a key exists in the key-value store.
         * 
         * @param key The key to check for existence, provided as a std::string_view.
         * @return score::Result<bool, ErrorCode> 
         *         - On success: A score::Result containing `true` if the key exists, or `false` if it does not.
         *         - On failure: A score::Result containing an appropriate ErrorCode.
         */
        score::Result<bool> key_exists(const std::string_view key);


        /**
         * @brief Retrieves the value associated with the specified key from the key-value store.
         * 
         * @param key The key for which the value is to be retrieved. It is passed as a string view to avoid unnecessary copying.
         * @return A score::Result object containing either the retrieved value (of type Key) or an error code (of type ErrorCode) 
         *         if the operation fails.
         */
        score::Result<KvsValue> get_value(const std::string_view key);


        /**
         * @brief Retrieves the default value associated with the specified key.
         * 
         * This function attempts to fetch the default value for a given key from the
         * key-value store. If the key exists, the corresponding value is returned.
         * Otherwise, an error code is provided to indicate the failure reason.
         * 
         * @param key The key for which the default value is to be retrieved.
         *            It is passed as a string view to avoid unnecessary string copies.
         * 
         * @return A score::Result object containing either the default value (Key) if
         *         the operation is successful, or an ErrorCode indicating the error
         *         if the operation fails.
         */
        score::Result<KvsValue> get_default_value(const std::string_view key);


        /**
         * @brief Checks if the specified key has a default value.
         * 
         * This function determines whether the given key is associated with a default
         * value in the key-value store.
         * 
         * @param key The key to check, provided as a string view.
         * @return score::Result<bool, ErrorCode> 
         *         - `true` if the key has a default value.
         *         - `false` if the key does not have a default value.
         *         - An `ErrorCode` if an error occurs during the operation.
         */
        score::Result<bool> is_value_default(const std::string_view key);



        /**
         * @brief Stores a key-value pair in the key-value store.
         * 
         * @param key The key associated with the value to be stored. 
         *            It is represented as a string view to avoid unnecessary copying.
         * @param value The value to be stored, represented as a KvsValue object.
         * 
         * @return score::Result<bool, ErrorCode> 
         *         - On success: Returns a score::Result containing `true` if the value was successfully set.
         *         - On failure: Returns a score::Result containing an appropriate ErrorCode.
         */
        score::Result<bool> set_value(const std::string_view key, const KvsValue& value);


        /**
         * @brief Removes a key-value pair from the store based on the specified key.
         * 
         * @param key The key of the key-value pair to be removed. It is passed as a 
         *            std::string_view to avoid unnecessary copying.
         * @return score::Result<void, ErrorCode> Returns a score::Result object. If the operation 
         *         is successful, it contains no value (void). If an error occurs, 
         *         it contains an appropriate ErrorCode.
         */
        score::ResultBlank remove_key(const std::string_view key);


        /**
         * @brief Flushes the key-value store, ensuring that all pending changes 
         *        are written to the underlying storage.
         * 
         * @return A score::Result object that indicates the success or failure of the operation.
         *         - On success: Returns a void score::Result.
         *         - On failure: Returns an ErrorCode describing the error.
         */
        score::ResultBlank flush();


        /**
         * @brief Retrieves the number of snapshots currently stored in the key-value store.
         * 
         * @return The total count of snapshots as a size_t value.
         */
        size_t snapshot_count() const;


        /**
         * @brief Retrieves the maximum number of snapshots that can be stored.
         * 
         * This function returns the upper limit on the number of snapshots
         * that the key-value store can maintain at any given time.
         * 
         * @return The maximum count of snapshots as a size_t value.
         */
        size_t max_snapshot_count() const;


        /**
         * @brief Restores the state of the key-value store from a specified snapshot.
         * 
         * This function attempts to restore the key-value store to the state
         * captured in the snapshot identified by the given snapshot ID. If the
         * restoration process fails, an appropriate error code is returned.
         * 
         * @param snapshot_id The identifier of the snapshot to restore from.
         * @return score::Result<void, ErrorCode> 
         *         - On success: An empty score::Result indicating the restoration was successful.
         *         - On failure: An error code describing the reason for the failure.
         */
        score::ResultBlank snapshot_restore(const SnapshotId& snapshot_id);


        /**
         * @brief Retrieves the filename associated with a given snapshot ID in the key-value store.
         * 
         * @param snapshot_id The identifier of the snapshot for which the filename is to be retrieved.
         * @return A Filename Object corresponding to the provided snapshot ID.
         */
        score::Result<std::string> get_kvs_filename(const SnapshotId& snapshot_id) const;


        /**
         * @brief Retrieves the filename of the hash file associated with a given snapshot ID.
         * 
         * This function returns a string view representing the filename of the hash file
         * corresponding to the provided snapshot ID. The hash file is typically used to
         * store metadata or integrity information for the snapshot.
         * 
         * @param snapshot_id The identifier of the snapshot for which the hash filename is requested.
         * @return A Filename Object associated with the given snapshot ID.
         */
        score::Result<std::string> get_kvs_hash_filename(const SnapshotId& snapshot_id) const;

    private:
        /* Private constructor to prevent direct instantiation */
        Kvs() = default;

        static score::Result<std::unordered_map<std::string, KvsValue>> open_json(const std::string& filename_prefix, OpenJsonNeedFile need_file, OpenJsonVerifyHash verify_hash);

        static score::Result<KvsValue> any_to_kvsvalue(const score::json::Any& any);

        /* Internal storage and configuration details.*/
        std::mutex kvs_mutex;
        std::unordered_map<std::string, KvsValue> kvs;
    
        /* Optional default values */
        std::unordered_map<std::string, KvsValue> default_values;
    
        /* Filename prefix */
        std::string filename_prefix;
    
        /* Flush on exit flag */
        std::atomic<bool> flush_on_exit;
   
};


/**
 * @class KvsBuilder
 * @brief Builder for opening a KVS object.
 */
class KvsBuilder {
public:
    /**
     * @brief Constructs a KvsBuilder for the given KVS instance.
     * @param instance_id Unique identifier for the KVS instance.
     */
    explicit KvsBuilder(const InstanceId& instance_id);

    /**
     * @brief Specify whether default values must be loaded.
     * @param flag True to require default values; false to make them optional.
     * @return Reference to this builder (for chaining).
     */
    KvsBuilder& need_defaults_flag(bool flag);

    /**
     * @brief Configure if KVS must exist when opening the KVS.
     * @param flag True to require an existing store; false to allow starting empty.
     * @return Reference to this builder (for chaining).
     */
    KvsBuilder& need_kvs_flag(bool flag);

    /**
     * @brief Set the key-value-storage permanent storage directory.
     * @param d Filesystem path to use for snapshots and data.
     * @return Reference to this builder (for chaining).
     */
    KvsBuilder& directory(const std::string& d);

    /**
     * @brief Builds and opens the Kvs instance with the configured options.
     *
     * Internally calls Kvs::open() with the selected flags and directory.
     *
     * @return A score::Result<Kvs> containing the opened store or an ErrorCode.
     */
    score::Result<Kvs> build() const;

private:
    InstanceId                         instance_id;   ///< ID of the KVS instance
    bool                               need_defaults; ///< Whether default values are required
    bool                               need_kvs;      ///< Whether an existing KVS is required
    std::optional<std::string>         dir;           ///< Optional custom directory path
};
