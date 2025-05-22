#include "kvs.hpp"
#include <iostream>
#include <algorithm>
#include <fstream>
#include <sstream>

using namespace std;


/*********************** Helper Functions *********************/
/*Adler 32 checksum algorithm*/ 
static uint32_t adler32(const string& data) {
    const uint32_t mod = 65521;
    uint32_t a = 1, b = 0;
    for (unsigned char c : data) {
        a = (a + c) % mod;
        b = (b + a) % mod;
    }
    return (b << 16) | a;
}


/*********************** Error Implementation *********************/
std::string_view MyErrorDomain::MessageFor(score::result::ErrorCode const& code) const noexcept
    {
        switch (static_cast<MyErrorCode>(code))
        {
            case MyErrorCode::UnmappedError:
                return "Error that was not yet mapped";
            case MyErrorCode::FileNotFound:
                return "File not found";
            case MyErrorCode::KvsFileReadError:
                return "KVS file read error";
            case MyErrorCode::KvsHashFileReadError:
                return "KVS hash file read error";
            case MyErrorCode::JsonParserError:
                return "JSON parser error";
            case MyErrorCode::JsonGeneratorError:
                return "JSON generator error";
            case MyErrorCode::PhysicalStorageFailure:
                return "Physical storage failure";
            case MyErrorCode::IntegrityCorrupted:
                return "Integrity corrupted";
            case MyErrorCode::ValidationFailed:
                return "Validation failed";
            case MyErrorCode::EncryptionFailed:
                return "Encryption failed";
            case MyErrorCode::ResourceBusy:
                return "Resource is busy";
            case MyErrorCode::OutOfStorageSpace:
                return "Out of storage space";
            case MyErrorCode::QuotaExceeded:
                return "Quota exceeded";
            case MyErrorCode::AuthenticationFailed:
                return "Authentication failed";
            case MyErrorCode::KeyNotFound:
                return "Key not found";
            case MyErrorCode::SerializationFailed:
                return "Serialization failed";
            case MyErrorCode::InvalidSnapshotId:
                return "Invalid snapshot ID";
            case MyErrorCode::ConversionFailed:
                return "Conversion failed";
            case MyErrorCode::MutexLockFailed:
                return "Mutex failed";
            default:
                return "Unknown Error!";
        }
    }

score::result::Error MakeError(MyErrorCode code, std::string_view user_message) noexcept
{
    return {static_cast<score::result::ErrorCode>(code), my_error_domain, user_message};
}


/*********************** KVS Builder Implementation *********************/
KvsBuilder::KvsBuilder(const InstanceId& instance_id)
    : instance_id(instance_id)
    , need_defaults(false)
    , need_kvs(false)
    , dir(std::nullopt)
{}

KvsBuilder& KvsBuilder::need_defaults_flag(bool flag) {
    need_defaults = flag;
    return *this;
}

KvsBuilder& KvsBuilder::need_kvs_flag(bool flag) {
    need_kvs = flag;
    return *this;
}

KvsBuilder& KvsBuilder::directory(const std::string& d) {
    dir = d;
    return *this;
}

score::Result<Kvs> KvsBuilder::build() const {
    return Kvs::open(
        instance_id,
        need_defaults ? OpenNeedDefaults::Required : OpenNeedDefaults::Optional,
        need_kvs      ? OpenNeedKvs::Required      : OpenNeedKvs::Optional,
        dir
    );
}


/*********************** KVS Implementation *********************/
Kvs::~Kvs() = default;

Kvs::Kvs(Kvs&& other) noexcept
    : filename_prefix(std::move(other.filename_prefix))
    , flush_on_exit(other.flush_on_exit.load(std::memory_order_relaxed))
{
    {
        std::lock_guard<std::mutex> lock(other.kvs_mutex);
        kvs = std::move(other.kvs);
        default_values = std::move(other.default_values);
    }
}

Kvs& Kvs::operator=(Kvs&& other) noexcept
{
    if (this != &other) {
        {
            std::lock_guard<std::mutex> lock_this(kvs_mutex);
            kvs.clear();
            default_values.clear();
        }

        filename_prefix = std::move(other.filename_prefix);
        flush_on_exit.store(other.flush_on_exit.load(std::memory_order_relaxed),
                             std::memory_order_relaxed);

        {
            std::lock_guard<std::mutex> lock_other(other.kvs_mutex);
            std::lock_guard<std::mutex> lock_this(kvs_mutex);
            kvs = std::move(other.kvs);
            default_values = std::move(other.default_values);
        }
    }
    return *this;
}

score::Result<Kvs> Kvs::open(
    const InstanceId&      instance_id,
    OpenNeedDefaults       need_defaults,
    OpenNeedKvs            need_kvs,
    const optional<string>& dir)
{
    const string  d               = dir.has_value() ? dir.value() + "/" : string();
    const string  filename_default = d + "kvs_" + to_string(instance_id.id) + "_default";
    const string  filename_prefix  = d + "kvs_" + to_string(instance_id.id);
    const string  filename_kvs     = filename_prefix + "_0";

    score::Result<Kvs> result = score::MakeUnexpected(MyErrorCode::UnmappedError); /* Redundant initialization needed, since Resul<KVS> would call the implicitly-deleted default constructor of KVS */
    Kvs store;
    
    auto default_res = open_json(
        filename_default,
        need_defaults == OpenNeedDefaults::Required ? OpenJsonNeedFile::Required : OpenJsonNeedFile::Optional,
        OpenJsonVerifyHash::No);
    if (!default_res){
        result = score::MakeUnexpected(static_cast<MyErrorCode>(*default_res.error())); /* Dereferences the Error class to its underlying code -> error.h*/
    }
    else{
        auto kvs_res = open_json(
            filename_kvs,
            need_kvs == OpenNeedKvs::Required ? OpenJsonNeedFile::Required : OpenJsonNeedFile::Optional,
            OpenJsonVerifyHash::Yes);
        if (!kvs_res){
            result = score::MakeUnexpected(static_cast<MyErrorCode>(*kvs_res.error())); /* Dereferences the Error class to its underlying code -> error.h*/
        }else{ 
            cout << "opened KVS: instance '" << instance_id.id << "'" << endl;
            cout << "max snapshot count: " << KVS_MAX_SNAPSHOTS << endl;
            
            store.kvs = std::move(kvs_res.value());
            store.default_values = std::move(default_res.value());
            store.filename_prefix = filename_prefix;
            store.flush_on_exit.store(true, std::memory_order_relaxed);
            result = std::move(store);
        }
    }

    return result;
}

score::Result<unordered_map<string, KvsValue>> Kvs::open_json(
    const string& prefix,
    OpenJsonNeedFile need_file,
    OpenJsonVerifyHash verify_hash)
{
    string json_file = prefix + ".json";
    string hash_file = prefix + ".hash";
    string data;
    bool error = false; /* Error flag */
    score::Result<unordered_map<string, KvsValue>> result = score::MakeUnexpected(MyErrorCode::UnmappedError);
    
    /* Read JSON file */
    ifstream in(json_file);
    if (!in) {
        if (need_file == OpenJsonNeedFile::Required) {
            cerr << "error: file " << json_file << " could not be read" << endl;
            error = true;
            result = score::MakeUnexpected(MyErrorCode::KvsFileReadError);
        }else{
            cout << "file " << json_file << " not found, using empty data" << endl;
            result = score::Result<unordered_map<string,KvsValue>>({});
        }
    }else{
        ostringstream ss;
        ss << in.rdbuf();
        data = ss.str();
    }

    /* Verify JSON Hash */
    if(!error){
        if (verify_hash == OpenJsonVerifyHash::Yes) {
            ifstream hin(hash_file, ios::binary);
            if (!hin) {
                cerr << "error: hash file " << hash_file << " could not be read" << endl;
                error = true;
                result = score::MakeUnexpected(MyErrorCode::KvsHashFileReadError);
            }else{
                uint8_t buf[4];
                hin.read(reinterpret_cast<char*>(buf), 4);
                uint32_t file_hash = (uint32_t(buf[0]) << 24) | (uint32_t(buf[1]) << 16)
                                | (uint32_t(buf[2]) << 8)  | uint32_t(buf[3]);
                uint32_t calc_hash = adler32(data);
                if (file_hash != calc_hash) {
                    cerr << "error: KVS data corrupted (" << json_file << ", " << hash_file << ")" << endl;
                    error = true;
                    result = score::MakeUnexpected(MyErrorCode::ValidationFailed);
                }else{
                    cout << "JSON data has valid hash" << endl;
                }
            }
        }
    }
    

    /* Parse JSON Data */
    if(!error){
        score::json::JsonParser parser;
        std::unordered_map<std::string, KvsValue> result_value;

        auto any_res = parser.FromBuffer(data);
        if (!any_res) {
            error = true;
            result = score::MakeUnexpected(MyErrorCode::JsonParserError);
        }
        else{
            score::json::Any root = std::move(any_res).value();

            /* Convert JSON to map<string,KvsValue> */
            if (auto obj = root.As<score::json::Object>(); obj.has_value()) {
                for (const auto& element : obj.value().get()) {
                    
                    auto sv = element.first.GetAsStringView(); /* Get Key*/
                    std::string key(sv.data(), sv.size());
                 
                    auto conv = any_to_kvsvalue(element.second); /* Value Conversion*/
                    if (!conv) {
                        error = true; /* Conversion Error*/
                        result = score::MakeUnexpected(static_cast<MyErrorCode>(*conv.error()));
                        break;
                    }else{
                        result_value.emplace(std::move(key), std::move(conv.value()));
                    }
                }
            } else {
                std::cout << "warning: JSON root is not an object, using empty map\n";
            }
        }
        if(!error){
            result = std::move(result_value);
        }
    }

    return result;
}

score::Result<KvsValue> Kvs::any_to_kvsvalue(const score::json::Any& any){

    score::Result<KvsValue> result = score::MakeUnexpected(MyErrorCode::ConversionFailed);

    if (any.As<score::json::Null>().has_value()) {
        result = KvsValue(nullptr);
    }
    else if (auto b = any.As<bool>(); b.has_value()) {
        result = KvsValue(b.value());
    }
    else if (auto n = any.As<double>(); n.has_value()) {
        result = KvsValue(n.value());
    }
    else if (auto s = any.As<std::string>(); s.has_value()) {
        result = KvsValue(s.value().get());
    }
    else if (auto l = any.As<score::json::List>(); l.has_value()) {
        KvsValue::Array arr;
        bool error = false;
        for (auto const& elem : l.value().get()) {
            auto conv = any_to_kvsvalue(elem);
            if (!conv) {
                error = true;
                break;
            }
            arr.emplace_back(std::move(conv.value()));
        }
        if (!error) {
            result = KvsValue(std::move(arr));
        }
    }
    else if (auto o = any.As<score::json::Object>(); o.has_value()) {
        KvsValue::Object obj;
        bool error = false;
        for (auto const& element : o.value().get()) {
            auto conv = any_to_kvsvalue(element.second);
            if (!conv) {
                error = true;
                break;
            }
            obj.emplace(
                element.first.GetAsStringView().to_string(),
                std::move(conv.value())
            );
        }
        if (!error) {
            result = KvsValue(std::move(obj));
        }
    }

    return result;
}

/* Set flush on exit flag*/
void Kvs::set_flush_on_exit(bool flush) {
    flush_on_exit.store(flush, std::memory_order_relaxed);
    return;
}

/* Reset KVS to initial state*/
score::ResultBlank Kvs::reset() {

    return score::MakeUnexpected(MyErrorCode::UnmappedError);
}

/* Retrieve all keys in the KVS*/
score::Result<std::vector<std::string>> Kvs::get_all_keys() {

    return score::MakeUnexpected(MyErrorCode::UnmappedError);
}

/* Check if a key exists*/
score::Result<bool> Kvs::key_exists(const string_view key) {

    return score::MakeUnexpected(MyErrorCode::UnmappedError);
}

/* Retrieve the value associated with a key*/
score::Result<KvsValue> Kvs::get_value(const std::string_view key) {

    return score::MakeUnexpected(MyErrorCode::UnmappedError);
}

/*Retrieve the default value associated with a key*/
score::Result<KvsValue> Kvs::get_default_value(const std::string_view key) {

    return score::MakeUnexpected(MyErrorCode::UnmappedError);
}

/* Check if a key has a default value*/
score::Result<bool> Kvs::is_value_default(const std::string_view key) {

    return score::MakeUnexpected(MyErrorCode::UnmappedError);
}

/* Set the value for a key*/
score::Result<bool> Kvs::set_value(const std::string_view key, const KvsValue& value) {

    return score::MakeUnexpected(MyErrorCode::UnmappedError);
}

/* Remove a key-value pair*/
score::ResultBlank Kvs::remove_key(const string_view key) {
    
    return score::MakeUnexpected(MyErrorCode::UnmappedError);
}

/* Flush the key-value store*/
score::ResultBlank Kvs::flush() {
    
    return score::MakeUnexpected(MyErrorCode::UnmappedError);
}

/* Retrieve the snapshot count*/
size_t Kvs::snapshot_count() const {

    return 0;
}

/* Retrieve the max snapshot count*/
size_t Kvs::max_snapshot_count() const {

    return 0;
}

/* Restore the key-value store from a snapshot*/
score::ResultBlank Kvs::snapshot_restore(const SnapshotId& snapshot_id) {

    return score::MakeUnexpected(MyErrorCode::UnmappedError);
}

/* Get the filename for a snapshot*/
score::Result<std::string> Kvs::get_kvs_filename(const SnapshotId& snapshot_id) const {

    return score::MakeUnexpected(MyErrorCode::UnmappedError);
}

/* Get the hash filename for a snapshot*/
score::Result<std::string> Kvs::get_kvs_hash_filename(const SnapshotId& snapshot_id) const {

    return score::MakeUnexpected(MyErrorCode::UnmappedError);
}
