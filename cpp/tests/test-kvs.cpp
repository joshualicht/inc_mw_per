#include <gtest/gtest.h>
#include <cstdlib>
#include <cstdio>
#include <sys/stat.h>
#include <unistd.h>
#include <fstream>
#include <string>
#include "kvs.hpp"
#include "internal/kvs_helper.h"
#include "score/filesystem/filesystem.h"

/* Test Environment Setup - Standard Variables*/
const std::uint32_t instance = 123;
const InstanceId instance_id{instance};
const std::string process_name = "my_process";
const std::string base_dir = "./data_folder";
const std::string data_dir = base_dir + "/" + process_name;
const std::string default_prefix = data_dir + "/kvs_"+std::to_string(instance)+"_default";
const std::string kvs_prefix     = data_dir + "/kvs_"+std::to_string(instance)+"_0";
const std::string default_json = R"({ "default": 1 })";
const std::string kvs_json     = R"({ "kvs": 2 })";

uint32_t adler32(const std::string& data) {
    const uint32_t mod = 65521;
    uint32_t a = 1, b = 0;
    for (unsigned char c : data) {
        a = (a + c) % mod;
        b = (b + a) % mod;
    }
    return (b << 16) | a;
}

bool file_exists(const std::string& path) {
    struct stat buffer;
    return stat(path.c_str(), &buffer) == 0;
}

void cleanup_environment() {
    /* Cleanup the test environment */
    system(("rm -rf " + base_dir).c_str());
}

void prepare_enviromnnt(){
    /* Prepare the test environment */
    mkdir(base_dir.c_str(), 0777);
    mkdir(data_dir.c_str(), 0777);

    std::ofstream default_json_file(default_prefix + ".json");
    default_json_file << default_json;
    default_json_file.close();

    std::ofstream kvs_json_file(kvs_prefix + ".json");
    kvs_json_file << kvs_json;
    kvs_json_file.close();

    uint32_t default_hash = calculate_hash_adler32(default_json);
    uint32_t kvs_hash = calculate_hash_adler32(kvs_json);

    std::ofstream default_hash_file(default_prefix + ".hash", std::ios::binary);
    default_hash_file.put((default_hash >> 24) & 0xFF);
    default_hash_file.put((default_hash >> 16) & 0xFF);
    default_hash_file.put((default_hash >> 8)  & 0xFF);
    default_hash_file.put(default_hash & 0xFF);
    default_hash_file.close();

    std::ofstream kvs_hash_file(kvs_prefix + ".hash", std::ios::binary);
    kvs_hash_file.put((kvs_hash >> 24) & 0xFF);
    kvs_hash_file.put((kvs_hash >> 16) & 0xFF);
    kvs_hash_file.put((kvs_hash >> 8)  & 0xFF);
    kvs_hash_file.put(kvs_hash & 0xFF);
    kvs_hash_file.close();
}

TEST(kvs_TEST, checksum_adler32) {
    /* Test the adler32 hash calculation 
    Parsing test is done automatically by the open tests */

    std::string test_data = "Hello, World!";
    uint32_t calculated_hash = adler32(test_data);
    EXPECT_EQ(calculated_hash, calculate_hash_adler32(test_data));

    std::array<uint8_t,4> buf{};
    std::istringstream stream(test_data);
    stream.read(reinterpret_cast<char*>(buf.data()), buf.size());

    std::array<uint8_t, 4> value = {
        uint8_t((calculated_hash >> 24) & 0xFF),
        uint8_t((calculated_hash >> 16) & 0xFF),
        uint8_t((calculated_hash >>  8) & 0xFF),
        uint8_t((calculated_hash      ) & 0xFF)
    };
    EXPECT_EQ(value, get_hash_bytes(test_data));

}

TEST(kvs_TEST, kvsbuilder) {
    cleanup_environment(); /* Make sure environment is clean before testing */
    /* Test the KvsBuilder constructor */
    {
    KvsBuilder builder(std::string("kvsbuilder"), InstanceId(5));
    auto result = builder.build();
    ASSERT_TRUE(result);
    }
    /* Check Instance ID and process_name through Filename*/
    EXPECT_TRUE(file_exists(base_dir + "/kvsbuilder/kvs_5_0.json"));

    /* Check Required Flags (Optional is default) -> In this testcase are no kvs files available*/
    {
        KvsBuilder builder(std::string(process_name), instance_id);
        builder.need_defaults_flag(true);
        auto result = builder.build();
        ASSERT_FALSE(result);
        EXPECT_EQ(static_cast<MyErrorCode>(*result.error()), MyErrorCode::KvsFileReadError);
    }

    {
        KvsBuilder builder(std::string(process_name), instance_id);
        builder.need_kvs_flag(true);
        auto result = builder.build();
        ASSERT_FALSE(result);
        EXPECT_EQ(static_cast<MyErrorCode>(*result.error()), MyErrorCode::KvsFileReadError);
    }
    
    cleanup_environment();
}

TEST(kvs_TEST, constructor_move_assignment_operator) {
    cleanup_environment(); /* Make sure environment is clean before testing */
    const std::uint32_t instance_b = 5;
    {
        /* create object A with initial data */
        auto result_a = Kvs::open(std::string(process_name), instance_id, OpenNeedDefaults::Optional, OpenNeedKvs::Optional);
        ASSERT_TRUE(result_a);
        Kvs kvs_a = std::move(result_a.value());
        kvs_a.set_flush_on_exit(true);

        /* create object B with different data */
        auto result_b = Kvs::open(std::string(process_name), InstanceId(instance_b), OpenNeedDefaults::Optional, OpenNeedKvs::Optional);
        ASSERT_TRUE(result_b);
        Kvs kvs_b = std::move(result_b.value());
        kvs_b.set_flush_on_exit(false);

        /* Move assignment operator */
        kvs_a = std::move(kvs_b);

        /* Ignore Compiler Warning "Wself-move" for GCC and CLANG*/
        #if defined(__clang__) 
        #pragma clang diagnostic push
        #pragma clang diagnostic ignored "-Wself-move"
        #elif defined(__GNUC__)
        #pragma GCC diagnostic push
        #pragma GCC diagnostic ignored "-Wself-move"
        #endif
        kvs_a = std::move(kvs_a);  /* Intentional self-move */
        #if defined(__clang__)
        #pragma clang diagnostic pop
        #elif defined(__GNUC__)
        #pragma GCC diagnostic pop
        #endif

        kvs_a.flush();
    }
    /*Expectations:
    - kvs_a now contains data from the former kvs_b
    - kvs_b should overwrite kvs_a's data -> no data from kvs_a should be flushed
    - flush_on_exit of kvs_a should be false (was copied) -> only one snapshot through mnaual flush expected */

    EXPECT_TRUE(file_exists(data_dir + "/kvs_"+std::to_string(instance_b)+"_0" + ".json"));
    EXPECT_FALSE(file_exists(data_dir + "/kvs_"+std::to_string(instance_b)+"_1" + ".json"));
    EXPECT_FALSE(file_exists(data_dir + "/kvs_"+std::to_string(instance)+"_0" + ".json"));

    cleanup_environment();
}

TEST(kvs_TEST, open_normal) {
  
    prepare_enviromnnt();
    
    /* --------------------Testcases-------------------- 
    -> open with open_json with valid JSON, Default and Hash files */

    /* OpenNeedDefaults::Required, OpenNeedKvs::Required, JSON existing, Valid Hash existing, Default JSON existing, Valid Default Hash */
    auto result = Kvs::open(std::string(process_name), instance_id, OpenNeedDefaults::Required, OpenNeedKvs::Required);
    EXPECT_TRUE(result);
    result.value().set_flush_on_exit(false);
    
    /* OpenNeedDefaults::Required, OpenNeedKvs::Optional, JSON existing, Valid Hash existing, Default JSON existing, Valid Default Hash */
    result = Kvs::open(std::string(process_name), instance_id, OpenNeedDefaults::Required, OpenNeedKvs::Optional);
    EXPECT_TRUE(result);
    result.value().set_flush_on_exit(false);

    /* OpenNeedDefaults::Optional, OpenNeedKvs::Required, JSON existing, Valid Hash existing, Default JSON existing, Valid Default Hash */
    result = Kvs::open(std::string(process_name), instance_id, OpenNeedDefaults::Optional, OpenNeedKvs::Required);
    EXPECT_TRUE(result);
    result.value().set_flush_on_exit(false);

    /* OpenNeedDefaults::Optional, OpenNeedKvs::Optional, JSON existing, Valid Hash existing, Default JSON existing, Valid Default Hash */
    result = Kvs::open(std::string(process_name), instance_id, OpenNeedDefaults::Optional, OpenNeedKvs::Optional);
    EXPECT_TRUE(result);
    result.value().set_flush_on_exit(false);

    cleanup_environment();
}

TEST(kvs_TEST, open_default_hash_corrupted) {
  
    prepare_enviromnnt();
    
    /* --------------------Testcases-------------------- 
    -> open with open_json with valid JSON, Default but Default Hash corrupted or missing */

    /* OpenNeedDefaults::Required, OpenNeedKvs::Required, JSON existing, Valid Hash existing, Default JSON existing, Default Hash corrupted */
    std::fstream corrupt_default_hash_file(default_prefix + ".hash", std::ios::in | std::ios::out | std::ios::binary);
    corrupt_default_hash_file.seekp(0);
    corrupt_default_hash_file.put(0xFF);
    corrupt_default_hash_file.close();

    auto result = Kvs::open(std::string(process_name), instance_id, OpenNeedDefaults::Required, OpenNeedKvs::Required);
    ASSERT_FALSE(result);
    EXPECT_EQ(static_cast<MyErrorCode>(*result.error()), MyErrorCode::ValidationFailed);

    /* OpenNeedDefaults::Required, OpenNeedKvs::Required, JSON existing, Valid Hash existing, Default JSON existing, Default Hash not existing*/
    system(("rm -rf " + default_prefix + ".hash").c_str());

    result = Kvs::open(std::string(process_name), instance_id, OpenNeedDefaults::Required, OpenNeedKvs::Required);
    ASSERT_FALSE(result);
    EXPECT_EQ(static_cast<MyErrorCode>(*result.error()), MyErrorCode::KvsHashFileReadError);

    /* OpenNeedDefaults::Optional, OpenNeedKvs::Required, JSON existing, Valid Hash existing, Default JSON existing, Default Hash not existing*/
    result = Kvs::open(std::string(process_name), instance_id, OpenNeedDefaults::Optional, OpenNeedKvs::Required);
    ASSERT_FALSE(result);
    EXPECT_EQ(static_cast<MyErrorCode>(*result.error()), MyErrorCode::KvsHashFileReadError);

    cleanup_environment();
}

TEST(kvs_TEST, open_default_corrupted) {
  
    prepare_enviromnnt();
    system(("rm -rf " + default_prefix + ".hash").c_str());
    system(("rm -rf " + default_prefix + ".json").c_str());
    
    /* --------------------Testcases-------------------- 
    -> open with open_json with valid JSON but Default KVS file (and default hash) is missing */

    /* OpenNeedDefaults::Required, OpenNeedKvs::Required, JSON existing, Valid Hash existing, Default JSON not existing, Default Hash not existing */
    auto result = Kvs::open(std::string(process_name), instance_id, OpenNeedDefaults::Required, OpenNeedKvs::Required);
    ASSERT_FALSE(result);
    EXPECT_EQ(static_cast<MyErrorCode>(*result.error()), MyErrorCode::KvsFileReadError);

    /* OpenNeedDefaults::Required, OpenNeedKvs::Optional, JSON existing, Valid Hash existing, Default JSON not existing, Default Hash not existing */
    result = Kvs::open(std::string(process_name), instance_id, OpenNeedDefaults::Required, OpenNeedKvs::Optional);
    ASSERT_FALSE(result);
    EXPECT_EQ(static_cast<MyErrorCode>(*result.error()), MyErrorCode::KvsFileReadError);

    /* OpenNeedDefaults::Optional, OpenNeedKvs::Required, JSON existing, Valid Hash existing, Default JSON not existing, Default Hash not existing */
    result = Kvs::open(std::string(process_name), instance_id, OpenNeedDefaults::Optional, OpenNeedKvs::Required);
    EXPECT_TRUE(result);
    result.value().set_flush_on_exit(false);

    cleanup_environment();
}

TEST(kvs_TEST, open_kvs_corrupted) {
  
    prepare_enviromnnt();
    system(("rm -rf " + default_prefix + ".hash").c_str());
    system(("rm -rf " + default_prefix + ".json").c_str());
    
    /* --------------------Testcases-------------------- 
    -> open with open_json with corrupted or missing KVS JSON Hash */

    /* OpenNeedDefaults::Optional, OpenNeedKvs::Required, JSON existing, Hash corrupted, Default JSON not existing, Default Hash not existing */
    std::fstream corrupt_hash_file(kvs_prefix + ".hash", std::ios::in | std::ios::out | std::ios::binary);
    corrupt_hash_file.seekp(0);
    corrupt_hash_file.put(0xFF);
    corrupt_hash_file.close();
    
    auto result = Kvs::open(std::string(process_name), instance_id, OpenNeedDefaults::Optional, OpenNeedKvs::Required);
    ASSERT_FALSE(result);
    EXPECT_EQ(static_cast<MyErrorCode>(*result.error()), MyErrorCode::ValidationFailed);

    /* OpenNeedDefaults::Optional, OpenNeedKvs::Optional, JSON existing, Hash corrupted, Default JSON not existing, Default Hash not existing */
    result = Kvs::open(std::string(process_name), instance_id, OpenNeedDefaults::Optional, OpenNeedKvs::Optional);
    ASSERT_FALSE(result);
    EXPECT_EQ(static_cast<MyErrorCode>(*result.error()), MyErrorCode::ValidationFailed);

    /* OpenNeedDefaults::Optional, OpenNeedKvs::Required, JSON existing, Hash not existing, Default JSON not existing, Default Hash not existing */
    system(("rm -rf " + kvs_prefix + ".hash").c_str());
    
    result = Kvs::open(std::string(process_name), instance_id, OpenNeedDefaults::Optional, OpenNeedKvs::Required);
    ASSERT_FALSE(result);
    EXPECT_EQ(static_cast<MyErrorCode>(*result.error()), MyErrorCode::KvsHashFileReadError);

    /* OpenNeedDefaults::Optional, OpenNeedKvs::Optional, JSON existing, Hash not existing, Default JSON not existing, Default Hash not existing */
    result = Kvs::open(std::string(process_name), instance_id, OpenNeedDefaults::Optional, OpenNeedKvs::Optional);
    ASSERT_FALSE(result);
    EXPECT_EQ(static_cast<MyErrorCode>(*result.error()), MyErrorCode::KvsHashFileReadError);


    cleanup_environment();
}

TEST(kvs_TEST, open_kvs_missing) {
  
    prepare_enviromnnt();
    system(("rm -rf " + default_prefix + ".hash").c_str());
    system(("rm -rf " + default_prefix + ".json").c_str());
    system(("rm -rf " + kvs_prefix + ".hash").c_str());
    system(("rm -rf " + kvs_prefix + ".json").c_str());
    
    /* --------------------Testcases-------------------- 
    -> open with open_json with missing KVS JSON */

    /* OpenNeedDefaults::Optional, OpenNeedKvs::Required, JSON not existing, Valid Hash existing, Default JSON not existing, Default Hash not existing */
    auto result = Kvs::open(std::string(process_name), instance_id, OpenNeedDefaults::Optional, OpenNeedKvs::Required);
    ASSERT_FALSE(result);
    EXPECT_EQ(static_cast<MyErrorCode>(*result.error()), MyErrorCode::KvsFileReadError);

    /* OpenNeedDefaults::Optional, OpenNeedKvs::Optional, JSON not existing, Valid Hash existing, Default JSON not existing, Default Hash not existing */
    result = Kvs::open(std::string(process_name), instance_id, OpenNeedDefaults::Optional, OpenNeedKvs::Optional);
    EXPECT_TRUE(result);
    result.value().set_flush_on_exit(false);

    cleanup_environment();
}

TEST(kvs_TEST, flush_on_exit) {
    prepare_enviromnnt();
    {
    auto result = Kvs::open(std::string(process_name), instance_id, OpenNeedDefaults::Required, OpenNeedKvs::Required);
    ASSERT_TRUE(result);
    result.value().set_flush_on_exit(false);
    }
    /* check if Snapshot 1 exists after destructor*/
    ASSERT_FALSE(file_exists((data_dir + "/kvs_"+std::to_string(instance)+"_1" + ".json").c_str()));

    {
    auto result = Kvs::open(std::string(process_name), instance_id, OpenNeedDefaults::Required, OpenNeedKvs::Required);
    ASSERT_TRUE(result);
    result.value().set_flush_on_exit(true);
    }
    /* check if Snapshot 1 exists after destructor*/
    ASSERT_TRUE(file_exists((data_dir + "/kvs_"+std::to_string(instance)+"_1" + ".json").c_str()));

    cleanup_environment();

}

TEST(kvs_TEST, snapshot_count){
    prepare_enviromnnt();
    for(uint32_t i = 0; i <= KVS_MAX_SNAPSHOTS; i++){
        {
        auto result = Kvs::open(std::string(process_name), instance_id, OpenNeedDefaults::Required, OpenNeedKvs::Required);
        ASSERT_TRUE(result);
        EXPECT_EQ(result.value().snapshot_count(), i);
        result.value().set_flush_on_exit(true);
        }
    }

    cleanup_environment();
}

TEST(kvs_TEST, MessageFor) {
   struct {
        MyErrorCode code;
        std::string_view expected_message;
    } test_cases[] = {
        {MyErrorCode::UnmappedError,          "Error that was not yet mapped"},
        {MyErrorCode::FileNotFound,           "File not found"},
        {MyErrorCode::KvsFileReadError,       "KVS file read error"},
        {MyErrorCode::KvsHashFileReadError,   "KVS hash file read error"},
        {MyErrorCode::JsonParserError,        "JSON parser error"},
        {MyErrorCode::JsonGeneratorError,     "JSON generator error"},
        {MyErrorCode::PhysicalStorageFailure, "Physical storage failure"},
        {MyErrorCode::IntegrityCorrupted,     "Integrity corrupted"},
        {MyErrorCode::ValidationFailed,       "Validation failed"},
        {MyErrorCode::EncryptionFailed,       "Encryption failed"},
        {MyErrorCode::ResourceBusy,           "Resource is busy"},
        {MyErrorCode::OutOfStorageSpace,      "Out of storage space"},
        {MyErrorCode::QuotaExceeded,          "Quota exceeded"},
        {MyErrorCode::AuthenticationFailed,   "Authentication failed"},
        {MyErrorCode::KeyNotFound,            "Key not found"},
        {MyErrorCode::KeyDefaultNotFound,     "Key default value not found"},
        {MyErrorCode::SerializationFailed,    "Serialization failed"},
        {MyErrorCode::InvalidSnapshotId,      "Invalid snapshot ID"},
        {MyErrorCode::ConversionFailed,       "Conversion failed"},
        {MyErrorCode::MutexLockFailed,        "Mutex failed"},
        {MyErrorCode::InvalidValueType,       "Invalid value type"}
    };
    for (const auto& test : test_cases) {
        SCOPED_TRACE(static_cast<int>(test.code));
        score::result::ErrorCode code = static_cast<score::result::ErrorCode>(test.code);
        EXPECT_EQ(my_error_domain.MessageFor(code), test.expected_message);
    }

    score::result::ErrorCode invalid_code = static_cast<score::result::ErrorCode>(9999);
    EXPECT_EQ(my_error_domain.MessageFor(invalid_code), "Unknown Error!");

}
