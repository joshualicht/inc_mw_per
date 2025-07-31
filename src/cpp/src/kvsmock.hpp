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
#ifndef SCORE_LIB_KVS_KVSMOCK_HPP
#define SCORE_LIB_KVS_KVSMOCK_HPP

#include <gmock/gmock.h>
#include "internal/ikvs.hpp"

namespace score 
{
namespace mw 
{
namespace pers 
{
namespace kvs 
{

class MockKvs : public IKvs {
    public:
        MockKvs() = default;
        virtual ~MockKvs() = default;

        MOCK_METHOD(void, set_flush_on_exit, (bool flush), (override));
        MOCK_METHOD(score::ResultBlank, reset, (), (override));
        MOCK_METHOD((score::Result<std::vector<std::string_view>>), get_all_keys, (), (override));
        MOCK_METHOD((score::Result<bool>), key_exists, (const std::string_view key), (override));
        MOCK_METHOD((score::Result<KvsValue>), get_value, (const std::string_view key), (override));
        MOCK_METHOD((score::Result<KvsValue>), get_default_value, (const std::string_view key), (override));
        MOCK_METHOD(score::ResultBlank, reset_key, (const std::string_view key), (override));
        MOCK_METHOD((score::Result<bool>), has_default_value, (const std::string_view key), (override));
        MOCK_METHOD(score::ResultBlank, set_value, (const std::string_view key, const KvsValue& value), (override));
        MOCK_METHOD(score::ResultBlank, remove_key, (const std::string_view key), (override));
        MOCK_METHOD(score::ResultBlank, flush, (), (override));
        MOCK_METHOD((score::Result<size_t>), snapshot_count, (), (const, override));
        MOCK_METHOD(size_t, snapshot_max_count, (), (const, override));
        MOCK_METHOD(score::ResultBlank, snapshot_restore, (const SnapshotId& snapshot_id), (override));
        MOCK_METHOD((score::Result<score::filesystem::Path>), get_kvs_filename, (const SnapshotId& snapshot_id), (const, override));
        MOCK_METHOD((score::Result<score::filesystem::Path>), get_hash_filename, (const SnapshotId& snapshot_id), (const, override));

        // Static method to simulate the KVSBuilder.build() behavior
        static score::Result<MockKvs> open(const InstanceId& instance_id, OpenNeedDefaults need_defaults, OpenNeedKvs need_kvs, const std::string&& dir)
        {
            if (kvs_builder_should_fail) {
                return score::MakeUnexpected(ErrorCode::KvsFileReadError); /* Example Error to simulate KVSBuilder.build() Failure */
            } else {
                return score::Result<MockKvs>{std::in_place}; /* Problem, if the application wants to use a move constructor on the kvs object, 
                                                                it CANNOT be tested with a gmock object*/
            }
        }

    private:
        inline static bool kvs_builder_should_fail = false;

};

} /* namespace kvs */
} /* namespace pers */
} /* namespace mw */
} /* namespace score */

#endif /* SCORE_LIB_KVS_KVSMOCK_HPP */
