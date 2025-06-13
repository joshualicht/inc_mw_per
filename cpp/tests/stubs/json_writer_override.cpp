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

 /* This file is a stub implementation of the JsonWriter class for testing purposes.
  *   It overrides the ToBuffer method to return a stubbed JSON string or an error based on a global flag. */
 #include "score/json/json_writer.h"

namespace score {
namespace json {

bool g_JsonWriterShouldFail = false; /* Global flag to control failure of ToBuffer */
std::string g_JsonWriterReturnValue = "{\"stubbed\": true}"; /* Global return value for ToBuffer */

score::Result<std::string> JsonWriter::ToBuffer(const score::json::Object& json_data) {
    if (g_JsonWriterShouldFail) {
        return score::MakeUnexpected(Error::kUnknownError);
    }
    return g_JsonWriterReturnValue;
}

}  // namespace json
}  // namespace score
