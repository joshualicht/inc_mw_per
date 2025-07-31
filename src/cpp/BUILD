# *******************************************************************************
# Copyright (c) 2025 Contributors to the Eclipse Foundation
#
# See the NOTICE file(s) distributed with this work for additional
# information regarding copyright ownership.
#
# This program and the accompanying materials are made available under the
# terms of the Apache License Version 2.0 which is available at
# https://www.apache.org/licenses/LICENSE-2.0
#
# SPDX-License-Identifier: Apache-2.0
# *******************************************************************************

# The filegroup is used to collect all source files for the tests.

cc_library(
    name = "kvsvalue",
    srcs = [
        "src/kvsvalue.cpp",
    ],
    hdrs = ["src/kvsvalue.hpp"],
    includes = ["src"],
    visibility = [
        "//:__pkg__",
        "//src/cpp/src/internal:__pkg__",
    ],
)

cc_library(
    name = "kvs_cpp",
    srcs = [
        "src/kvs.cpp",
        "src/kvsbuilder.cpp",
    ],
    hdrs = [
        "src/kvsbuilder.hpp",
        "src/kvs.hpp",
    ],
    implementation_deps = [
        "//src/cpp/src/internal:kvs_helper",
    ],
    includes = ["src"],
    visibility = ["//:__pkg__"],
    deps = [
        "//src/cpp/src/internal:ikvs",
    ],
)

cc_library(
    name = "mock",
    srcs = [
        "src/kvsmock.cpp",
    ],
    hdrs = [
        "src/kvsmock.hpp",
    ],
    includes = ["src"],
    visibility = ["//:__pkg__"],
    deps = [
        ":kvs_cpp",
        "//src/cpp/src/internal:ikvs",
        "@googletest//:gtest",
    ],
)
