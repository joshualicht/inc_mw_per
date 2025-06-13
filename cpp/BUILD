cc_library(
    name = "kvs_cpp",
    srcs = ["src/kvs.cpp",
            "inc/internal/kvs_helper.h" # only for unittest usage
    ],
    hdrs = ["inc/kvs.hpp"],
    includes = ["inc"],
    deps = [
        "@score-baselibs//score/filesystem:filesystem",
        "@score-baselibs//score/json:interface",
        "@score-baselibs//score/json:json_parser",
        "@score-baselibs//score/result:result",
    ],

    visibility = ["//visibility:public"]
)

cc_test(
    name = "test-kvs",
    srcs = [
        "tests/stubs/json_writer_override.cpp",
        "tests/test-kvs.cpp",
        "src/kvs.cpp",
        "inc/internal/kvs_helper.h",
        "inc/kvs.hpp"
    ],
    includes = [
        "inc",
        "tests/stubs",
    ],
    deps = [
        "@googletest//:gtest_main",
        "@score-baselibs//score/filesystem:filesystem",
        "@score-baselibs//score/json:interface",
        "@score-baselibs//score/json:json_parser",
        "@score-baselibs//score/result:result",
    ],
)
