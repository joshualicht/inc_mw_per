cc_library(
    name = "kvs_cpp",
    srcs = [
        "inc/internal/kvs_helper.h",  # only for unittest usage
        "src/kvs.cpp",
    ],
    hdrs = ["inc/kvs.hpp"],
    includes = ["inc"],
    visibility = ["//visibility:public"],
    deps = [
        "@score-baselibs//score/filesystem:filesystem",
        "@score-baselibs//score/json:interface",
        "@score-baselibs//score/json:json_parser",
        "@score-baselibs//score/result:result",
    ],
)

cc_test(
    name = "test-kvs",
    srcs = [
        "inc/internal/kvs_helper.h",
        "inc/kvs.hpp",
        "src/kvs.cpp",
        "tests/stubs/score/json/internal/model/any.h",
        "tests/stubs/score/json/internal/model/error.h",
        "tests/stubs/score/json/json_parser.h",
        "tests/stubs/score/json/json_writer.h",
        "tests/test-kvs.cpp",
    ],
    includes = [
        "inc",
        "tests/stubs",
    ],
    deps = [
        "@googletest//:gtest_main",
        "@score-baselibs//score/filesystem:filesystem",
        "@score-baselibs//score/memory:string_comparison_adaptor",
        "@score-baselibs//score/result:result",
    ],
)
