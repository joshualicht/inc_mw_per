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
        "tests/test-kvs.cpp",
        "src/kvs.cpp",
        "inc/internal/kvs_helper.h",
        "inc/kvs.hpp",
        "tests/stubs/score/json/json_writer.h",
        "tests/stubs/score/json/json_parser.h",
        "tests/stubs/score/json/internal/model/any.h",
        "tests/stubs/score/json/internal/model/error.h",

    ],
    includes = [
        "inc",
        "tests/stubs",
    ],
    deps = [
        "@googletest//:gtest_main",
        "@score-baselibs//score/memory:string_comparison_adaptor",
        "@score-baselibs//score/filesystem:filesystem",
        "@score-baselibs//score/result:result",
    ],
)
