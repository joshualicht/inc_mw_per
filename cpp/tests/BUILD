cc_test(
    name = "test-kvs",
    srcs = ["test-kvs.cpp"],
    includes = ["../inc"],
    deps = [
        "//cpp:kvs_cpp",
        "@googletest//:gtest_main",
        "@score-baselibs//score/filesystem:filesystem",
        "@score-baselibs//score/json:json_parser",
        "@score-baselibs//score/result:result",
    ],
)
