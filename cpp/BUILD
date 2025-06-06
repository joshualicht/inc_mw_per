cc_library(
    name = "kvs_cpp",
    srcs = ["src/kvs.cpp",
            "inc/internal/kvs_helper.h" # only for unittest usage
    ],
    hdrs = ["inc/kvs.hpp"],
    includes = ["inc"],
    deps = [
        "@score-baselibs//score/json:interface",
        "@score-baselibs//score/json:json_parser",
        "@score-baselibs//score/result:result",
        "@score-baselibs//score/filesystem:filesystem",
    ],
    visibility = ["//visibility:public"]
)

