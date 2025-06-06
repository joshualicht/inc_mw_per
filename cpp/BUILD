config_setting(
    name = "unit_test",
    define_values = {"UNITTEST": "1"},
)

cc_library(
    name = "kvs_cpp",
    srcs = ["src/kvs.cpp",
            "inc/internal/kvs_helper.h" # only for unittest usage
    ],
    hdrs = ["inc/kvs.hpp"],
    includes = ["inc"],
    copts = select({
        ":unit_test": ["-DUNITTEST"],
        "//conditions:default": [],
    }),
    deps = [
        "@score-baselibs//score/filesystem:filesystem",
        "@score-baselibs//score/json:interface",
        "@score-baselibs//score/json:json_parser",
        "@score-baselibs//score/result:result",
        
    ],
    visibility = ["//visibility:public"]
)

