#include <benchmark/benchmark.h>
#include "kvs.hpp"
#include <iostream>

static void BM_KVS_GetValue_single_threaded(benchmark::State& state) {
    auto open_res = KvsBuilder("Process_Name", 0)
                    .need_defaults_flag(false)
                    .need_kvs_flag(false)
                    .single_threaded_flag(true) // Set single-threaded for this benchmark
                    .build();
    if (!open_res){
        state.SkipWithError("KVS open failed");
        return;
    }
    Kvs kvs = std::move(open_res.value());
    kvs.set_value("test_key", KvsValue(42.0));

    for (auto _ : state) {
        // test case
        auto res = kvs.get_value("test_key");
        benchmark::DoNotOptimize(res);

        // stop measuring time for this operation
        state.PauseTiming();
        if (!res) {
            std::cerr << "Error getting value: " << res.error() << std::endl;
            state.SkipWithError("kvs.get_value failed");
        }
        state.ResumeTiming(); // resume measuring time for the next iteration
    }
}

static void BM_KVS_GetValue(benchmark::State& state) {
    auto open_res = KvsBuilder("Process_Name", 0)
                    .need_defaults_flag(false)
                    .need_kvs_flag(false)
                    .build();
    if (!open_res){
        state.SkipWithError("KVS open failed");
        return;
    }
    Kvs kvs = std::move(open_res.value());
    kvs.set_value("test_key", KvsValue(42.0));

    for (auto _ : state) {
        // test case
        auto res = kvs.get_value("test_key");
        benchmark::DoNotOptimize(res);

        // stop measuring time for this operation
        state.PauseTiming();
        if (!res) {
            std::cerr << "Error getting value: " << res.error() << std::endl;
            state.SkipWithError("kvs.get_value failed");
        }
        state.ResumeTiming(); // resume measuring time for the next iteration
    }
}
BENCHMARK(BM_KVS_GetValue)
    ->Unit(benchmark::kNanosecond)
    ->Iterations(100000000);

BENCHMARK(BM_KVS_GetValue_single_threaded)
    ->Unit(benchmark::kNanosecond)
    ->Iterations(100000000);
BENCHMARK_MAIN();