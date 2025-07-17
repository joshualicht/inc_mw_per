#include <benchmark/benchmark.h>
#include "kvs.hpp"
#include <iostream>

static void BM_KVS_GetValue_single_threaded(benchmark::State& state) { //dedicated function without if clause
    auto open_res = KvsBuilder("Process_Name", 0)
                    .need_defaults_flag(false)
                    .need_kvs_flag(false)
                    .single_threaded_flag(true)
                    .build();
    if (!open_res){
        state.SkipWithError("KVS open failed");
        return;
    }
    Kvs kvs = std::move(open_res.value());
    kvs.set_value("test_key", KvsValue(42.0));

    for (auto _ : state) {
        benchmark::DoNotOptimize(kvs.get_value_s("test_key"));
    }
}

static void BM_KVS_GetValue(benchmark::State& state) { //get_value with standard mutex protection
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
        benchmark::DoNotOptimize(kvs.get_value("test_key"));
    }
}

static void BM_KVS_GetValue_single_threaded_if(benchmark::State& state) { //get_value with single_threaded flag and if clause
    auto open_res = KvsBuilder("Process_Name", 0)
                    .need_defaults_flag(false)
                    .need_kvs_flag(false)
                    .single_threaded_flag(true)
                    .build();
    if (!open_res){
        state.SkipWithError("KVS open failed");
        return;
    }
    Kvs kvs = std::move(open_res.value());
    kvs.set_value("test_key", KvsValue(42.0));

    for (auto _ : state) {
        benchmark::DoNotOptimize(kvs.get_value_if("test_key"));
    }
}

static void BM_KVS_GetValue_if(benchmark::State& state) { //get_value with multi_threaded flag and if clause
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
        benchmark::DoNotOptimize(kvs.get_value_if("test_key"));
    }
}


static void BM_KVS_GetValue_Double(benchmark::State& state) { //mutex protection with double get_value (just for comparison)
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
        benchmark::DoNotOptimize(kvs.get_value("test_key"));
        benchmark::ClobberMemory();
        benchmark::DoNotOptimize(kvs.get_value("test_key"));
    }
}




static void BM_KVS_GetValue_single_threaded_ptr(benchmark::State& state) { //dedicated function without if clause
    auto open_res = KvsBuilder("Process_Name", 0)
                    .need_defaults_flag(false)
                    .need_kvs_flag(false)
                    .single_threaded_flag(true)
                    .build();
    if (!open_res){
        state.SkipWithError("KVS open failed");
        return;
    }
    Kvs kvs = std::move(open_res.value());
    kvs.set_value("test_key", KvsValue(42.0));

    for (auto _ : state) {
        benchmark::DoNotOptimize(kvs.get_value_ptr("test_key"));
    }
}

static void BM_KVS_GetValue_ptr(benchmark::State& state) { //get_value with standard mutex protection
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
        benchmark::DoNotOptimize(kvs.get_value_ptr("test_key"));
    }
}


BENCHMARK(BM_KVS_GetValue)
 ->Iterations(100000000);
BENCHMARK(BM_KVS_GetValue_Double)
 ->Iterations(100000000);
BENCHMARK(BM_KVS_GetValue_single_threaded)
 ->Iterations(100000000);
BENCHMARK(BM_KVS_GetValue_if)
 ->Iterations(100000000);
BENCHMARK(BM_KVS_GetValue_single_threaded_if)
 ->Iterations(100000000);
BENCHMARK(BM_KVS_GetValue_ptr)
 ->Iterations(100000000);
BENCHMARK(BM_KVS_GetValue_single_threaded_ptr)
 ->Iterations(100000000);

BENCHMARK_MAIN();