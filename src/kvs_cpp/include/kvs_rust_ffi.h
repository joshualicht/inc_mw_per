#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

enum class FFIErrorCode : int32_t {
  Ok = 100,
  InvalidKvsHandle = 101,
  InvalidArgument = 102,
  UnmappedError = 0,
  FileNotFound = 1,
  KvsFileReadError = 2,
  KvsHashFileReadError = 3,
  JsonParserError = 4,
  JsonGeneratorError = 5,
  PhysicalStorageFailure = 6,
  IntegrityCorrupted = 7,
  ValidationFailed = 8,
  EncryptionFailed = 9,
  ResourceBusy = 10,
  OutOfStorageSpace = 11,
  QuotaExceeded = 12,
  AuthenticationFailed = 13,
  KeyNotFound = 14,
  SerializationFailed = 15,
  InvalidSnapshotId = 16,
  ConversionFailed = 17,
  MutexLockFailed = 18,
};

extern "C" {

/// FFI function to drop the KVS instance.
void drop_kvs(void *kvshandle);

/// FFI function to open the KVS.
FFIErrorCode open_ffi(uintptr_t instance_id,
                      uint32_t need_defaults,
                      uint32_t need_kvs,
                      void **kvshandle);

/// FFI function to reset the KVS.
FFIErrorCode reset_ffi(void *kvshandle);

/// FFI function to get all keys from the KVS.
FFIErrorCode get_all_keys_ffi(void *kvshandle, const char ***vec_keys, uintptr_t *vec_len);

/// FFI helper to free the array of *const c_char produced by get_all_keys_ffi.
/// It does *not* free the individual C-Strings.
void free_all_keys_vec_ffi(const char **vec_ptr, uintptr_t vec_len);

/// FFI helper to free a single C string produced by get_all_keys_ffi.
void free_rust_cstring(char *ptr);

/// FFI function to check if a key exists in the KVS.
FFIErrorCode key_exists_ffi(void *kvshandle, const char *key, uint8_t *key_exists);

/// FFI function to get a value from the KVS.
/// FFI function to get a default value from the KVS.
/// FFI function to check if a key is default in the KVS.
FFIErrorCode is_value_default_ffi(void *kvshandle, const char *key, uint8_t *is_default);

/// FFI function to set a value.
/// FFI function to remove a key from the KVS.
FFIErrorCode remove_key_ffi(void *kvshandle, const char *key);

/// FFI function to flush the KVS.
FFIErrorCode flush_ffi(void *kvshandle);

/// FFI function to get the number of snapshots.
FFIErrorCode snapshot_count_ffi(void *kvshandle, uintptr_t *count);

/// FFI function to get the maximum number of snapshots.
FFIErrorCode snapshot_max_count_ffi(uintptr_t *max);

/// FFI function to restore a snapshot.
FFIErrorCode snapshot_restore_ffi(void *kvshandle, uintptr_t id);

/// FFI function to get the kvs filename.
FFIErrorCode get_kvs_filename_ffi(void *kvshandle, uintptr_t id, const char **filename);

/// FFI function to get the kvs hashname.
FFIErrorCode get_hash_filename_ffi(void *kvshandle, uintptr_t id, const char **filename);

}  // extern "C"
