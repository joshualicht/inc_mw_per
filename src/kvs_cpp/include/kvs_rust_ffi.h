#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

enum class FFIErrorCode : int32_t {
  Ok = 100,
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
void drop_kvs(void *handle);

/// FFI function to open the KVS.
FFIErrorCode open_ffi(uintptr_t instance_id,
                      uint32_t need_defaults,
                      uint32_t need_kvs,
                      void **out_handle);

/// FFI function to reset the KVS.
FFIErrorCode reset_ffi(void *handle);

}  // extern "C"
