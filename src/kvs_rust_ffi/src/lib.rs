// Copyright (c) 2025 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Apache License Version 2.0 which is available at
// <https://www.apache.org/licenses/LICENSE-2.0>
//
// SPDX-License-Identifier: Apache-2.0

//! # Key-Value-Storage API and Implementation
//!
//! ## Introduction
//!
//! This crate provides FFI access to the Key-Value Storage (KVS) API and its implementation.
//!

#![allow(unsafe_code)] //FFI

use std::ffi::{c_void, CStr, CString};
use std::os::raw::{c_char};
use rust_kvs::*;

#[repr(i32)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum FFIErrorCode {
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
}

impl From<rust_kvs::ErrorCode> for FFIErrorCode {
    fn from(err: rust_kvs::ErrorCode) -> Self {
        use rust_kvs::ErrorCode::*;
        match err {
            UnmappedError => FFIErrorCode::UnmappedError,
            FileNotFound => FFIErrorCode::FileNotFound,
            KvsFileReadError => FFIErrorCode::KvsFileReadError,
            KvsHashFileReadError => FFIErrorCode::KvsHashFileReadError,
            JsonParserError => FFIErrorCode::JsonParserError,
            JsonGeneratorError => FFIErrorCode::JsonGeneratorError,
            PhysicalStorageFailure => FFIErrorCode::PhysicalStorageFailure,
            IntegrityCorrupted => FFIErrorCode::IntegrityCorrupted,
            ValidationFailed => FFIErrorCode::ValidationFailed,
            EncryptionFailed => FFIErrorCode::EncryptionFailed,
            ResourceBusy => FFIErrorCode::ResourceBusy,
            OutOfStorageSpace => FFIErrorCode::OutOfStorageSpace,
            QuotaExceeded => FFIErrorCode::QuotaExceeded,
            AuthenticationFailed => FFIErrorCode::AuthenticationFailed,
            KeyNotFound => FFIErrorCode::KeyNotFound,
            SerializationFailed => FFIErrorCode::SerializationFailed,
            InvalidSnapshotId => FFIErrorCode::InvalidSnapshotId,
            ConversionFailed => FFIErrorCode::ConversionFailed,
            MutexLockFailed => FFIErrorCode::MutexLockFailed,
        }
    }
}

/// FFI function to drop the KVS instance.
#[no_mangle]
pub extern "C" fn drop_kvs(kvshandle: *mut c_void) {
    if !kvshandle.is_null() {
        unsafe {
            let _ = Box::from_raw(kvshandle as *mut Kvs); 
        }
    }
}

/// FFI function to open the KVS.
#[no_mangle]
pub extern "C" fn open_ffi(
    instance_id: usize,
    need_defaults: u32,
    need_kvs: u32,
    kvshandle: *mut *mut c_void,
) -> FFIErrorCode {
    if kvshandle.is_null() {
        return FFIErrorCode::InvalidKvsHandle;
    }

    match Kvs::open(
        InstanceId::new(instance_id),
        if need_defaults == 1{
            OpenNeedDefaults::Required
        } else {
            OpenNeedDefaults::Optional
        },
        if need_kvs == 1 {
            OpenNeedKvs::Required
        } else {
            OpenNeedKvs::Optional
        },
    ) {
        Ok(kvs) => {
            let boxed = Box::new(kvs);
            unsafe { *kvshandle = Box::into_raw(boxed) as *mut c_void; }
            FFIErrorCode::Ok
        }
        Err(e) => e.into(),
    }
}

// FFI function to flush the KVS on exit. (Currently disabled due to lib.rs flush_on_exit owenership issue -> refer to written findings)
//#[no_mangle]
//pub extern "C" fn flush_on_exit_ffi(kvs_ptr: *mut Kvs, flush_on_exit: bool) {
//    if kvs_ptr.is_null() {
//        return;
//    }
//
//    unsafe {
//        (*kvs_ptr).flush_on_exit(flush_on_exit);
//    }
//}


/// FFI function to reset the KVS.
#[no_mangle]
pub extern "C" fn reset_ffi(kvshandle: *mut c_void) -> FFIErrorCode {
    if kvshandle.is_null() {
        return FFIErrorCode::InvalidKvsHandle;
    }
    let kvs: &mut Kvs = unsafe { &mut *(kvshandle as *mut Kvs) };
    kvs.reset().map(|_| FFIErrorCode::Ok).unwrap_or_else(|e| e.into())
}

/// FFI function to get all keys from the KVS.
#[no_mangle]
pub extern "C" fn get_all_keys_ffi(
    kvshandle: *mut c_void,
    vec_keys: *mut *mut *const c_char,
    vec_len: *mut usize,
) -> FFIErrorCode {
    if kvshandle.is_null() {
        return FFIErrorCode::InvalidKvsHandle;
    }
    if vec_keys.is_null() || vec_len.is_null() {
        return FFIErrorCode::InvalidArgument;
    }

    let kvs = unsafe { &*(kvshandle as *mut Kvs) };
    match kvs.get_all_keys() {
        Ok(keys) => {
            let mut ptrs: Vec<*const c_char> = keys
                .into_iter()
                .map(|s| {
                   
                    let cstr = CString::new(s).unwrap();
                    let raw = cstr.into_raw();
                    raw as *const c_char
                })
                .collect();
       
            let len = ptrs.len();
            let ptr_array = ptrs.as_mut_ptr();
            std::mem::forget(ptrs);

            unsafe {
                *vec_keys = ptr_array;
                *vec_len = len;
            }
            FFIErrorCode::Ok
        }
        Err(e) => e.into(),
    }
}

/// FFI helper to free the array of *const c_char produced by get_all_keys_ffi.
/// It does *not* free the individual C-Strings.
#[no_mangle]
pub extern "C" fn free_all_keys_vec_ffi(
    vec_ptr: *mut *const c_char,
    vec_len: usize,
) {
    if vec_ptr.is_null() {
        return;
    }
    unsafe { let _ = Vec::from_raw_parts(vec_ptr, vec_len, vec_len); }; // length = capacity 
}

/// FFI helper to free a single C string produced by get_all_keys_ffi.
#[no_mangle]
pub extern "C" fn free_key_id_ffi(ptr: *mut c_char) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        drop(CString::from_raw(ptr as *mut c_char));
    }
}

/// FFI function to check if a key exists in the KVS.
#[no_mangle]
pub extern "C" fn key_exists_ffi(
    kvshandle: *mut c_void,
    key: *const c_char,
    key_exists: *mut u8,
) -> FFIErrorCode {
    if kvshandle.is_null(){
        return FFIErrorCode::InvalidKvsHandle;
    } else if key.is_null() || key_exists.is_null() {
        return FFIErrorCode::InvalidArgument;
    }
    let kvs = unsafe { &*(kvshandle as *mut Kvs) };
    let key_cstr = unsafe { CStr::from_ptr(key) };
    match kvs.key_exists(key_cstr.to_str().unwrap()) {
        Ok(bool) => {
            unsafe { *key_exists = bool as u8; } // Cast Bool to c_int (0 or 1)
            FFIErrorCode::Ok
        }
        Err(e) => e.into(),
    }
}
/// FFI function to get a value from the KVS.
//  get_value_ffi is not implemented yet

/// FFI function to get a default value from the KVS.
//  get_default_value_ffi is not implemented yet

/// FFI function to check if a key is default in the KVS.
#[no_mangle]
pub extern "C" fn is_value_default_ffi(
    kvshandle: *mut c_void,
    key: *const c_char,
    is_default: *mut u8,
) -> FFIErrorCode {
    if kvshandle.is_null(){
        return FFIErrorCode::InvalidKvsHandle;
    } else if key.is_null() || is_default.is_null() {
        return FFIErrorCode::InvalidArgument;
    }
    let kvs = unsafe { &*(kvshandle as *mut Kvs) };
    let key_cstr = unsafe { CStr::from_ptr(key) }.to_str().unwrap();
    match kvs.is_value_default(key_cstr) {
        Ok(bool) => {
            unsafe { *is_default = bool as u8; }
            FFIErrorCode::Ok
        }
        Err(e) => e.into(),
    }
}

/// FFI function to set a value.
//  set_value_ffi is not implemented yet

/// FFI function to remove a key from the KVS.
#[no_mangle]
pub extern "C" fn remove_key_ffi(kvshandle: *mut c_void, key: *const c_char) -> FFIErrorCode {
    if kvshandle.is_null(){
        return FFIErrorCode::InvalidKvsHandle;
    }else if key.is_null() {
        return FFIErrorCode::InvalidArgument;
    }
    let kvs = unsafe { &*(kvshandle as *mut Kvs) };
    let key_cstr = unsafe { CStr::from_ptr(key) }.to_str().unwrap();
    kvs.remove_key(key_cstr).map(|_| FFIErrorCode::Ok).unwrap_or_else(Into::into)
}


/// FFI function to flush the KVS.
#[no_mangle]
pub extern "C" fn flush_ffi(kvshandle: *mut c_void) -> FFIErrorCode {
    if kvshandle.is_null() {
        return FFIErrorCode::InvalidKvsHandle;
    }
    let kvs = unsafe { &*(kvshandle as *mut Kvs) };
    kvs.flush().map(|_| FFIErrorCode::Ok).unwrap_or_else(Into::into)
}

/// FFI function to get the number of snapshots.
#[no_mangle]
pub extern "C" fn snapshot_count_ffi(kvshandle: *mut c_void, count: *mut usize) -> FFIErrorCode {
    if kvshandle.is_null(){
        return FFIErrorCode::InvalidKvsHandle;
    } else if count.is_null() {
        return FFIErrorCode::InvalidArgument;
    }
    let kvs = unsafe { &*(kvshandle as *mut Kvs) };
    let _count = kvs.snapshot_count();
    unsafe { *count = _count; }
    FFIErrorCode::Ok
}

/// FFI function to get the maximum number of snapshots.
#[no_mangle]
pub extern "C" fn snapshot_max_count_ffi(max: *mut usize) -> FFIErrorCode {
    if max.is_null() {
        return FFIErrorCode::InvalidArgument;
    }
    unsafe { *max = Kvs::snapshot_max_count(); }
    FFIErrorCode::Ok
}

/// FFI function to restore a snapshot.
#[no_mangle]
pub extern "C" fn snapshot_restore_ffi(kvshandle: *mut c_void, id: usize) -> FFIErrorCode {
    if kvshandle.is_null() {
        return FFIErrorCode::InvalidKvsHandle;
    }
    let kvs = unsafe { &*(kvshandle as *mut Kvs) };
    kvs.snapshot_restore(SnapshotId::new(id))
        .map(|_| FFIErrorCode::Ok)
        .unwrap_or_else(Into::into)
}

/// Internal Helper Function: Function to allocate a C string and return its pointer.
fn cstring_alloc(string: String, cstring_ptr: *mut *const c_char) {
    let cstring = CString::new(string).map_err(|_| {
        panic!("CString::new failed");
    }).unwrap();
    let ptr = cstring.into_raw();
    unsafe {
        *cstring_ptr = ptr;
    }
}

/// FFI function to deallocate a C string (allocated by get_kvs_filename_ffi or get_hash_filename_ffi).
#[no_mangle]
pub extern "C" fn filename_dealloc_ffi(cstring: *mut c_char) {
    if !cstring.is_null() {
        unsafe { drop(CString::from_raw(cstring)) }
    }
}

/// FFI function to get the kvs filename.
#[no_mangle]
pub extern "C" fn get_kvs_filename_ffi(
    kvshandle: *mut c_void,
    id: usize,
    filename: *mut *const c_char,
) -> FFIErrorCode {
    if kvshandle.is_null(){
        return FFIErrorCode::InvalidKvsHandle;
    }
    else if filename.is_null() {
        return FFIErrorCode::InvalidArgument;
    }
    let kvs = unsafe { &*(kvshandle as *mut Kvs) };
    let _filename = kvs.get_kvs_filename(SnapshotId::new(id));
    cstring_alloc(_filename, filename);
    FFIErrorCode::Ok
}

/// FFI function to get the kvs hashname.
#[no_mangle]
pub extern "C" fn get_hash_filename_ffi(
    kvshandle: *mut c_void,
    id: usize,
    filename: *mut *const c_char,
) -> FFIErrorCode {
    if kvshandle.is_null(){
        return FFIErrorCode::InvalidKvsHandle;
    } else if filename.is_null() {
        return FFIErrorCode::InvalidArgument;
    }
    let kvs = unsafe { &*(kvshandle as *mut Kvs) };
    let _filename = kvs.get_hash_filename(SnapshotId::new(id));
    cstring_alloc(_filename, filename);
    FFIErrorCode::Ok
}
