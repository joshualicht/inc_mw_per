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
use std::collections::HashMap;

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

#[repr(C)]
pub enum FFI_KvsValueType {
    Number,
    Boolean,
    String,
    Null,
    Array,
    Object,
}
#[repr(C)]
pub struct FFI_KvsValue {
    pub type_: FFI_KvsValueType,
    pub number: f64,            
    pub boolean: u8,            
    pub string: *mut c_char,   
    pub array_ptr: *mut FFI_KvsValue,
    pub array_len: usize,
    pub obj_keys: *mut *const c_char,
    pub obj_values: *mut FFI_KvsValue,
    pub obj_len: usize,
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


//----------------------------------------------------------
// Helper functions to convert between KvsValue and FFI_KvsValue

fn ffi_kvsvalue_to_kvsvalue(value: &FFI_KvsValue) -> KvsValue {
    let result: KvsValue;

    match value.type_ {
        FFI_KvsValueType::Number => {
            result = KvsValue::Number(value.number);
        }

        FFI_KvsValueType::Boolean => {
            let boolean_value: bool = value.boolean != 0;
            result = KvsValue::Boolean(boolean_value);
        }

        FFI_KvsValueType::String => {
            let cstr: &CStr = unsafe { CStr::from_ptr(value.string) };
            let owned_string: String = cstr.to_string_lossy().into_owned();
            result = KvsValue::String(owned_string);
        }

        FFI_KvsValueType::Null => {
            result = KvsValue::Null;
        }

        FFI_KvsValueType::Array => {
            let ffi_slice: &[FFI_KvsValue] = unsafe {
                std::slice::from_raw_parts(value.array_ptr, value.array_len)
            };
            let rust_array: Vec<KvsValue> = ffi_slice
                .iter()
                .map(|element| ffi_kvsvalue_to_kvsvalue(element))
                .collect();
            result = KvsValue::Array(rust_array);
        }

        FFI_KvsValueType::Object => {
            let key_ptrs: &[*const c_char] = unsafe {
                std::slice::from_raw_parts(value.obj_keys, value.obj_len)
            };
            let value_ptrs: &[FFI_KvsValue] = unsafe {
                std::slice::from_raw_parts(value.obj_values, value.obj_len)
            };

            let mut object_map: HashMap<String, KvsValue> = HashMap::with_capacity(value.obj_len);

            for i in 0..value.obj_len {
                let c_key: &CStr = unsafe { CStr::from_ptr(key_ptrs[i]) };
                let key_string: String = c_key.to_string_lossy().into_owned();
                let val: KvsValue = ffi_kvsvalue_to_kvsvalue(&value_ptrs[i]);

                object_map.insert(key_string, val);
            }

            result = KvsValue::Object(object_map);
        }
    }

    result
}

fn kvsvalue_to_ffi_kvsvalue(value: &KvsValue) -> FFI_KvsValue {
    let mut result = FFI_KvsValue {
        type_: FFI_KvsValueType::Null,
        number: 0.0,
        boolean: 0,
        string: std::ptr::null_mut(),
        array_ptr: std::ptr::null_mut(),
        array_len: 0,
        obj_keys: std::ptr::null_mut(),
        obj_values: std::ptr::null_mut(),
        obj_len: 0,
    };

    match value {
        KvsValue::Number(n) => {
            result.type_ = FFI_KvsValueType::Number;
            result.number = *n;
        }
        KvsValue::Boolean(b) => {
            result.type_ = FFI_KvsValueType::Boolean;
            result.boolean = *b as u8;
        }
        KvsValue::String(s) => {
            result.type_ = FFI_KvsValueType::String;
            result.string = CString::new(s.as_str()).unwrap().into_raw();
        }
        KvsValue::Null => {
            result.type_ = FFI_KvsValueType::Null;
        }
        KvsValue::Array(arr) => {
            let mut items: Vec<FFI_KvsValue> =
                arr.iter().map(kvsvalue_to_ffi_kvsvalue).collect();
            result.type_ = FFI_KvsValueType::Array;
            result.array_len = items.len();
            result.array_ptr = items.as_mut_ptr();
            std::mem::forget(items); // Prevent freeing the vector
        }
        KvsValue::Object(obj) => {
            let mut keys: Vec<*const c_char> = Vec::with_capacity(obj.len());
            let mut values: Vec<FFI_KvsValue> = Vec::with_capacity(obj.len());
            for (k, v) in obj {
                keys.push(CString::new(k.as_str()).unwrap().into_raw());
                values.push(kvsvalue_to_ffi_kvsvalue(v));
            }
            result.type_ = FFI_KvsValueType::Object;
            result.obj_len = keys.len();
            result.obj_keys = keys.as_mut_ptr();
            result.obj_values = values.as_mut_ptr();
            std::mem::forget(keys);
            std::mem::forget(values);
        }
    }

    result
}

/// Free FFI_KvsValue created in Rust
#[no_mangle]
pub extern "C" fn free_ffi_kvsvalue_rust(ptr: *mut FFI_KvsValue) {
    if !ptr.is_null() {
        let value = unsafe { &*ptr };

        match value.type_ {
            FFI_KvsValueType::String => {
                let cstr_ptr = value.string;
                if !cstr_ptr.is_null() {
                    unsafe {
                        drop(CString::from_raw(cstr_ptr));
                    }
                }
            }
            FFI_KvsValueType::Array => {
                let len = value.array_len;
                let items = value.array_ptr;
                if !items.is_null() {
                    for i in 0..len {
                        let item_ptr = unsafe { items.add(i) };
                        free_ffi_kvsvalue_rust(item_ptr);
                    }
                    unsafe {
                        Vec::from_raw_parts(items, len, len);
                    }
                }
            }
            FFI_KvsValueType::Object => {
                let len = value.obj_len;
                let kptr = value.obj_keys;
                let vptr = value.obj_values;

                if !kptr.is_null() && !vptr.is_null() {
                    for i in 0..len {
                        let key_ptr = unsafe { *kptr.add(i) };
                        if !key_ptr.is_null() {
                            unsafe {
                                drop(CString::from_raw(key_ptr as *mut c_char));
                            }
                        }
                        let val_ptr = unsafe { vptr.add(i) };
                        free_ffi_kvsvalue_rust(val_ptr);
                    }

                    unsafe {
                        Vec::from_raw_parts(kptr as *mut *const c_char, len, len);
                        Vec::from_raw_parts(vptr, len, len);
                    }
                }
            }
            _ => {}
        }
    }
}
//----------------------------------------------------------

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
    let mut result = FFIErrorCode::Ok;

    if kvshandle.is_null() {
        result = FFIErrorCode::InvalidKvsHandle;
    } else {
        let defaults = if need_defaults == 1 {
            OpenNeedDefaults::Required
        } else {
            OpenNeedDefaults::Optional
        };

        let kvs_flag = if need_kvs == 1 {
            OpenNeedKvs::Required
        } else {
            OpenNeedKvs::Optional
        };

        match Kvs::open(InstanceId::new(instance_id), defaults, kvs_flag) {
            Ok(kvs) => {
                let boxed = Box::new(kvs);
                unsafe { *kvshandle = Box::into_raw(boxed) as *mut c_void; }
            }
            Err(e) => {
                result = e.into();
            }
        }
    }

    result
}

// FFI function to flush the KVS on exit. (Currently disabled due to lib.rs flush_on_exit owenership issue -> refer to written findings)
//#[no_mangle]
//pub extern "C" fn flush_on_exit_ffi(kvs_ptr: *mut Kvs, flush_on_exit: bool) {
// unsafe {
//     if !kvs_ptr.is_null() {
//         (*kvs_ptr).flush_on_exit(flush_on_exit);
//     }
// }
//}


/// FFI function to reset the KVS.
#[no_mangle]
pub extern "C" fn reset_ffi(kvshandle: *mut c_void) -> FFIErrorCode {
    let result: FFIErrorCode;

    if !kvshandle.is_null() {
        let kvs: &mut Kvs = unsafe { &mut *(kvshandle as *mut Kvs) };
        result = kvs.reset().map(|_| FFIErrorCode::Ok).unwrap_or_else(|e| e.into());
    } else {
        result = FFIErrorCode::InvalidKvsHandle;
    }

    result
}

/// FFI function to get all keys from the KVS.
#[no_mangle]
pub extern "C" fn get_all_keys_ffi(
    kvshandle: *mut c_void,
    vec_keys: *mut *mut *const c_char,
    vec_len: *mut usize,
) -> FFIErrorCode {
    let result: FFIErrorCode;

    if kvshandle.is_null() {
        result = FFIErrorCode::InvalidKvsHandle;
    } else if vec_keys.is_null() || vec_len.is_null() {
        result = FFIErrorCode::InvalidArgument;
    } else {
        let kvs = unsafe { &*(kvshandle as *mut Kvs) };
        match kvs.get_all_keys() {
            Ok(keys) => {
                let mut ptrs: Vec<*const c_char> = keys
                    .into_iter()
                    .map(|s| {
                        let cstr = CString::new(s).unwrap();
                        cstr.into_raw() as *const c_char
                    })
                    .collect();

                let len = ptrs.len();
                let ptr_array = ptrs.as_mut_ptr();
                std::mem::forget(ptrs);

                unsafe {
                    *vec_keys = ptr_array;
                    *vec_len = len;
                }
                result = FFIErrorCode::Ok;
            }
            Err(e) => {
                result = e.into();
            }
        }
    }

    result
}

/// FFI helper to free the array of *const c_char produced by get_all_keys_ffi.
/// It does *not* free the individual C-Strings.
#[no_mangle]
pub extern "C" fn free_all_keys_vec_ffi(
    vec_ptr: *mut *const c_char,
    vec_len: usize,
) {
    if !vec_ptr.is_null() {
        unsafe {
            let _ = Vec::from_raw_parts(vec_ptr, vec_len, vec_len);
        }
    }
}

/// FFI helper to free a single C string produced by get_all_keys_ffi.
#[no_mangle]
pub extern "C" fn free_rust_cstring(ptr: *mut c_char) {
    if !ptr.is_null() {
        unsafe {
            drop(CString::from_raw(ptr));
        }
    }
}

/// FFI function to check if a key exists in the KVS.
#[no_mangle]
pub extern "C" fn key_exists_ffi(
    kvshandle: *mut c_void,
    key: *const c_char,
    key_exists: *mut u8,) -> FFIErrorCode {
    let mut result = FFIErrorCode::Ok;

    if kvshandle.is_null() {
        result = FFIErrorCode::InvalidKvsHandle;
    } else if key.is_null() || key_exists.is_null() {
        result = FFIErrorCode::InvalidArgument;
    } else {
        let kvs = unsafe { &*(kvshandle as *mut Kvs) };
        let key_cstr = unsafe { CStr::from_ptr(key) };
        match kvs.key_exists(key_cstr.to_str().unwrap()) {
            Ok(bool) => {
                unsafe { *key_exists = bool as u8; } // Cast Bool to c_int (0 or 1)
            }
            Err(e) => {
                result = e.into();
            }
        }
    }

    result
}

/// FFI function to get a value from the KVS.
//  get_value_ffi is not implemented yet, since the API will be reworked to be equivalent to get_default_value_ffi in context of return value

/// FFI function to get a default value from the KVS.
#[no_mangle]
pub extern "C" fn get_default_value_ffi(
    kvshandle: *mut c_void,
    key: *const c_char,
    out: *mut FFI_KvsValue,
) -> FFIErrorCode {
    let mut result = FFIErrorCode::Ok;

    if kvshandle.is_null() {
        result = FFIErrorCode::InvalidKvsHandle;
    } else if key.is_null() || out.is_null() {
        result = FFIErrorCode::InvalidArgument;
    } else {
        let kvs: &Kvs = unsafe { &*(kvshandle as *mut Kvs) };
        let cstr = unsafe { std::ffi::CStr::from_ptr(key) };
        match kvs.get_default_value(cstr.to_str().unwrap()) {
            Ok(val) => {
                let ffi = kvsvalue_to_ffi_kvsvalue(&val);
                unsafe { *out = ffi };
            }
            Err(e) => {
                result = e.into();
            }
        }
    }

    result
}

/// FFI function to check if a key is default in the KVS.
#[no_mangle]
pub extern "C" fn is_value_default_ffi(
    kvshandle: *mut c_void,
    key: *const c_char,
    is_default: *mut u8,
) -> FFIErrorCode {
    let mut result = FFIErrorCode::Ok;

    if kvshandle.is_null() {
        result = FFIErrorCode::InvalidKvsHandle;
    } else if key.is_null() || is_default.is_null() {
        result = FFIErrorCode::InvalidArgument;
    } else {
        let kvs = unsafe { &*(kvshandle as *mut Kvs) };
        let key_cstr = unsafe { CStr::from_ptr(key) }.to_str().unwrap();
        match kvs.is_value_default(key_cstr) {
            Ok(bool) => {
                unsafe { *is_default = bool as u8; }
            }
            Err(e) => {
                result = e.into();
            }
        }
    }

    result
}

/// FFI function to set a value.
#[no_mangle]
pub extern "C" fn set_value_ffi(
    kvshandle: *mut c_void,
    key: *const c_char,
    ffi_val: *const FFI_KvsValue,
) -> FFIErrorCode {
    let result: FFIErrorCode;

    if kvshandle.is_null() {
        result = FFIErrorCode::InvalidKvsHandle;
    } else if key.is_null() || ffi_val.is_null() {
        result = FFIErrorCode::InvalidArgument;
    } else {
        let kvs = unsafe { &*(kvshandle as *mut Kvs) };
        let key_cstr = unsafe { CStr::from_ptr(key) }.to_str().unwrap();
        let rust_val = ffi_kvsvalue_to_kvsvalue(unsafe { &*ffi_val });

        match kvs.set_value(key_cstr, rust_val) {
            Ok(_) => {result = FFIErrorCode::Ok;}
            Err(e) => result = e.into(),
        }
        
    }

    result
}

/// FFI function to remove a key from the KVS.
#[no_mangle]
pub extern "C" fn remove_key_ffi(kvshandle: *mut c_void, key: *const c_char) -> FFIErrorCode {
    let result: FFIErrorCode;

    if kvshandle.is_null() {
        result = FFIErrorCode::InvalidKvsHandle;
    } else if key.is_null() {
        result = FFIErrorCode::InvalidArgument;
    } else {
        let kvs = unsafe { &*(kvshandle as *mut Kvs) };
        let key_cstr = unsafe { CStr::from_ptr(key) }.to_str().unwrap();
        result = kvs.remove_key(key_cstr).map(|_| FFIErrorCode::Ok).unwrap_or_else(Into::into);
    }

    result
}


/// FFI function to flush the KVS.
#[no_mangle]
pub extern "C" fn flush_ffi(kvshandle: *mut c_void) -> FFIErrorCode {
    let result: FFIErrorCode;

    if kvshandle.is_null() {
        result = FFIErrorCode::InvalidKvsHandle;
    } else {
        let kvs = unsafe { &*(kvshandle as *mut Kvs) };
        result = kvs.flush().map(|_| FFIErrorCode::Ok).unwrap_or_else(Into::into);
    }

    result
}

/// FFI function to get the number of snapshots.
#[no_mangle]
pub extern "C" fn snapshot_count_ffi(kvshandle: *mut c_void, count: *mut usize) -> FFIErrorCode {
    let result: FFIErrorCode;

    if kvshandle.is_null() {
        result = FFIErrorCode::InvalidKvsHandle;
    } else if count.is_null() {
        result = FFIErrorCode::InvalidArgument;
    } else {
        let kvs = unsafe { &*(kvshandle as *mut Kvs) };
        let _count = kvs.snapshot_count();
        unsafe { *count = _count; }
        result = FFIErrorCode::Ok;
    }

    result
}

/// FFI function to get the maximum number of snapshots.
#[no_mangle]
pub extern "C" fn snapshot_max_count_ffi(max: *mut usize) -> FFIErrorCode {
    let result: FFIErrorCode;

    if max.is_null() {
        result = FFIErrorCode::InvalidArgument;
    } else {
        unsafe { *max = Kvs::snapshot_max_count(); }
        result = FFIErrorCode::Ok;
    }

    result
}

/// FFI function to restore a snapshot.
#[no_mangle]
pub extern "C" fn snapshot_restore_ffi(kvshandle: *mut c_void, id: usize) -> FFIErrorCode {
    let result: FFIErrorCode;

    if kvshandle.is_null() {
        result = FFIErrorCode::InvalidKvsHandle;
    } else {
        let kvs = unsafe { &*(kvshandle as *mut Kvs) };
        result = kvs.snapshot_restore(SnapshotId::new(id))
            .map(|_| FFIErrorCode::Ok)
            .unwrap_or_else(Into::into);
    }

    result
}

/// FFI function to get the kvs filename.
#[no_mangle]
pub extern "C" fn get_kvs_filename_ffi(
    kvshandle: *mut c_void,
    id: usize,
    filename: *mut *const c_char,
) -> FFIErrorCode {
    let result :FFIErrorCode;

    if kvshandle.is_null() {
        result = FFIErrorCode::InvalidKvsHandle;
    } else if filename.is_null() {
        result = FFIErrorCode::InvalidArgument;
    } else {
        let kvs = unsafe { &*(kvshandle as *mut Kvs) };
        let _filename = kvs.get_kvs_filename(SnapshotId::new(id));
        let cstring = CString::new(_filename).map_err(|_| {
            panic!("CString::new failed");}).unwrap();
        let ptr = cstring.into_raw();
        unsafe {
            *filename = ptr;
        }
        result = FFIErrorCode::Ok
    }

    result
}

/// FFI function to get the kvs hashname.
#[no_mangle]
pub extern "C" fn get_hash_filename_ffi(
    kvshandle: *mut c_void,
    id: usize,
    filename: *mut *const c_char,
) -> FFIErrorCode {
    let result :FFIErrorCode;
    
    if kvshandle.is_null() {
        result = FFIErrorCode::InvalidKvsHandle;
    } else if filename.is_null() {
        result = FFIErrorCode::InvalidArgument;
    } else {
        let kvs = unsafe { &*(kvshandle as *mut Kvs) };
        let _filename = kvs.get_hash_filename(SnapshotId::new(id));
        let cstring = CString::new(_filename).map_err(|_| {
            panic!("CString::new failed");}).unwrap();
        let ptr = cstring.into_raw();
        unsafe {
            *filename = ptr;
        }
        result = FFIErrorCode::Ok
    }

    result
}
