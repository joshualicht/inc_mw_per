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
    match value.type_ {
        FFI_KvsValueType::Number  => KvsValue::Number(value.number),
        FFI_KvsValueType::Boolean => KvsValue::Boolean(value.boolean != 0),
        FFI_KvsValueType::String  => {
            let s = unsafe { CStr::from_ptr(value.string) }
                .to_string_lossy()
                .into_owned();
            KvsValue::String(s)
        }
        FFI_KvsValueType::Null    => KvsValue::Null,
        FFI_KvsValueType::Array   => {
            let slice = unsafe { std::slice::from_raw_parts(value.array_ptr, value.array_len) };
            let vec = slice.iter().map(ffi_kvsvalue_to_kvsvalue).collect();
            KvsValue::Array(vec)
        }
        FFI_KvsValueType::Object  => {
            let keys = unsafe { std::slice::from_raw_parts(value.obj_keys, value.obj_len) };
            let values = unsafe { std::slice::from_raw_parts(value.obj_values, value.obj_len) };
            let mut map = HashMap::with_capacity(value.obj_len);
            for i in 0..value.obj_len {
                let k = unsafe { CStr::from_ptr(keys[i]) }
                    .to_string_lossy()
                    .into_owned();
                map.insert(k, ffi_kvsvalue_to_kvsvalue(&values[i]));
            }
            KvsValue::Object(map)
        }
    }
}

fn kvsvalue_to_ffi_kvsvalue(value: &KvsValue) -> FFI_KvsValue {
    match value {
        KvsValue::Number(n) => FFI_KvsValue {
            type_: FFI_KvsValueType::Number,
            number: *n,
            boolean: 0, string: std::ptr::null_mut(),
            array_ptr: std::ptr::null_mut(), array_len: 0,
            obj_keys: std::ptr::null_mut(), obj_values: std::ptr::null_mut(), obj_len: 0,
        },
        KvsValue::Boolean(b) => FFI_KvsValue {
            type_: FFI_KvsValueType::Boolean,
            number: 0.0, boolean: *b as u8, string: std::ptr::null_mut(),
            array_ptr: std::ptr::null_mut(), array_len: 0,
            obj_keys: std::ptr::null_mut(), obj_values: std::ptr::null_mut(), obj_len: 0,
        },
        KvsValue::String(s) => FFI_KvsValue {
            type_: FFI_KvsValueType::String,
            number: 0.0, boolean: 0, string: CString::new(s.as_str()).unwrap().into_raw(),
            array_ptr: std::ptr::null_mut(), array_len: 0,
            obj_keys: std::ptr::null_mut(), obj_values: std::ptr::null_mut(), obj_len: 0,
        },
        KvsValue::Null => FFI_KvsValue {
            type_: FFI_KvsValueType::Null,
            number: 0.0, boolean: 0, string: std::ptr::null_mut(),
            array_ptr: std::ptr::null_mut(), array_len: 0,
            obj_keys: std::ptr::null_mut(), obj_values: std::ptr::null_mut(), obj_len: 0,
        },
        KvsValue::Array(arr) => {
            let mut items: Vec<FFI_KvsValue> = arr.iter()
                .map(kvsvalue_to_ffi_kvsvalue)
                .collect();
            let len = items.len();
            let ptr = items.as_mut_ptr();
            std::mem::forget(items);
            FFI_KvsValue {
                type_: FFI_KvsValueType::Array,
                number: 0.0, boolean: 0, string: std::ptr::null_mut(),
                array_ptr: ptr, array_len: len,
                obj_keys: std::ptr::null_mut(), obj_values: std::ptr::null_mut(), obj_len: 0,
            }
        }
        KvsValue::Object(obj) => {
            let mut keys: Vec<*const c_char> = Vec::new();
            let mut values: Vec<FFI_KvsValue> = Vec::new();
            keys.reserve(obj.len());
            values.reserve(obj.len());
            for (k, v) in obj {
                keys.push(CString::new(k.as_str()).unwrap().into_raw());
                values.push(kvsvalue_to_ffi_kvsvalue(v));
            }
            let key_ptr = keys.as_mut_ptr();
            let val_ptr = values.as_mut_ptr();
            let len = keys.len();
            std::mem::forget(keys);
            std::mem::forget(values);
            FFI_KvsValue {
                type_: FFI_KvsValueType::Object,
                number: 0.0, boolean: 0, string: std::ptr::null_mut(),
                array_ptr: std::ptr::null_mut(), array_len: 0,
                obj_keys: key_ptr, obj_values: val_ptr, obj_len: len,
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn free_ffi_kvsvalue_rust(ptr: *mut FFI_KvsValue) {
    if ptr.is_null() { return; }
    let value = unsafe { &*ptr };
    match value.type_ {
        FFI_KvsValueType::String => {
            unsafe { drop(CString::from_raw(value.string)) };
        }
        FFI_KvsValueType::Array => {
            let len = value.array_len;
            let items = value.array_ptr;
            for i in 0..len {
                free_ffi_kvsvalue_rust(unsafe { items.add(i) });
            }
            unsafe { Vec::from_raw_parts(items, len, len) };
        }
        FFI_KvsValueType::Object => {
            let len = value.obj_len;
            let kptr = value.obj_keys;
            let vptr = value.obj_values;
            for i in 0..len {
                unsafe { drop(CString::from_raw(*kptr.add(i) as *mut c_char)) };
                free_ffi_kvsvalue_rust(unsafe { vptr.add(i) });
            }
            unsafe {
                Vec::from_raw_parts(kptr as *mut *const c_char, len, len);
                Vec::from_raw_parts(vptr, len, len);
            }
        }
        _ => {}
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
pub extern "C" fn free_rust_cstring(ptr: *mut c_char) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        let string = CString::from_raw(ptr as *mut c_char);
        drop(string);
    }
}

/// FFI function to check if a key exists in the KVS.
#[no_mangle]
pub extern "C" fn key_exists_ffi(
    kvshandle: *mut c_void,
    key: *const c_char,
    key_exists: *mut u8,) -> FFIErrorCode {
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
#[no_mangle]
pub extern "C" fn get_default_value_ffi(
    kvshandle: *mut c_void,
    key: *const c_char,
    out: *mut FFI_KvsValue,
) -> FFIErrorCode {
    if kvshandle.is_null(){
        return FFIErrorCode::InvalidKvsHandle
    } 
    else if key.is_null() || out.is_null() {
        return FFIErrorCode::InvalidArgument;
    }
    let kvs: &Kvs = unsafe { &*(kvshandle as *mut Kvs) };
    let cstr = unsafe { std::ffi::CStr::from_ptr(key) };
    match kvs.get_default_value(cstr.to_str().unwrap()) {
        Ok(val) => {
            let ffi = kvsvalue_to_ffi_kvsvalue(&val);
            unsafe { *out = ffi };
            FFIErrorCode::Ok
        }
        Err(e) => e.into(),
    }
}

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
#[no_mangle]
pub extern "C" fn set_value_ffi(
    kvshandle: *mut c_void,
    key: *const c_char,
    ffi_val: *const FFI_KvsValue,
) -> FFIErrorCode {
    if kvshandle.is_null(){
        return FFIErrorCode::InvalidKvsHandle;
    }
    else if key.is_null() || ffi_val.is_null() {
        return FFIErrorCode::InvalidArgument;
    }

    let kvs = unsafe { &*(kvshandle as *mut Kvs) };

    let key_cstr = unsafe { CStr::from_ptr(key) }.to_str().unwrap();

    let rust_val = ffi_kvsvalue_to_kvsvalue(unsafe { &*ffi_val });

    match kvs.set_value(key_cstr, rust_val) {
        Ok(_)  => FFIErrorCode::Ok,
        Err(e) => e.into(),
    }
}

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
    let cstring = CString::new(_filename).map_err(|_| {
        panic!("CString::new failed");}).unwrap();
    let ptr = cstring.into_raw();
    unsafe {
        *filename = ptr;
    }
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
    let cstring = CString::new(_filename).map_err(|_| {
        panic!("CString::new failed");}).unwrap();
    let ptr = cstring.into_raw();
    unsafe {
        *filename = ptr;
    }
    FFIErrorCode::Ok
}
