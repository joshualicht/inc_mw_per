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
/// This 

#![allow(unsafe_code)] //FFI

use std::ffi::c_void;
use rust_kvs::*;

#[repr(i32)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum FFIErrorCode {
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
pub extern "C" fn drop_kvs(handle: *mut c_void) {
    if !handle.is_null() {
        unsafe {
            let _ = Box::from_raw(handle as *mut Kvs); //Drop the Kvs instance
        }
    }
}

/// FFI function to open the KVS.
#[no_mangle]
pub extern "C" fn open_ffi(
    instance_id: usize,
    need_defaults: u32,
    need_kvs: u32,
    out_handle: *mut *mut c_void,
) -> FFIErrorCode {
    if out_handle.is_null() {
        return FFIErrorCode::UnmappedError;
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
            unsafe { *out_handle = Box::into_raw(boxed) as *mut c_void; }
            FFIErrorCode::Ok
        }
        Err(e) => e.into(),
    }
}


/// FFI function to reset the KVS.
#[no_mangle]
pub extern "C" fn reset_ffi(handle: *mut c_void) -> FFIErrorCode {
    if handle.is_null() {
        return FFIErrorCode::UnmappedError;
    }
    let kvs: &mut Kvs = unsafe { &mut *(handle as *mut Kvs) };
    kvs.reset().map(|_| FFIErrorCode::Ok).unwrap_or_else(|e| e.into())
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