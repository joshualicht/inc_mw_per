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
use rust_kvs::*;

#[repr(i32)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum FFIErrorCode {
    Ok = 0,
    UnmappedError = -1,
    FileNotFound = -2,
    KvsFileReadError = -3,
    KvsHashFileReadError = -4,
    JsonParserError = -5,
    JsonGeneratorError = -6,
    PhysicalStorageFailure = -7,
    IntegrityCorrupted = -8,
    ValidationFailed = -9,
    EncryptionFailed = -10,
    ResourceBusy = -11,
    OutOfStorageSpace = -12,
    QuotaExceeded = -13,
    AuthenticationFailed = -14,
    KeyNotFound = -15,
    SerializationFailed = -16,
    InvalidSnapshotId = -17,
    ConversionFailed = -18,
    MutexLockFailed = -19,
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

/// FFI function to flush the KVS on exit. (Currently disabled due to lib.rs flush_on_exit owenership issue -> refer to written findings)
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
pub extern "C" fn reset_ffi(kvs_ptr: *mut Kvs) -> FFIErrorCode {
    if kvs_ptr.is_null() {
        return FFIErrorCode::UnmappedError;
    }

    unsafe {
        (*kvs_ptr).reset().map(|_| FFIErrorCode::Ok).unwrap_or_else(|e| e.into())
    }
}
