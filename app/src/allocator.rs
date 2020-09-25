use std::alloc::{GlobalAlloc, Layout, System};
use sgx_types::*;

#[no_mangle]
pub unsafe extern "C" fn ocall_alloc(size: usize, align: usize) -> usize {
    let layout = Layout::from_size_align_unchecked(size, align);
    System.alloc(layout) as usize
}

#[no_mangle]
pub unsafe extern "C" fn ocall_dealloc(ptr: usize, size: usize, align: usize) -> sgx_status_t {
    let layout = Layout::from_size_align_unchecked(size, align);
    System.dealloc(ptr as *mut u8, layout);
    sgx_status_t::SGX_SUCCESS
}

#[no_mangle]
pub unsafe extern "C" fn ocall_print_id(id: usize) -> sgx_status_t {
    println!("");
    println!("thread id = {:?}", id);
    sgx_status_t::SGX_SUCCESS
}

#[no_mangle]
pub unsafe extern "C" fn ocall_print_usize(i: usize) -> sgx_status_t {
    println!("{:?}", i);
    sgx_status_t::SGX_SUCCESS
}

#[no_mangle]
pub unsafe extern "C" fn ocall_print_mapper(arr: usize) -> sgx_status_t {
    print!("{:?} ", arr);
    sgx_status_t::SGX_SUCCESS
}

