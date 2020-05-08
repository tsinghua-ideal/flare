SecureSpark
===

##Overview

This is the secure version of `Native Spark`, or `Vega`(https://github.com/rajasekarv/vega). It is in development now.

##Infrastructure

It contains two submodules named `native_spark`, which is the focus to modify, and `incubator-teaclave-sgx-sdk`, the Rust SGX SDK developed by Baidu X Lab. `native_spark` has two branches: one is `master`, developed by the development team of `Vega`; the other is `secure`, which is modified by me based on `master`.

##Setup Environment

###Install rust toolchain in the master node

Please refer to https://www.rust-lang.org/tools/install. It's recommended to use `rustup` for management. 

And it's necessary to install `nightly` version using `rustup`. You can run `rustup toolchain list` to see which version you have. After installation, you need to switch to `nightly` toolchain. One of the methods is running `rustup override set nightly-x86_64-unknown-linux-gnu` in the top dir of this project.

###Install Intel SGX toolchain

Suppose tasks are not allowed to run on the master node, and all the tasks run on slave nodes. So, SGX toolchain (Driver, PSW, SDK) needs to be installed on slave nodes, while SDK needs to be installed on the master node for compiling the code. All are installed in `/opt/intel/`.

###Config for Spark

Checkout the `secure` branch of `native spark` first.

Please refer to https://rajasekarv.github.io/vega/ for `Getting started` part and `Executing an application` part. For setting environment variables, here are two examples in `./bin`. For example of distributed mode, the master is '172.16.124.9' and the slave is '172.16.111.236'.

###Write an app and run

You can write you Spark App in `./app/src/main.rs`. But only supports `map` now. And the you need to revise the correspond part in enclave. Even you need to slightly reviseone place in `native_spark`. I mark `TODO` for it, and you can use `grep` to search.


