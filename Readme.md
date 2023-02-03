SecureSpark
===

## Overview

This is the secure version of `Native Spark`, or `Vega`(https://github.com/rajasekarv/vega). It is in development now.

## Infrastructure

It contains two submodules named `flare-core-untrusted`, which is the focus to modify, and `incubator-teaclave-sgx-sdk`, the Rust SGX SDK developed by Baidu X Lab. `flare-core-untrusted` has one branch `master`, which is modified by me based on `master` branch of `Vega` repo.

## Setup Environment

### git procedure

```
git clone https://github.com/tsinghua-ideal/SecureSpark
git submodule update --init --recursive
```

Patching with the original `Vega` is feasible, you can do this in directory `flare-core-untrusted`:
```
git remote add upstream https://github.com/rajasekarv/vega
```
When there is update in `master` branch of `Vega`, you can do this to merge it into our secure version: 
```
git fetch upstream
git merge remotes/upstream/master
```

### Install rust toolchain in the master node

Please refer to https://www.rust-lang.org/tools/install. It's recommended to use `rustup` for management. 

### Install Intel SGX toolchain

We require Intel SGX SDK 2.14.

Tasks will not run on the master node, and all the tasks run on slave nodes. So, SGX toolchain (Driver, PSW, SDK) needs to be installed on slave nodes, while SDK needs to be installed on the master node for compiling the code. All are installed in `/opt/intel/`.

### Config for Spark

Checkout the `secure` branch of `native spark` first.

Please refer to https://rajasekarv.github.io/vega/ for `Getting started` part and `Executing an application` part. For setting environment variables, here are two examples in `./bin`. For example of distributed mode, the master is '172.16.124.9' and the slave is '172.16.111.236'. To make the configuration effective, you need to type, e.g., `source bin/set_env_local.sh`. `bin/set_env_local.sh` is useless.

## Usage

You can write you Spark App in `./app/src/main.rs`. And the you need to revise the correspond part in './enclave/src/lib.rs'.
