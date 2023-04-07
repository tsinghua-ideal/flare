FLARE
===

## Overview

This is the secure version of `Native Spark`, or `Vega`(https://github.com/rajasekarv/vega). It contains two modes: encryption mode and oblivious mode. You can switch the mode by switching to the expected branch.

## Infrastructure

It contains two submodules named `flare-core-untrusted`, which is the focus to modify, and `incubator-teaclave-sgx-sdk`, the Rust SGX SDK developed by Baidu X Lab. `flare-core-untrusted` has one branch `master`, which is modified by me based on `master` branch of `Vega` repo.

## Setup Environment

### Install rust toolchain in the leader node

Please refer to https://www.rust-lang.org/tools/install. It's recommended to use `rustup` for management. 

### Install Intel SGX toolchain

We require Intel SGX SDK 2.14.

Tasks will not run on the master node, and all the tasks run on worker nodes. So, SGX toolchain (Driver, PSW, SDK) needs to be installed on worker nodes, while SDK needs to be installed on the leader node for compiling the code. All are installed in `/opt/intel/`.

### Config for Spark

Checkout the `secure` branch of `native spark` first.

Please refer to https://rajasekarv.github.io/vega/ for `Getting started` part and `Executing an application` part. 

### Compile

Just type `make`

## Reference

If you use this framework in your research, please kindly cite the following papers.

Xiang Li, Fabing Li, and Mingyu Gao.
Flare: A Fast, Secure, and Memory-Efficient Distributed Analytics Framework.
*Proc. VLDB Endow*. 16, 6 (2023), 1439–1452.

Xiang Li, Nuozhou Sun, Yunqian Luo, and Mingyu Gao.
SODA: A Set of Fast Oblivious Algorithms in Distributed Secure Data Analytics.
*Proc. VLDB Endow*. 16, 7 (2023), 1671-1684.
