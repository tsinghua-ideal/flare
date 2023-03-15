SODA
===

## Overview

It is an oblivious mode of FLARE, applying SODA oblivious algorithms. 

## Setup Environment

### Install rust toolchain in the leader node

Please refer to https://www.rust-lang.org/tools/install. It's recommended to use `rustup` for management. 

### Install Intel SGX toolchain

We require Intel SGX SDK 2.14.

Tasks will not run on the master node, and all the tasks run on worker nodes. So, SGX toolchain (Driver, PSW, SDK) needs to be installed on worker nodes, while SDK needs to be installed on the leader node for compiling the code. All are installed in `/opt/intel/`.

### Config for Spark

Please refer to https://rajasekarv.github.io/vega/ for `Getting started` part and `Executing an application` part. 

### Compile

Just type `make`
