# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

######## SGX SDK Settings ########

SGX_SDK ?= /opt/intel/sgxsdk
SGX_MODE ?= HW
SGX_ARCH ?= x64

TOP_DIR := ./incubator-teaclave-sgx-sdk
APP_DIR := app
SPARK_CORE_DIR := Submodule-of-SecureSpark
include $(TOP_DIR)/buildenv.mk

ifeq ($(shell getconf LONG_BIT), 32)
	SGX_ARCH := x86
else ifeq ($(findstring -m32, $(CXXFLAGS)), -m32)
	SGX_ARCH := x86
endif


ifeq ($(SGX_ARCH), x86)
	SGX_COMMON_CFLAGS := -m32
	SGX_LIBRARY_PATH := $(SGX_SDK)/lib
	SGX_ENCLAVE_SIGNER := $(SGX_SDK)/bin/x86/sgx_sign
	SGX_EDGER8R := $(SGX_SDK)/bin/x86/sgx_edger8r
else
	SGX_COMMON_CFLAGS := -m64
	#SGX_COMMON_CFLAGS := -m64 -ggdb
	SGX_LIBRARY_PATH := $(SGX_SDK)/lib64
	SGX_ENCLAVE_SIGNER := $(SGX_SDK)/bin/x64/sgx_sign
	SGX_EDGER8R := $(SGX_SDK)/bin/x64/sgx_edger8r
endif

ifeq ($(SGX_DEBUG), 1)
ifeq ($(SGX_PRERELEASE), 1)
$(error Cannot set SGX_DEBUG and SGX_PRERELEASE at the same time!!)
endif
endif

ifeq ($(SGX_DEBUG), 1)
	SGX_COMMON_CFLAGS += -O0 -g
else
	SGX_COMMON_CFLAGS += -O2
endif

SGX_COMMON_CFLAGS += -fstack-protector

######## CUSTOM Settings ########

CUSTOM_LIBRARY_PATH := ./lib
CUSTOM_BIN_PATH := ./bin
CUSTOM_EDL_PATH := $(TOP_DIR)/edl
CUSTOM_COMMON_PATH := $(TOP_DIR)/common

######## EDL Settings ########

Enclave_EDL_Files := enclave/Enclave_t.c enclave/Enclave_t.h $(APP_DIR)/Enclave_u.c $(APP_DIR)/Enclave_u.h

######## APP Settings ########

App_Rust_Flags := --release
#App_Rust_Flags := 
App_SRC_Files := $(shell find $(APP_DIR)/ -type f -name '*.rs') $(shell find $(APP_DIR)/ -type f -name 'Cargo.toml') $(shell find $(SPARK_CORE_DIR)/ -type f -name '*.rs')
App_Include_Paths := -I $(APP_DIR) -I./include -I$(SGX_SDK)/include -I$(CUSTOM_EDL_PATH)
App_C_Flags := $(SGX_COMMON_CFLAGS) -fPIC -Wno-attributes $(App_Include_Paths)

App_Rust_Path := $(APP_DIR)/target/release
#App_Rust_Path := $(APP_DIR)/target/debug
App_Enclave_u_Object := $(APP_DIR)/libEnclave_u.a
App_Name := bin/app

######## Enclave Settings ########

ifneq ($(SGX_MODE), HW)
	Trts_Library_Name := sgx_trts_sim
	Service_Library_Name := sgx_tservice_sim
else
	Trts_Library_Name := sgx_trts
	Service_Library_Name := sgx_tservice
endif
Tcmalloc_Flag := sgx_tcmalloc

Crypto_Library_Name := sgx_tcrypto
KeyExchange_Library_Name := sgx_tkey_exchange
ProtectedFs_Library_Name := sgx_tprotected_fs

RustEnclave_C_Files := $(wildcard ./enclave/*.c)
RustEnclave_C_Objects := $(RustEnclave_C_Files:.c=.o)
RustEnclave_Include_Paths := -I$(CUSTOM_COMMON_PATH)/inc -I$(CUSTOM_EDL_PATH) -I$(SGX_SDK)/include -I$(SGX_SDK)/include/tlibc -I./enclave -I./include -I$(SGX_SDK)/include/libcxx

RustEnclave_Link_Libs := -L$(CUSTOM_LIBRARY_PATH) -lenclave
RustEnclave_Compile_Flags := $(SGX_COMMON_CFLAGS) $(ENCLAVE_CFLAGS) $(RustEnclave_Include_Paths)
RustEnclave_Link_Flags := -Wl,--no-undefined -nostdlib -nodefaultlibs -nostartfiles -L$(SGX_LIBRARY_PATH) \
	-Wl,--whole-archive -l$(Trts_Library_Name) -Wl,--no-whole-archive \
	-Wl,--start-group -lsgx_tstdc -lsgx_tcxx -lsgx_pthread -l$(Service_Library_Name) -l$(Crypto_Library_Name) $(RustEnclave_Link_Libs) -Wl,--end-group \
	-Wl,--version-script=enclave/Enclave.lds \
	$(ENCLAVE_LDFLAGS)

RustEnclave_Name := enclave/enclave.so
Signed_RustEnclave_Name := bin/enclave.signed.so

TCMALLOC_Default_Include_Paths := -I./enclave/gperftools
TCMALLOC_Include_Paths := $(TCMALLOC_Default_Include_Paths) -I$(SGX_SDK)/include -I$(SGX_SDK)/include/tlibc -I$(SGX_SDK)/include/libcxx -IEnclave

am__append_2 = -Wall -Wwrite-strings -Woverloaded-virtual

am__append_3 = -fno-builtin -fno-builtin-function
am__append_5 = -Wno-unused-result
am__append_8 = -DNO_FRAME_POINTER

TCMALLOC_CFlags := -Wall -DHAVE_CONFIG_H -DNO_TCMALLOC_SAMPLES -DNDEBUG -DNO_HEAP_CHECK -DTCMALLOC_SGX -DTCMALLOC_NO_ALIASES -fstack-protector -ffreestanding -nostdinc -fvisibility=hidden -fPIC $(am__append_2) $(am__append_3) $(am__append_5) $(am__append_8)

SYSTEM_ALLOC_CC = ./enclave/gperftools/system-alloc.cc
libtcmalloc_minimal_internal_la_SOURCES = ./enclave/gperftools/common.cc \
                                          ./enclave/gperftools/internal_logging.cc \
                                          $(SYSTEM_ALLOC_CC) \
                                          ./enclave/gperftools/central_freelist.cc \
                                          ./enclave/gperftools/page_heap.cc \
                                          ./enclave/gperftools/sampler.cc \
                                          ./enclave/gperftools/span.cc \
                                          ./enclave/gperftools/stack_trace_table.cc \
                                          ./enclave/gperftools/static_vars.cc \
                                          ./enclave/gperftools/symbolize.cc \
                                          ./enclave/gperftools/thread_cache.cc \
                                          ./enclave/gperftools/malloc_hook.cc \
                                          ./enclave/gperftools/maybe_threads.cc \
										  ./enclave/gperftools/malloc_extension.cc \
										  ./enclave/gperftools/tcmalloc.cc

TCMALLOC_Objects := $(libtcmalloc_minimal_internal_la_SOURCES:.cc=.o) enclave/gperftools/base/spinlock.o enclave/gperftools/base/sgx_utils.o
Break_Objests := $(wildcard ./lib/*.o)

.PHONY: all
all: $(App_Name) $(Signed_RustEnclave_Name)

######## EDL Objects ########

$(Enclave_EDL_Files): $(SGX_EDGER8R) enclave/Enclave.edl
	$(SGX_EDGER8R) --trusted enclave/Enclave.edl --search-path $(SGX_SDK)/include --search-path $(TOP_DIR)/edl --trusted-dir enclave
	$(SGX_EDGER8R) --untrusted enclave/Enclave.edl --search-path $(SGX_SDK)/include --search-path $(TOP_DIR)/edl --untrusted-dir $(APP_DIR)
	@echo "GEN  =>  $(Enclave_EDL_Files)"

######## App Objects ########

$(APP_DIR)/Enclave_u.o: $(Enclave_EDL_Files)
	@$(CC) $(App_C_Flags) -c $(APP_DIR)/Enclave_u.c -o $@
	@echo "CC   <=  $<"

$(App_Enclave_u_Object): $(APP_DIR)/Enclave_u.o
	$(AR) rcsD $@ $^
	cp $(App_Enclave_u_Object) ./lib

$(App_Name): $(App_Enclave_u_Object) $(App_SRC_Files)
	@cd $(APP_DIR) && SGX_SDK=$(SGX_SDK) cargo build $(App_Rust_Flags)
	@echo "Cargo  =>  $@"
	mkdir -p bin
	cp $(App_Rust_Path)/app ./bin

######## enclave/Tcmalloc Objects ########

#For TCMALLOC
enclave/gperftools/base/sgx_utils.o: enclave/gperftools/base/sgx_utils.cc
	@$(CXX) $(SGX_COMMON_CFLAGS) $(TCMALLOC_CFlags) $(TCMALLOC_Include_Paths) -c $< -o $@
	@echo "CXX  <= $<"

enclave/gperftools/base/spinlock.o: enclave/gperftools/base/spinlock.cc
	@$(CXX) $(SGX_COMMON_CFLAGS) $(TCMALLOC_CFlags) $(TCMALLOC_Include_Paths) -c $< -o $@
	@echo "CXX  <= $<"

enclave/gperftools/maybe_threads.o: enclave/gperftools/maybe_threads.cc
	@$(CXX) $(SGX_COMMON_CFLAGS) $(TCMALLOC_CFlags) $(TCMALLOC_Include_Paths) -c $< -o $@
	@echo "CXX  <= $<"

enclave/gperftools/common.o: enclave/gperftools/common.cc
	@$(CXX) $(SGX_COMMON_CFLAGS) $(TCMALLOC_CFlags) $(TCMALLOC_Include_Paths) -c $< -o $@
	@echo "CXX  <= $<"

enclave/gperftools/internal_logging.o: enclave/gperftools/internal_logging.cc
	@$(CXX) $(SGX_COMMON_CFLAGS) $(TCMALLOC_CFlags) $(TCMALLOC_Include_Paths) -c $< -o $@ 
	@echo "CXX  <= $<"

#OCALL is needed in system-alloc.cc
enclave/gperftools/system-alloc.o: enclave/gperftools/system-alloc.cc
	@$(CXX) $(RustEnclave_Compile_Flags) $(TCMALLOC_CFlags) $(TCMALLOC_Include_Paths) -c $< -o $@
	@echo "CXX  <= $<"

enclave/gperftools/central_freelist.o: enclave/gperftools/central_freelist.cc
	@$(CXX) $(SGX_COMMON_CFLAGS) $(TCMALLOC_CFlags) $(TCMALLOC_Include_Paths) -c $< -o $@
	@echo "CXX  <= $<"

enclave/gperftools/page_heap.o: enclave/gperftools/page_heap.cc
	@$(CXX) $(SGX_COMMON_CFLAGS) $(TCMALLOC_CFlags) $(TCMALLOC_Include_Paths) -c $< -o $@
	@echo "CXX  <= $<"

enclave/gperftools/sampler.o: enclave/gperftools/sampler.cc
	@$(CXX) $(SGX_COMMON_CFLAGS) $(TCMALLOC_CFlags) $(TCMALLOC_Include_Paths) -c $< -o $@
	@echo "CXX  <= $<"

enclave/gperftools/span.o: enclave/gperftools/span.cc
	@$(CXX) $(SGX_COMMON_CFLAGS) $(TCMALLOC_CFlags) $(TCMALLOC_Include_Paths) -c $< -o $@
	@echo "CXX  <= $<"

enclave/gperftools/stack_trace_table.o: enclave/gperftools/stack_trace_table.cc
	@$(CXX) $(SGX_COMMON_CFLAGS) $(TCMALLOC_CFlags) $(TCMALLOC_Include_Paths) -c $< -o $@
	@echo "CXX  <= $<"

enclave/gperftools/static_vars.o: enclave/gperftools/static_vars.cc
	@$(CXX) $(SGX_COMMON_CFLAGS) $(TCMALLOC_CFlags) $(TCMALLOC_Include_Paths) -c $< -o $@
	@echo "CXX  <= $<"

enclave/gperftools/symbolize.o: enclave/gperftools/symbolize.cc
	@$(CXX) $(SGX_COMMON_CFLAGS) $(TCMALLOC_CFlags) $(TCMALLOC_Include_Paths) -c $< -o $@
	@echo "CXX  <= $<"

enclave/gperftools/thread_cache.o: enclave/gperftools/thread_cache.cc
	@$(CXX) $(SGX_COMMON_CFLAGS) $(TCMALLOC_CFlags) $(TCMALLOC_Include_Paths) -c $< -o $@
	@echo "CXX  <= $<"

enclave/gperftools/malloc_hook.o: enclave/gperftools/malloc_hook.cc
	@$(CXX) $(SGX_COMMON_CFLAGS) $(TCMALLOC_CFlags) $(TCMALLOC_Include_Paths) -c $< -o $@
	@echo "CXX  <= $<"

enclave/gperftools/malloc_extension.o: enclave/gperftools/malloc_extension.cc
	@$(CXX) $(SGX_COMMON_CFLAGS) $(TCMALLOC_CFlags) $(TCMALLOC_Include_Paths) -c $< -o $@
	@echo "CXX  <= $<"

enclave/gperftools/tcmalloc.o: enclave/gperftools/tcmalloc.cc
	@$(CXX) $(SGX_COMMON_CFLAGS) $(TCMALLOC_CFlags) $(TCMALLOC_Include_Paths) -c $< -o $@
	@echo "CXX  <= $<"

TCMALLOC_name := libsgx_ucmalloc.a
$(TCMALLOC_name): $(TCMALLOC_Objects)
	$(AR) rcsD $@ $^
	mv $@ $(CUSTOM_LIBRARY_PATH)

######## Enclave Objects ########

enclave/Enclave_t.o: $(Enclave_EDL_Files)
	@$(CC) $(RustEnclave_Compile_Flags) -c enclave/Enclave_t.c -o $@
	@echo "CC   <=  $<"

$(RustEnclave_Name): enclave enclave/Enclave_t.o $(TCMALLOC_name)
	@rm -rf tcmalloc_objs enclave_objs
	@mkdir tcmalloc_objs && ar x $(CUSTOM_LIBRARY_PATH)/$(TCMALLOC_name) && mv *.o tcmalloc_objs
	@mkdir enclave_objs && ar x $(CUSTOM_LIBRARY_PATH)/libenclave.a && mv *.o enclave_objs
	@$(AR) rcs $(CUSTOM_LIBRARY_PATH)/libenclave.a tcmalloc_objs/*.o enclave_objs/*.o
	@rm -rf tcmalloc_objs enclave_objs
	@$(CXX) enclave/Enclave_t.o -o $@ $(RustEnclave_Link_Flags) # -Wl,--allow-multiple-definition
	@echo "LINK =>  $@"

$(Signed_RustEnclave_Name): $(RustEnclave_Name)
	mkdir -p bin
	@$(SGX_ENCLAVE_SIGNER) sign -key enclave/Enclave_private.pem -enclave $(RustEnclave_Name) -out $@ -config enclave/Enclave.config.xml
	@echo "SIGN =>  $@"

.PHONY: enclave
enclave:
	$(MAKE) -C ./enclave/


.PHONY: clean
clean:
	@rm -f $(App_Name) $(RustEnclave_Name) $(Signed_RustEnclave_Name) enclave/*_t.* $(APP_DIR)/*_u.* lib/*.a
	@cd enclave && cargo clean && rm -f Cargo.lock
	@cd $(APP_DIR) && cargo clean && rm -f Cargo.lock
	@cd $(SPARK_CORE_DIR) && cargo clean && rm -f Cargo.lock
	@cd lib && rm -rf *.a
	@rm -rf $(TCMALLOC_Objects)
