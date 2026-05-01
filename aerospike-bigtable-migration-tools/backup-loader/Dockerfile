# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ----------- Stage 1: builder -----------
FROM ubuntu:22.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

# Update and install dependencies
RUN apt-get update -y && apt-get install -y --no-install-recommends 
    build-essential 
    libssl-dev 
    libzstd-dev 
    libpsl-dev 
    cmake 
    pkg-config 
    zlib1g-dev 
    autotools-dev 
    automake 
    libtool 
    git 
    openjdk-17-jdk 
    maven 
    curl 
    libuv1-dev 
    libcurl4-openssl-dev 
    libjansson-dev

WORKDIR /build

# Build AWS SDK for C++ (needed by aerospike-tools-backup)
RUN git clone https://github.com/aws/aws-sdk-cpp.git --branch 1.11.210 --depth 1 --recurse-submodules -j"$(nproc)" --shallow-submodules
RUN cd aws-sdk-cpp && 
    mkdir build && 
    cmake -S . -B build 
        -DCMAKE_BUILD_TYPE=Release 
        -DBUILD_ONLY="s3" 
        -DBUILD_SHARED_LIBS=ON 
        -DENABLE_TESTING=OFF 
        -DCMAKE_INSTALL_PREFIX=/usr/local 
        -DCMAKE_INSTALL_LIBDIR=lib 
        -DENABLE_UNITY_BUILD=ON && 
    make -j$(nproc) -C build && make install -C build

# Clone and build Aerospike backup tools (provides headers and asrestore.so)
RUN git clone https://github.com/aerospike/aerospike-tools-backup.git /app --branch 4.5.1 --depth 1 --recurse-submodules -j"$(nproc)" --shallow-submodules
WORKDIR /app
RUN CPPFLAGS="-g -Derr=aerospike_backup_err_func__" 
    EVENT_LIB=libuv 
    CC="gcc" 
    CXX="g++" 
    make -f - shared all -j$(nproc) <<EOF
include Makefile
CFLAGS += \$(CPPFLAGS)
CXXFLAGS += \$(CPPFLAGS)
EOF

WORKDIR /src

# Copy the whole project to handle inter-module dependencies (backup-loader depends on adapter)
COPY . .

# Build the project
# This will:
# 1. Compile adapter and install it to local mvn repo
# 2. Compile backup-loader, which triggers JNI compilation (writing to /usr/lib/libbackupreader.so)
# 3. Create the fat jar (jar-with-dependencies)
RUN mvn clean install -pl adapter,backup-loader -am -DskipTests

# Bundling the native libraries into the fat jar (optional but often desired for "fat" jars)
# We place them in the target/classes before running assembly again, or we can just append them.
# However, BackupReader.java expects them in java.library.path.
# For a true fat jar that works out of the box, one would need a custom loader.
# Here we just ensure the fat jar is produced as requested.

# The fat jar will be located at:
# backup-loader/target/backup-loader-1.0.0-SNAPSHOT-jar-with-dependencies.jar

# Stage 2: final (to keep only the artifact)
FROM scratch AS artifact
COPY --from=builder /src/backup-loader/target/backup-loader-1.0.0-SNAPSHOT-jar-with-dependencies.jar /backup-loader-fat.jar
COPY --from=builder /usr/lib/libbackupreader.so /libbackupreader.so
COPY --from=builder /usr/lib/libasrestore.so /libasrestore.so
