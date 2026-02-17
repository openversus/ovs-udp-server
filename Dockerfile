# ---- Build stage (AMD64) ----
FROM node:24-trixie AS builder

RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    cmake \
    git \
    curl \
    pkg-config \
    ninja-build \
    zip \
    unzip

RUN git clone --depth=1 https://github.com/microsoft/vcpkg /opt/vcpkg \
 && /opt/vcpkg/bootstrap-vcpkg.sh -disableMetrics

ENV VCPKG_ROOT=/opt/vcpkg

WORKDIR /src
COPY . /src

# (Optional) Install system libs if you need them
RUN apt-get install -y libssl-dev ninja-build generate-ninja

# Install vcpkg deps (manifest mode)
RUN /opt/vcpkg/vcpkg install --triplet x64-linux

# Build
RUN cmake -B build \
    -S . \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_TOOLCHAIN_FILE=/opt/vcpkg/scripts/buildsystems/vcpkg.cmake \
    -DVCPKG_BUILD_TYPE=release \
    -DVCPKG_TARGET_TRIPLET=x64-linux \
    -G Ninja \
 && cmake --build build

RUN strip /src/build/rollback-server

FROM gcr.io/distroless/static-debian13:nonroot AS artifact

# Copy only the built binary
COPY --from=builder /src/build/rollback-server /rollback-server

ARG OVS_SERVER=http://testing.openversus.org:8000
ENV OVS_SERVER=${OVS_SERVER}
EXPOSE 57000-58000

# Default to running the binary, but allow shell override
ENTRYPOINT ["/rollback-server"]
