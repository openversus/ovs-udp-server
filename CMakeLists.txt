cmake_minimum_required(VERSION 3.15)
set(CMAKE_TOOLCHAIN_FILE "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake" CACHE STRING "Vcpkg toolchain file")

project(rollback-server LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(BUILD_SHARED_LIBS OFF)

if(MSVC)
    #set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded")
endif()

if(WIN32)
    message(STATUS "Configuring for Windows")
elseif(APPLE)
    message(STATUS "Configuring for macOS")
elseif(UNIX)
    message(STATUS "Configuring for Linux")
endif()

find_package(asio REQUIRED)
find_package(CURL REQUIRED)
find_package(ZLIB REQUIRED)

include_directories(${ASIO_INCLUDE_DIR} include)

file(GLOB SOURCES src/*.cpp)
file(GLOB_RECURSE HEADERS include/*.h include/*.hpp)

add_executable(rollback-server ${SOURCES} ${HEADERS})
if(MSVC)
set_property(TARGET rollback-server PROPERTY
  MSVC_RUNTIME_LIBRARY "MultiThreaded")
endif()

source_group(
  TREE "${CMAKE_CURRENT_SOURCE_DIR}/include"
  PREFIX "Header Files"
  FILES ${HEADERS}
)

source_group(
  TREE "${CMAKE_CURRENT_SOURCE_DIR}/src"
  PREFIX "Source Files"
  FILES ${SOURCES}
)

target_link_libraries(rollback-server
    PRIVATE
        CURL::libcurl
        ZLIB::ZLIB
)
