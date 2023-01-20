cmake_minimum_required(VERSION 3.13)
project(aws-c)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/aws-c-common/cmake)
include(AwsFindPackage)
set(IN_SOURCE_BUILD ON)

add_subdirectory(aws-c-common)
if(UNIX AND NOT APPLE)
    # set(DISABLE_GO ON) # Build without using Go, we don't want the extra dependency
    # set(DISABLE_PERL ON) # Build without using Perl, we don't want the extra dependency
    # set(BUILD_LIBSSL OFF) # Don't need libssl, only need libcrypto
    # add_subdirectory(aws-lc)

    set(UNSAFE_TREAT_WARNINGS_AS_ERRORS OFF)
    add_subdirectory(s2n)
endif()
add_subdirectory(aws-c-cal)
add_subdirectory(aws-c-io)
add_subdirectory(aws-checksums)
add_subdirectory(aws-c-compression)
add_subdirectory(aws-c-event-stream)
add_subdirectory(aws-c-http)
add_subdirectory(aws-c-sdkutils)
add_subdirectory(aws-c-auth)
add_subdirectory(aws-c-mqtt)
add_subdirectory(aws-c-s3)
add_subdirectory(aws-c-iot)

add_subdirectory(s3perf)
