cmake_minimum_required(VERSION 3.13)
project(aws-c)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/aws-c-common/cmake)
include(AwsFindPackage)
set(IN_SOURCE_BUILD ON)

add_subdirectory(aws-c-common)
if(UNIX AND NOT APPLE)
    # don't build tests for s2n or aws-lc
    set(BUILD_TESTING_PREV ${BUILD_TESTING})
    set(BUILD_TESTING OFF)

    if(NOT USE_OPENSSL)
        set(BUILD_LIBSSL OFF) # only build libcrypto, don't build libssl
        set(DISABLE_GO ON) # don't run codegen
        set(DISABLE_PERL ON) # don't run codegen
        add_subdirectory(aws-lc)
    endif()

    set(UNSAFE_TREAT_WARNINGS_AS_ERRORS OFF) # disable warnings-as-errors when building s2n
    add_subdirectory(s2n)

    set(BUILD_TESTING ${BUILD_TESTING_PREV})
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
