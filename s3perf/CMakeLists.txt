cmake_minimum_required(VERSION 3.13)
project(s3perf CXX)


file(GLOB SRC_FILES
       "s3perf.cpp"
)

add_executable(${PROJECT_NAME} ${SRC_FILES})

set_target_properties(${PROJECT_NAME} PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON)

target_compile_options(${PROJECT_NAME} PRIVATE -Wall)

include(AwsFindPackage)
aws_use_package(aws-c-s3)
target_link_libraries(${PROJECT_NAME} PUBLIC ${DEP_AWS_LIBS})
