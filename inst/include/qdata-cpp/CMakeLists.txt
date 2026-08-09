cmake_minimum_required(VERSION 3.18)

project(qdata_cpp VERSION 1.1.0 LANGUAGES CXX)

if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME AND NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the build type." FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS Debug Release MinSizeRel RelWithDebInfo)
endif()

option(QDATA_USE_TBB "Link oneTBB for multithreaded qdata/io consumers" ON)
option(QDATA_BUILD_BENCHMARKS "Build qdata-cpp benchmarks" ${QDATA_USE_TBB})

find_package(PkgConfig QUIET)

set(QDATA_ZSTD_TARGET "")

find_package(zstd CONFIG QUIET)
if(TARGET zstd::libzstd_shared)
    set(QDATA_ZSTD_TARGET zstd::libzstd_shared)
elseif(TARGET zstd::libzstd_static)
    set(QDATA_ZSTD_TARGET zstd::libzstd_static)
elseif(TARGET zstd::libzstd)
    set(QDATA_ZSTD_TARGET zstd::libzstd)
elseif(PkgConfig_FOUND)
    pkg_check_modules(ZSTD IMPORTED_TARGET libzstd)
    if(TARGET PkgConfig::ZSTD)
        set(QDATA_ZSTD_TARGET PkgConfig::ZSTD)
    endif()
endif()

if(NOT QDATA_ZSTD_TARGET)
    message(FATAL_ERROR "zstd not found. Install zstd or expose it via CMake config or pkg-config.")
endif()

set(QDATA_TBB_TARGET "")

if(QDATA_USE_TBB)
    find_package(TBB CONFIG QUIET)
    if(TARGET TBB::tbb)
        set(QDATA_TBB_TARGET TBB::tbb)
    elseif(PkgConfig_FOUND)
        pkg_check_modules(TBB IMPORTED_TARGET tbb)
        if(TARGET PkgConfig::TBB)
            set(QDATA_TBB_TARGET PkgConfig::TBB)
        endif()
    endif()
    if(NOT QDATA_TBB_TARGET)
        message(FATAL_ERROR
            "QDATA_USE_TBB=ON requires oneTBB 2021.1 or newer. Install oneTBB, "
            "or configure with -DQDATA_USE_TBB=OFF for a single-threaded build.")
    endif()
endif()

# qdata-cpp uses the oneTBB flow-graph API (input_node, tbb::flow_control).
# Probe for it here so an old TBB fails while it is still obvious why, rather
# than part way through a template instantiation in multithreaded_block_module.h.
if(QDATA_TBB_TARGET)
    include(CMakePushCheckState)
    include(CheckCXXSourceCompiles)
    cmake_push_check_state(RESET)
    set(CMAKE_REQUIRED_LIBRARIES ${QDATA_TBB_TARGET})
    set(CMAKE_CXX_STANDARD 17)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
    set(CMAKE_REQUIRED_QUIET ON)
    check_cxx_source_compiles("
        #include <tbb/flow_graph.h>
        int main() {
            tbb::flow::graph g;
            tbb::flow::input_node<int> n(g, [](tbb::flow_control & fc) { fc.stop(); return 0; });
            return 0;
        }
    " QDATA_TBB_HAS_INPUT_NODE)
    cmake_pop_check_state()

    if(NOT QDATA_TBB_HAS_INPUT_NODE)
        message(FATAL_ERROR
            "The TBB found does not provide tbb::flow::input_node. qdata-cpp "
            "requires oneTBB 2021.1 or newer (TBB_INTERFACE_VERSION >= 12000); "
            "classic TBB (2020.3 and earlier) is not supported. Install a newer "
            "TBB, or configure with -DQDATA_USE_TBB=OFF to build single-threaded.")
    endif()
endif()

add_library(qdata INTERFACE)
add_library(qdata::qdata ALIAS qdata)
target_compile_features(qdata INTERFACE cxx_std_17)
target_compile_options(qdata
    INTERFACE
    $<$<CXX_COMPILER_ID:MSVC>:/bigobj>
)
target_include_directories(qdata
    INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)
target_link_libraries(qdata INTERFACE ${QDATA_ZSTD_TARGET})

if(QDATA_TBB_TARGET)
    target_link_libraries(qdata INTERFACE ${QDATA_TBB_TARGET})
    target_compile_definitions(qdata INTERFACE QIO_HAS_TBB=1)
endif()

option(QDATA_BUILD_EXAMPLES "Build qdata-cpp examples" ON)
option(QDATA_BUILD_TESTS "Build qdata-cpp tests" ON)

if(QDATA_BUILD_EXAMPLES)
    add_executable(qdata_mtcars_roundtrip examples/mtcars_roundtrip.cpp)
    target_compile_features(qdata_mtcars_roundtrip PRIVATE cxx_std_17)
    target_link_libraries(qdata_mtcars_roundtrip PRIVATE qdata)
endif()

if(QDATA_BUILD_BENCHMARKS)
    if(NOT QDATA_TBB_TARGET)
        message(FATAL_ERROR
            "QDATA_BUILD_BENCHMARKS=ON requires QDATA_USE_TBB=ON and oneTBB 2021.1 or newer.")
    endif()
    add_executable(qdata_benchmark benchmarks/qdata_benchmark.cpp)
    target_compile_features(qdata_benchmark PRIVATE cxx_std_17)
    target_link_libraries(qdata_benchmark PRIVATE qdata)
endif()

if(QDATA_BUILD_TESTS)
    enable_testing()

    add_executable(qdata_roundtrip_file tests/cpp/roundtrip_file.cpp)
    target_compile_features(qdata_roundtrip_file PRIVATE cxx_std_17)
    target_link_libraries(qdata_roundtrip_file PRIVATE qdata)

    add_executable(qdata_roundtrip_memory tests/cpp/roundtrip_memory.cpp)
    target_compile_features(qdata_roundtrip_memory PRIVATE cxx_std_17)
    target_link_libraries(qdata_roundtrip_memory PRIVATE qdata)

    add_executable(qdata_write_builtin_support tests/cpp/write_builtin_support.cpp)
    target_compile_features(qdata_write_builtin_support PRIVATE cxx_std_17)
    target_link_libraries(qdata_write_builtin_support PRIVATE qdata)

    add_executable(qdata_buffer_api tests/cpp/buffer_api.cpp)
    target_compile_features(qdata_buffer_api PRIVATE cxx_std_17)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(qdata_buffer_api PRIVATE -fno-elide-constructors)
    endif()
    target_link_libraries(qdata_buffer_api PRIVATE qdata)
    add_test(NAME qdata_buffer_api COMMAND qdata_buffer_api)
    set_tests_properties(qdata_buffer_api PROPERTIES TIMEOUT 120)

    add_executable(qdata_compat_limits tests/cpp/compat_limits.cpp)
    target_compile_features(qdata_compat_limits PRIVATE cxx_std_17)
    target_link_libraries(qdata_compat_limits PRIVATE qdata)
    add_test(
        NAME qdata_compat_limits
        COMMAND qdata_compat_limits ${CMAKE_CURRENT_BINARY_DIR}/invalid_save_destination.qdata
    )
    set_tests_properties(qdata_compat_limits PROPERTIES TIMEOUT 120)

    add_executable(qdata_io_regressions tests/cpp/io_regressions.cpp)
    target_compile_features(qdata_io_regressions PRIVATE cxx_std_17)
    target_link_libraries(qdata_io_regressions PRIVATE qdata)
    add_test(NAME qdata_io_regressions COMMAND qdata_io_regressions)
    set_tests_properties(qdata_io_regressions PROPERTIES TIMEOUT 120)

    find_program(RSCRIPT_EXECUTABLE Rscript REQUIRED)
    execute_process(
        COMMAND ${RSCRIPT_EXECUTABLE} -e
                "pkgs <- c('qs2', 'stringi', 'stringfish'); cat(as.integer(all(vapply(pkgs, requireNamespace, logical(1), quietly = TRUE))))"
        OUTPUT_VARIABLE QDATA_HAS_R_TEST_DEPS
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    if(NOT QDATA_HAS_R_TEST_DEPS STREQUAL "1")
        message(FATAL_ERROR "QDATA_BUILD_TESTS=ON requires Rscript with the qs2, stringi, and stringfish packages installed.")
    endif()

    add_test(
        NAME qdata_builtin_write_support
        COMMAND ${RSCRIPT_EXECUTABLE}
                ${CMAKE_CURRENT_SOURCE_DIR}/tests/r/builtin_write_support.R
                $<TARGET_FILE:qdata_write_builtin_support>
                ${CMAKE_CURRENT_BINARY_DIR}/builtin_cases
    )
    set_tests_properties(qdata_builtin_write_support PROPERTIES TIMEOUT 600)
    add_test(
        NAME qdata_roundtrip_qs2
        COMMAND ${RSCRIPT_EXECUTABLE}
                ${CMAKE_CURRENT_SOURCE_DIR}/tests/r/roundtrip_testing.R
                $<TARGET_FILE:qdata_roundtrip_file>
                $<TARGET_FILE:qdata_roundtrip_memory>
    )
endif()
