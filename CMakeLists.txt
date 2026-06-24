cmake_minimum_required(VERSION 3.16)
project(raw_hailo_inference CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_BUILD_TYPE Release CACHE STRING "")

# ── HailoRT ──────────────────────────────────────────────────────────────────
find_library(HAILORT_LIB NAMES hailort HINTS /usr/lib /usr/local/lib REQUIRED)
find_path(HAILORT_INCLUDE_DIR NAMES hailo/hailort.hpp
    HINTS /usr/include /usr/local/include)
if(NOT HAILORT_INCLUDE_DIR)
    message(FATAL_ERROR "HailoRT headers not found.")
endif()

message(STATUS "HailoRT lib     : ${HAILORT_LIB}")
message(STATUS "HailoRT headers : ${HAILORT_INCLUDE_DIR}")

# ── No FFmpeg needed — input is raw UDP YUV420, not RTSP/H.264 ───────────────

# ── Executable ───────────────────────────────────────────────────────────────
add_executable(raw_hailo_inference raw_hailo_inference.cpp)

target_include_directories(raw_hailo_inference PRIVATE
        picam-hailo_1_0_0_202606171434
    ${HAILORT_INCLUDE_DIR}
)

target_link_libraries(raw_hailo_inference PRIVATE
    ${HAILORT_LIB}
    pthread
)

target_compile_options(raw_hailo_inference PRIVATE
    -O3
    -march=native          # enables NEON SIMD on Pi 5 Cortex-A76
    -ffast-math
    -funroll-loops
    -Wall -Wextra -pthread
)
target_link_options(raw_hailo_inference PRIVATE -pthread)

install(TARGETS raw_hailo_inference DESTINATION /usr/bin)
install(FILES config.ini DESTINATION /usr/share/picam-hailo)
