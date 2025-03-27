cmake_minimum_required(VERSION 3.10)
project(RedkaTalk LANGUAGES CXX)

# Set C++ standard (adjust if needed)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
add_compile_options(-fsanitize=address)
add_link_options(-fsanitize=address)
# For clangd syntax highlighting to work properly
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Add the executable (your main application file)
file(GLOB_RECURSE SOURCES "${CMAKE_SOURCE_DIR}/src/*.cpp")

add_executable(RedkaTalk ${SOURCES})

# Enable AVX and AVX2 support for this target (works for GCC/Clang)
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(RedkaTalk PRIVATE -mavx -mavx2 -march=native)
endif()

# Add the uuid_v4 library as a subdirectory.
# Assuming you've added the uuid_v4 library in the "external/uuid_v4" directory.
add_subdirectory(external/uuid_v4)

# Link the uuid_v4 target with your executable.
target_link_libraries(RedkaTalk PRIVATE uuid_v4::uuid_v4)
