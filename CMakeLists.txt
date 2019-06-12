cmake_minimum_required(VERSION 3.8.2)

################################################################################
# See https://github.com/google/googletest/tree/master/googletest

# Download and unpack googletest at configure time
configure_file(CMakeLists.txt.in googletest-download/CMakeLists.txt)
execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" .
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/googletest-download" )
execute_process(COMMAND "${CMAKE_COMMAND}" --build .
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/googletest-download" )

# Prevent GoogleTest from overriding our compiler/linker options
# when building with Visual Studio
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

# Add googletest directly to our build. This adds
# the following targets: gtest, gtest_main
add_subdirectory("${CMAKE_BINARY_DIR}/googletest-src"
                 "${CMAKE_BINARY_DIR}/googletest-build")

# The gtest targets carry header search path dependencies 
# automatically when using CMake 2.8.11 or later. 

# Now simply link your own targets against gtest.
################################################################################


if ($ENV{TRAVIS_BUILD_NUMBER})
    project(cmd VERSION 1.0.$ENV{TRAVIS_BUILD_NUMBER})
else()
    project(cmd VERSION 1.0.0)
endif()

find_package(Threads)
find_package(Boost COMPONENTS system REQUIRED)
include_directories(${Boost_INCLUDE_DIR})

include_directories(util memstore)

add_executable(join_server 
                            main.cpp 
                            server.cpp 
                            util/util.cpp
                            memstore/memstore.cpp
                            memstore/parse.cpp
                            memstore/table.cpp
                            memstore/data_object.cpp)

set_target_properties(join_server PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
    COMPILE_OPTIONS "-Wpedantic;-Wall;-Wextra"
)

target_link_libraries(join_server ${CMAKE_THREAD_LIBS_INIT} ${Boost_LIBRARIES}) 


################################################################################
enable_testing()

add_executable(test_util 
                         util/util_test.cpp
                         util/util.cpp)

set_target_properties(test_util PROPERTIES
    CMAKE_CXX_STANDARD 17
    CMAKE_CXX_STANDARD_REQUIRED ON
    COMPILE_OPTIONS "-Wpedantic;-Wall;-Wextra")
    
target_link_libraries(test_util gtest)

add_test(util test_util)

################################################################################

install (TARGETS join_server RUNTIME DESTINATION bin)

################################################################################

set(CPACK_GENERATOR DEB)

set(CPACK_PACKAGE_NAME join_server)
set(CPACK_PACKAGE_VERSION "${PROJECT_VERSION}")
set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE amd64)
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "Anton Sochnev <an.sochnev@yandex.ru>")
set(CPACK_DEBIAN_PACKAGE_SECTION "homework")
set(CPACK_DEBIAN_PACKAGE_DESCRIPTION 
"join_server
 join_server."
)
set(CPACK_PACKAGE_FILE_NAME ${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION})

include(CPack)
