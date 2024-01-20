cmake_minimum_required(VERSION 3.13 FATAL_ERROR)
project(pipashan VERSION 0.1 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)


add_executable(pipashan examples/example-test.cpp)

set(path_3rdparty ${PROJECT_SOURCE_DIR}/3rdparty)

### Find the boost library

find_path(boost_INCLUDE boost/asio.hpp HINTS	"${path_3rdparty}/boost"
												"${path_3rdparty}/boost_1_84_0"
												"${path_3rdparty}/boost_1_83_0"
												"${path_3rdparty}/boost_1_82_0"
												"${path_3rdparty}/boost_1_81_0"
												"${path_3rdparty}/boost_1_80_0")
if(boost_INCLUDE)
	message(STATUS "Found boost include at: ${boost_INCLUDE}")
else()
	message(FATAL_ERROR "Boost C++ Library is not found")
endif()

### Find json
find_path(json_INCLUDE json.hpp HINTS "${path_3rdparty}")
if(json_INCLUDE)
	message(STATUS "Found nlohmann::json include at: ${json_INCLUDE}")
else()
	message(FATAL_ERROR "nlohmann::json is not found. Download at https://github.com/nlohmann/json/blob/develop/single_include/nlohmann/json.hpp and put it in 3rdparty folder.")
endif()

target_include_directories(pipashan PUBLIC ./)

target_include_directories(pipashan PUBLIC ../../ ../ ${path_3rdparty} ${boost_INCLUDE})


if(WIN32)
	target_link_libraries(pipashan PUBLIC ssl crypto ws2_32 wsock32)
elseif(APPLE)
	#target_link_libraries(pipashan PUBLIC ssl crypto)
endif()