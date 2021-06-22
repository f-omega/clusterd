cmake_minimum_required(VERSION 3.10)

project(clusterd)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS ON)

find_package(PkgConfig)
pkg_check_modules(LIBUV REQUIRED libuv)
pkg_check_modules(RAFT REQUIRED raft)
pkg_check_modules(NETFILTER_QUEUE REQUIRED libnetfilter_queue)
pkg_check_modules(MNL REQUIRED libmnl)
pkg_check_modules(NFTABLES REQUIRED libnftables)
pkg_check_modules(JANSSON REQUIRED jansson)

find_package(Lua REQUIRED)
find_package(SQLite3 REQUIRED)
find_package(Threads REQUIRED)

function(preprocess_rc out_var)
  set(result)
  foreach(in_f ${ARGN})
    set(out_f ${in_f})
    set(out_f "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_FILES_DIRECTORY}/${out_f}.c")
    get_filename_component(name_f ${in_f} NAME)
    string(REGEX REPLACE "[^A-Za-z0-9_]" "_" name_f ${name_f})
    add_custom_command(OUTPUT "${out_f}"
      COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tools/rc ${name_f} ${in_f} ${out_f}
      DEPENDS ${in_f}
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
      COMMENT "Creating preprocessed rc file ${out_f}"
      VERBATIM
    )
    list(APPEND result ${out_f})
  endforeach()
  set(${out_var} "${result}" PARENT_SCOPE)
endfunction()

set(CLUSTERD_CFLAGS ${LIBUV_CFLAGS} ${RAFT_CFLAGS} ${NETFILTER_QUEUE_CFLAGS} ${MNL_CFLAGS} -D_GNU_SOURCE)

add_compile_options(${CLUSTERD_CFLAGS})

include_directories(PRIVATE src/include)
include_directories(PUBLIC ${LUA_INCLUDE_DIR} ${SQLite3_INCLUDE_DIRS} ${NFTABLES_INCLUDE_DIRS} ${JANSSON_INCLUDE_DIRS})
add_executable(clusterd-monitor src/programs/clusterd-monitor.c)
target_link_libraries(clusterd-monitor ${LIBUV_LIBRARIES})

preprocess_rc(controller_rc src/programs/controller-schema.sql src/programs/json.lua
  src/programs/controller-api.lua)
add_executable(clusterd-controller src/programs/clusterd-controller.c src/programs/clusterd-db.c
  src/programs/sha256.c ${controller_rc})
target_link_libraries(clusterd-controller ${LIBUV_LIBRARIES} ${RAFT_LIBRARIES} ${LUA_LIBRARIES} ${SQLite3_LIBRARIES})

add_library(clusterctllib STATIC src/programs/libclusterctl.c src/programs/sha256.c)

add_executable(clusterctl src/programs/clusterctl.c)
target_link_libraries(clusterctl clusterctllib)

add_executable(clusterd-ns src/programs/clusterd-ns.c)
target_link_libraries(clusterd-ns clusterctllib)

add_executable(clusterd-service src/programs/clusterd-service.c)
target_link_libraries(clusterd-service clusterctllib)

add_executable(clusterd-host src/programs/clusterd-host.c src/programs/config.c)
target_link_libraries(clusterd-host clusterctllib ${LIBUV_LIBRARIES})

add_executable(clusterd-exec src/programs/clusterd-exec.c)
target_link_libraries(clusterd-exec clusterctllib)

add_executable(clusterd-ps src/programs/clusterd-ps.c)
target_link_libraries(clusterd-ps clusterctllib)

add_executable(clusterd-setuidgidmap src/programs/clusterd-setuidgidmap.c src/programs/config.c)

add_executable(clusterd-config src/programs/clusterd-config.c src/programs/config.c)

add_executable(clusterd-resource src/programs/clusterd-resource.c)
target_link_libraries(clusterd-resource clusterctllib)

add_executable(clusterd-nsportd src/programs/clusterd-nsportd.c)
target_link_libraries(clusterd-nsportd clusterctllib ${CMAKE_THREAD_LIBS_INIT} ${NETFILTER_QUEUE_LIBRARIES} ${MNL_LIBRARIES} ${NFTABLES_LIBRARIES})

add_executable(clusterd-schedule src/programs/clusterd-schedule.c)
target_link_libraries(clusterd-schedule clusterctllib ${CMAKE_THREAD_LIBS_INIT} ${JANSSON_LIBRARIES})

add_executable(clusterd-dns src/programs/clusterd-dns.c)
target_link_libraries(clusterd-dns clusterctllib ${CMAKE_THREAD_LIBS_INIT})
