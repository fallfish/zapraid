set( CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -fPIC" )
# set( CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -fPIC -fsanitize=address -fsanitize=undefined -g -static-libasan" )

add_library(zns_raid raid_controller.cc device.cc zone.cc segment.cc common.cc poller.cc zns_raid.cc persistent_metadata.cc)
# add_library(zns_raid SHARED raid_controller.cc device.cc zone.cc zone_group.cc common.cc poller.cc zns_raid.cc)
# add_library(bdev_zns_raid bdev/bdev_zns_raid.c bdev/bdev_zns_raid_rpc.c)

find_package(PkgConfig)
pkg_search_module(SPDK_NVME REQRUIED spdk_nvme)
pkg_search_module(SPDK_NVMF REQRUIED spdk_nvmf)
pkg_search_module(SPDK_BDEV REQRUIED spdk_bdev)
pkg_search_module(SPDK_EVENT REQRUIED spdk_event)
pkg_search_module(SPDK_THREAD REQRUIED spdk_thread)
pkg_search_module(SPDK_UTIL REQRUIED spdk_util)
pkg_search_module(SPDK_TRACE REQRUIED spdk_trace)
pkg_search_module(SPDK_ENV REQRUIED spdk_env_dpdk)

target_include_directories(zns_raid SYSTEM PUBLIC ${SPDK_NVME_INCLUDE_DIRS} /home/ubuntu/spdk/dpdk/build/include)
# target_link_libraries(raid spdk spdk_env_dpdk -Wl,--no-whole-archive -Wl,-Bdynamic ${SPDK_ENV_LINK_LIBRARIES} pthread uuid isal rt numa)
target_link_libraries(zns_raid -Wl,--whole-archive -Wl,--no-as-needed -Wl,-Bstatic ${SPDK_NVME_LINK_LIBRARIES} ${SPDK_EVENT_LINK_LIBRARIES} isal -Wl,--no-whole-archive -Wl,-Bdynamic ${SPDK_ENV_LINK_LIBRARIES} ${SPDK_THREAD_LINK_LIBRARIES} pthread uuid rt)

add_executable(app main.cpp)
target_link_libraries(app zns_raid isal -Wl,--no-as-needed ${SPDK_NVME_LINK_LIBRARIES} ${SPDK_EVENT_LINK_LIBRARIES} ${SPDK_ENV_LINK_LIBRARIES} ${SPDK_THREAD_LINK_LIBRARIES})
