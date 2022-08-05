set( CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -fPIC -g -O3" )
# set( CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -fPIC -fsanitize=address -fsanitize=undefined -g -static-libasan" )

add_library(zns_raid raid_controller.cc device.cc zone.cc segment.cc common.cc poller.cc zns_raid.cc)
# add_library(zns_raid SHARED raid_controller.cc device.cc zone.cc zone_group.cc common.cc poller.cc zns_raid.cc)
# add_library(bdev_zns_raid bdev/bdev_zns_raid.c bdev/bdev_zns_raid_rpc.c)


target_include_directories(zns_raid SYSTEM PUBLIC /home/ubuntu/spdk/build/include /home/ubuntu/spdk/dpdk/build/include)

target_link_libraries(zns_raid -L/home/ubuntu/spdk/build/lib -L/home/ubuntu/spdk/dpdk/build/lib
	-Wl,--whole-archive -Wl,--no-as-needed -Wl,-Bstatic 
	spdk_json spdk_jsonrpc spdk_rpc
	spdk_log spdk_sock spdk_util spdk_trace
	spdk_thread spdk_nvme
	-Wl,--no-whole-archive -Wl,-Bdynamic 
	spdk_env_dpdk rte_mempool rte_telemetry
	rte_eal rte_kvargs rte_pci rte_bus_pci rte_ring rte_mempool_ring
	pthread uuid rt isal dl)

add_executable(app main.cpp)
target_link_libraries(app zns_raid isal) 
# rte_eal rte_kvargs rte_pci rte_bus_pci rte_ring
	#	-Wl,--no-whole-archive -Wl,-Bdynamic pthread uuid rt isal)

