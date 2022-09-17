set( CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -fPIC -O3" )

add_library(zapraid raid_controller.cc device.cc zone.cc segment.cc common.cc messages_and_functions.cc zns_raid.cc)

set(SPDK_DIR "")

target_include_directories(zapraid SYSTEM PUBLIC ${SPDK_DIR}/build/include ${SPDK_DIR}/dpdk/build/include)

target_link_libraries(zapraid -L${SPDK_DIR}/build/lib -L${SPDK_DIR}/dpdk/build/lib
	-Wl,--whole-archive -Wl,--no-as-needed -Wl,-Bstatic 
	spdk_json spdk_jsonrpc spdk_rpc
	spdk_log spdk_sock spdk_util spdk_trace
	spdk_thread spdk_nvme
        spdk_init spdk_env_dpdk_rpc spdk_event
	-Wl,--no-whole-archive -Wl,-Bdynamic 
	spdk_env_dpdk rte_mempool rte_telemetry
	rte_eal rte_kvargs rte_pci rte_bus_pci rte_ring rte_mempool_ring
	pthread uuid rt isal dl)

add_executable(app main.cpp)
target_link_libraries(app zapraid) 

add_executable(simple_traffic_gen simple_traffic_gen.cc)
target_include_directories(simple_traffic_gen SYSTEM PUBLIC ${SPDK_DIR}/build/include ${SPDK_DIR}/dpdk/build/include)
target_link_libraries(simple_traffic_gen -L${SPDK_DIR}/build/lib -L${SPDK_DIR}/dpdk/build/lib
	-Wl,--whole-archive -Wl,--no-as-needed -Wl,-Bstatic 
	spdk_json spdk_jsonrpc spdk_rpc
	spdk_log spdk_sock spdk_util spdk_trace
	spdk_thread spdk_nvme
        spdk_init spdk_env_dpdk_rpc spdk_event
	-Wl,--no-whole-archive -Wl,-Bdynamic 
	spdk_env_dpdk rte_mempool rte_telemetry
	rte_eal rte_kvargs rte_pci rte_bus_pci rte_ring rte_mempool_ring
	pthread uuid rt isal dl)
