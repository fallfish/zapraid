# Step 2: create NVMe-oF transport

sudo ${SPDK_DIR}/scripts/rpc.py nvmf_create_transport -t TCP -u 16384 -m 1 -c 8192
sudo ${SPDK_DIR}/scripts/rpc.py nvmf_create_subsystem nqn.2016-06.io.spdk:cnode0 -a -s SPDK00000000000001 -d SPDK_Controller1
sudo ${SPDK_DIR}/scripts/rpc.py nvmf_subsystem_add_ns nqn.2016-06.io.spdk:cnode0 ZnsRaid0
sudo ${SPDK_DIR}/scripts/rpc.py nvmf_subsystem_add_listener nqn.2016-06.io.spdk:cnode0 -t tcp -a 127.0.0.1 -s 4420
sudo nvme discover -t tcp -a 127.0.0.1 -s 4420
sudo nvme connect -t tcp -n "nqn.2016-06.io.spdk:cnode0" -a 127.0.0.1 -s 4420 --nr-io-queues 1 -k 0 -v
sudo nvme list
