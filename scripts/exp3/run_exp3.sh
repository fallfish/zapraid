source ../common.sh

for workload in write_only read_only; do
  mode=2
  json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.system_mode=$mode"
  mkdir -p ./results/mode_${mode}
  for group_size in 4 8 16 32 64 128 256 512 1024 2048 4096; do
    json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.sync_group_size=${group_size}"
    sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH LD_PRELOAD=${SPDK_DIR}/build/fio/spdk_bdev ${FIO_DIR}/fio ./conf/${workload}.fio | tee ./results/mode_${mode}/result_${workload}_${group_size}
  done
done

cd ..
