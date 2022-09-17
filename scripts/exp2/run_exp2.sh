source ../common.sh

json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.inject_degraded_read=0"
for workload in read_only; do
  for mode in 0; do
    mkdir -p ./results/mode_${mode}
    json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.system_mode=$mode"
    sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH LD_PRELOAD=${SPDK_DIR}/build/fio/spdk_bdev ${FIO_DIR}/fio ./conf/${workload}.fio | tee ./results/result_${workload}_static_mapping
  done
done

json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.inject_degraded_read=0"
for workload in read_only; do
  for mode in 3; do
    mkdir -p ./results/mode_${mode}
    json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.system_mode=$mode"
    sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH LD_PRELOAD=${SPDK_DIR}/build/fio/spdk_bdev ${FIO_DIR}/fio ./conf/${workload}.fio | tee ./results/result_${workload}_normal_read
  done
done

json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.inject_degraded_read=1"
for workload in read_only; do
  mode=3
  mkdir ./results/mode_${mode}
  json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.system_mode=$mode"
  sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH LD_PRELOAD=${SPDK_DIR}/build/fio/spdk_bdev ${FIO_DIR}/fio ./conf/${workload}.fio | tee ./results/mode_${mode}/result_degraded_read
done
