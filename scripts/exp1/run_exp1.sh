# Need json tool
json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.sync_group_size=256"
for workload in write_only; do
  for try in 0 1 2 3 4; do
    for mode in 0 1 2; do
      mkdir -p ./results/mode${mode}
      json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.system_mode=$mode"
      sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH LD_PRELOAD=${SPDK_DIR}/build/fio/spdk_bdev ${FIO_DIR}/fio ./conf/${workload}.fio > ./results/mode${mode}/result_try${try}
    done
  done
done

cd ..
