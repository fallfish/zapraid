source ../common.sh

# please adapt this script if run on FEMU SSD; also, the device.cc in the source code needs changing (for zone capacity)
for mode in 3; do
  mkdir -p ./results/mode_${mode}
  json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.system_mode=$mode"
  for try in 0 1 2 3 4; do
    for qd in 1 2 4 8 16 32 64; do
       sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH LD_PRELOAD=${SPDK_DIR}/build/fio/spdk_bdev ${FIO_DIR}/fio ./conf/${qd}qd.fio > ./results/mode_${mode}/result_qd${qd}_try${try}
    done
  done
done
