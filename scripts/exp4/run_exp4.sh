source ../common.sh

json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.sync_group_size=256"
for try in 0 1 2 3 4; do
  for mode in 0 2; do
    json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.system_mode=${mode}"
    for qd in 16; do
      mkdir -p results/mode_${mode}
      # RAID 0
      json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_data_blocks=4"
      json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_parity_blocks=0"
      json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.raid_level=0"
      sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH LD_PRELOAD=${SPDK_DIR}build/fio/spdk_bdev ${FIO_DIR}/fio ./conf/${qd}qd.fio > ./results/mode_${mode}/result_raid0_try_${try}

      # RAID 01
      json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_data_blocks=2"
      json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_parity_blocks=2"
      json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.raid_level=5"
      sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH LD_PRELOAD=${SPDK_DIR}build/fio/spdk_bdev ${FIO_DIR}/fio ./conf/${qd}qd.fio > ./results/mode_${mode}/result_raid01_try_${try}

     # RAID 4
      json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_data_blocks=3"
      json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_parity_blocks=1"
      json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.raid_level=2"
      sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH LD_PRELOAD=${SPDK_DIR}build/fio/spdk_bdev ${FIO_DIR}/fio ./conf/${qd}qd.fio > ./results/mode_${mode}/result_raid4_try_${try}

      # RAID 5
      json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_data_blocks=3"
      json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_parity_blocks=1"
      json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.raid_level=3"
      sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH LD_PRELOAD=${SPDK_DIR}build/fio/spdk_bdev ${FIO_DIR}/fio ./conf/${qd}qd.fio > ./results/mode_${mode}/result_raid5_try_${try}

      # RAID 6
      json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_data_blocks=2"
      json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.num_parity_blocks=2"
      json -I -f zns_raid.json -e "this.subsystems[0].config[0].params.raid_level=4"
      sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH LD_PRELOAD=${SPDK_DIR}build/fio/spdk_bdev ${FIO_DIR}/fio ./conf/${qd}qd.fio > ./results/mode_${mode}/result_raid6_try_${try}
    done
  done
done
