# Step 1: Start Target
# Pay attention: for real applications, the json file has "event_framework" enabled; please refer to the conf/ directory
source ../common.sh

scheme=$1
sudo stdbuf -oL ${SPDK_DIR}/build/bin/spdk_tgt -m 0x1fc -c ./conf/zns_raid_${scheme}.json
