#!/bin/bash
#
# Huaicheng Li <hcli@cmu.edu>
# Run FEMU as Zoned-Namespace (ZNS) SSDs
#

# Image directory
# IMGDIR=$HOME/images
# Virtual machine disk image
IMGDIR=""
# OSIMGF=$IMGDIR/u20s.qcow2

# if [[ ! -e "$OSIMGF" ]]; then
# 	echo ""
# 	echo "VM disk image couldn't be found ..."
# 	echo "Please prepare a usable VM image and place it as $OSIMGF"
# 	echo "Once VM disk image is ready, please rerun this script again"
# 	echo ""
# 	exit
# fi
#    -device virtio-scsi-pci,id=scsi0 \
#    -device scsi-hd,drive=hd0 \

sudo x86_64-softmmu/qemu-system-x86_64 \
    -name "FEMU-ZNSSD-VM" \
    -enable-kvm \
    -cpu host \
    -smp 8 \
    -m 32G \
    -drive file=$IMGDIR/zapraid.qcow2,format=qcow2 \
    -drive file=$IMGDIR/user-data.img,format=raw \
    -device femu,devsz_mb=16384,femu_mode=3 \
    -device femu,devsz_mb=16384,femu_mode=3 \
    -device femu,devsz_mb=16384,femu_mode=3 \
    -device femu,devsz_mb=16384,femu_mode=3 \
    -device femu,devsz_mb=16384,femu_mode=3 \
    -device femu,devsz_mb=16384,femu_mode=3 \
    -device femu,devsz_mb=16384,femu_mode=3 \
    -net nic,model=virtio \
    -net user,hostfwd=tcp::8080-:22 \
    -nographic \
    -qmp unix:./qmp-sock,server,nowait 2>&1 | tee log
