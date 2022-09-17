# Step 3: format and mount bdev
sudo umount $1
sudo mkfs.ext4 $1
sudo mount $1 /mnt/zapraid_mount
# change username to your own user
sudo chown username:username /mnt/zapraid_mount
