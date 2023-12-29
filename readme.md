# pbs-fast-restore

This tools enable you to recover single qcow2/raw from proxmox backup server fast.

## How it works

Read chunks in parallel and write to target device in serial

## How to use

1. set up target device
   ```
   modprobe nbd
   qemu-img create -f qcow2 victim.qcow2 100G
   qemu-nbd -c /dev/nbd0 victim.qcow2 --cache=unsafe --detect-zeroes=unmap
   ```

2. run
   ```
   ./pbsfastrestore -src /mnt/datastore/backup/pbs/dev/vm/123/2023-12-31T19:00:06Z/drive-scsi4.img.fidx -dest /dev/nbd0 -chunks /mnt/datastore/backup/pbs/dev/.chunks/ -workers 8
   ```
