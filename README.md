sparse-fio
==========

Sparse-fio provides a tool to conveniently work with files that are sparsely
filled with non-zero data, e.g., for transferring or applying filesystem images.

Compared with dd, sparse-fio:

 * will only write blocks with actual data by default
 * shows information about input and output files, devices and partitions
 * shows a warning if partitions of the output device are mounted
 * shows progress information by default
 * will send a discard command if the output is a block device by default (see
   Usage below)
 * offers in-band signaling to transfer sparse files efficiently over sockets,
   pipes or filesystems without sparse file support

What are sparse files?
----------------------

Typical sparse files in this case are images of filesystems or hard disks that
can be several terabytes large but consume much less space if the filesystem
was not full. Such files usually contain large parts that are only filled with
zeros. If these files were properly created, the blocks containing only zeros
are not actually stored on the disk and the file metadata indicates where in
the file are large portions of zeros. If a normaler reader will access the file,
the kernel will make it look like the zeros are stored in the file on the disk.
By using the right APIs, applications can detect in advance if a block only
contains zeros and is not stored on disk. If such images shall be, e.g., copied
to another host or applied to a disk, knowing which parts of the image are
actually required can result in significant benefits, e.g., reduction of
transfer time and write cycles on SSDs.

Usage
-----

```
Usage: sparse-fio [args] [<inputfile> <outputfile>]

sparse-fio will copy data from the input to the output file.
If the input file is already sparse, it will only copy the non-zero
blocks. If the input file is not sparse, sparse-fio will read the complete
file and create a sparse output file.

If no input or no output file is specified, sparse-fio will read from stdin
or write to stdout, respectively. If sparse-fio writes to stdout, the data
will be written in so-called packed format. As zero holes cannot be signaled
through pipes or similar transports, sparse-fio uses an own in-band protocol to
notify the other side about holes in order to avoid transferring blocks of
zeros. This way, images can be transferred efficiently over the network, for
example.

If the output file is actually a block device, sparse-fio will issue a
BLKDISCARD ioctl that instructs the disk controller to quickly forget ALL the
previously stored data. This is useful for SSD or SD cards where an internal
controller keeps track of used blocks for wear leveling. Sending a BLKDISCARD
avoids explicitly erasing or overwriting the old data.

Optional arguments:
 -A              do not ask for confirmation before writing
 -C              disable colored console output
 -D              do NOT discard ALL data on target device before writing
 -f              force (ignore warnings)
 -F              do not wait for completion using fsync()
 -i <inputfile>  
 -o <outputfile> 
 -p <0|1|2>      write in packed format: 0=off, 1=on, 2=auto (default)
 -P <0|1|2>      control stats output: 0=off, 1=on, 2=auto (default)
```

Example to flash an OS image to a disk:

```
$ sudo sparse-fio my_os.img /dev/sdd -a
Input size:              2048000000 (  1953 MB)
Non-zero bytes:           871043072 (   830 MB) (approx.)

Output device size:     31914983424 ( 30436 MB)
Disk model:          STORAGE DEVICE
Disk Id:                   f7251e4b
Partitions:
        /dev/sdd1:       2048     409600  0x0c uuid='f7251e4b-01'
        /dev/sdd2:     411648    3588352  0x83 uuid='f7251e4b-02'

Transfer size:            871043072 (   830 MB)

Start? ([y]/n)

written 830.691406 MB in 22.414081 s -> 37.061141 MB/s
```

Build
-----

Sparse-fio only uses Makefiles to build this project. Hence, simply execute

```
make
```

will build the executable.

The makefile recognizes the following arguments:

 * BUILD_SINGLE_EXECUTABLE=0
   build executable and library
 * DEBUG=1
   build with debug flags
 * WITH_UTIL_LINUX=0
   build without dependency on util-linux which results in less information
   shown about the target block device
 * NO_BENCHMARK=0
   build without benchmark/statistics output
