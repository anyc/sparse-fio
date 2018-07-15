sparse-fio
==========

Sparse-fio is a library to conveniently work with files that are sparsely filled
with non-zero data, e.g., transferring or applying filesystem images.

What are sparse files?
----------------------

Typical sparse files in this case are filesystem or partition images that can be
several terabytes large but consume much less space if the included filesystem
was not full. Such files usually contain large parts that are only filled with
zeros. If these files were properly created, the blocks containing only
zeros are not actually stored on the disk and the file metadata indicates
where in the file are large portions of zeros. If a normaler reader will access
the file, the kernel will make it look like the zeros are stored in the file
on the disk. By using the right APIs, applications can detect in advance if a
block only contains zeros and is not stored on disk. If such images shall be,
e.g., copied to another host or applied to a disk, knowing which parts of the
image are really required can result in significant benefits, e.g., reduction of
transfer time and write cycles on SSDs.

Usage
-----

```
Usage: ./sparse-fio [args] [<inputfile> <outputfile>]

sparse-fio will copy data from the input to the output file.
If the input file is already sparse, it will only copy the non-zero
blocks. If the input file is not sparse, sparse-fio will read the complete
file and create a sparse output file.

If no input or no output file is specified, sparse-fio will read from stdin
or write to stdout, respectively. If sparse-fio writes to stdout, the data
will be written in so-called packed format. As zero holes cannot be signaled
through pipes or similar, sparse-fio uses an own in-band protocol to notify the
other side about holes in order to avoid transferring blocks of zeros.
This way, images can be transferred efficiently over the network, for example.

Optional arguments:
 -D              do not discard data on target device before writing
 -f              force (overwrite existing file)
 -F              do not wait for completion using fsync
 -i <inputfile>  
 -o <outputfile> 
 -p              write output in packed SFIO format
 -P <0|1|2>      control stats output: 0=off, 1=on, 2=auto (default)
```
