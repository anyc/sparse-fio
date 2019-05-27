/*
 * sparse-fio
 * ----------
 * 
 * sparse-fio is a tool to create and work with sparse files
 * 
 * Written 2017 by Mario Kicherer (http://kicherer.org)
 * 
 */

#define _GNU_SOURCE
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <inttypes.h>
#include <endian.h>
#include <limits.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/ioctl.h>
#include <sys/time.h>

#include <linux/fs.h>
#include <linux/fiemap.h>

#ifdef WITH_UTIL_LINUX
#include <blkid/blkid.h>
#include <libmount/libmount.h>
#endif

#ifndef NO_BENCHMARK
#include <time.h>

#define TIMEVAL_FAC 1000000000
#define SPEED_SLOTS 10
#endif

#include "sparse-fio.h"

#ifndef BUILD_APP_ONLY

unsigned char sfio_verbosity = SFIO_L_INFO;
unsigned char sfio_no_stdout_print = 0;
unsigned char sfio_no_color_print = 0;

void sfio_print(char level, char *format, ...) {
	va_list args;
	int fd;
	
	
	va_start (args, format);
	
	if (level <= sfio_verbosity) {
		if (level == SFIO_L_ERR || sfio_no_stdout_print)
			fd = STDERR_FILENO;
		else
			fd = STDOUT_FILENO;
		
		if (level <= SFIO_L_WARN && !sfio_no_color_print)
			dprintf(fd, "\x1b[31m");
		
		if (getenv("SFIO_PID_OUTPUT"))
			dprintf(fd, "%d: ", getpid());
		
		vdprintf(fd, format, args);
		
		if (level <= SFIO_L_WARN && !sfio_no_color_print)
			dprintf(fd, "\x1b[0m");
	}
	
	va_end (args);
}

int sfio_parse_llong(char *arg, long long *value) {
	long long val;
	char *endptr;
	
	if ('0' <= arg[0] && arg[0] <= '9') {
		if (arg[0] == '0' && arg[1] == 'x') {
			val = strtoll(arg, &endptr, 16);
		} else {
			val = strtoll(arg, &endptr, 10);
		}
		
		if (endptr) {
			if ((errno == ERANGE && (val == LLONG_MAX || val == LLONG_MIN)) || (errno != 0 && val == 0)) {
				fprintf(stderr, "No digits were found\n");
				return -EINVAL;
			}
			
			if (endptr == arg) {
				fprintf(stderr, "No digits were found\n");
				return -EINVAL;
			}
		}
	} else {
		fprintf(stderr, "invalid argument: %s\n", arg);
		return -EINVAL;
	}
	
	*value = val;
	
	return 0;
}

void sfio_print_stats(struct sparse_fio_transfer *transfer) {
	#ifndef NO_BENCHMARK
	double time_diff;
	
	if (getenv("SFIO_NO_STATS"))
		return;
	
	if (transfer->osize_last_block == 0)
		clock_gettime(CLOCK_MONOTONIC, &transfer->bm_prev_timestamp);
	
	clock_gettime(CLOCK_MONOTONIC, &transfer->bm_timestamp);

	if (transfer->osize_last_block > 0)
		time_diff = (double)(transfer->bm_timestamp.tv_sec - transfer->bm_prev_timestamp.tv_sec)*TIMEVAL_FAC +
				(transfer->bm_timestamp.tv_nsec - transfer->bm_prev_timestamp.tv_nsec);
	else
		time_diff = 0;

	transfer->bm_written_in_slot += transfer->osize_last_block;

	// only update the stats every second
	if (time_diff >= 1000000000) {
		unsigned int k, weight;
		
		transfer->bm_speed[transfer->bm_cur_slot] = ((float) transfer->bm_written_in_slot) / (time_diff / TIMEVAL_FAC);
		transfer->bm_written_in_slot = 0;
		
		// calculate an average speed based on past time slots
		weight = 0;
		transfer->bm_avg_speed = 0;
		for (k=0; k < SPEED_SLOTS; k++) {
			if (transfer->bm_speed[k] > 0) {
				transfer->bm_avg_speed += transfer->bm_speed[k];
				weight += 1;
			}
		}
		transfer->bm_avg_speed = transfer->bm_avg_speed / weight;
		
		if (transfer->bm_cur_slot >= SPEED_SLOTS-1)
			transfer->bm_cur_slot = 0;
		else
			transfer->bm_cur_slot += 1;
		
		transfer->bm_prev_timestamp = transfer->bm_timestamp;
	}
	
	if (time_diff >= 100000000 && (
			transfer->print_stats == 1 || (transfer->print_stats == 2 && (transfer->oflags & SFIO_IS_STREAM) == 0) )
		)
	{
		if (transfer->osize_last_block > 0) {
			// move cursor up one line, erase line, return cursor to first column
			// sfio_print(SFIO_L_INFO, "\033[A\33[2K\r");
			
			// erase line and return
			sfio_print(SFIO_L_INFO, "\33[2K\r");
		}
		sfio_print(SFIO_L_INFO, "written %5.3f MB %7.3f MB/s ",
				(float) transfer->written_bytes / 1024 / 1024,
				transfer->bm_avg_speed / 1024 / 1024
			);
		if (transfer->bytes_to_write && transfer->bm_avg_speed > 0) {
			sfio_print(SFIO_L_INFO, "time left: %4.1f s",
					(transfer->bytes_to_write - transfer->written_bytes) / transfer->bm_avg_speed
				);
		} else {
			sfio_print(SFIO_L_INFO, "");
		}
	}
	#endif
}


// helper function to write a given amount of data to a file descriptor
static int write_helper_mmap(struct sparse_fio_transfer *transfer, void *buffer, size_t length) {
	ssize_t r;
	size_t j;
	size_t write_block_size;
	
	
	write_block_size = 8 * 1024 * 1024;
	for (j=0; j < length; j += write_block_size) {
		if (j + write_block_size > length)
			write_block_size = length - j;
		
		// copy data into the kernel's write buffer
		r = write(transfer->ofd, buffer + j, write_block_size);
		if (r < write_block_size) {
			sfio_print(SFIO_L_ERR, "write failed (%zd): %s\n", r, strerror(errno));
			return -errno;
		}
		
		// start "flush to disk" of the current blocks asynchronously
		r = sync_file_range(transfer->ofd, transfer->ooffset, write_block_size, SYNC_FILE_RANGE_WRITE);
		if (r < 0) {
			sfio_print(SFIO_L_ERR, "sync_file_range 1 failed (%zd): %s\n", r, strerror(errno));
			return -errno;
		}
		
		if (transfer->osize_last_block > 0) {
			// wait until all writes of the previous blocks have finished
			r = sync_file_range(transfer->ofd, transfer->ooffset_last_block, transfer->osize_last_block,
				SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER);
			if (r < 0) {
				sfio_print(SFIO_L_ERR, "sync_file_range 2 failed (%zd): %s\n", r, strerror(errno));
				return -errno;
			}
			
			// tell the kernel that we will not need the previous blocks anymore
			posix_fadvise(transfer->ofd, transfer->ooffset_last_block, transfer->osize_last_block, POSIX_FADV_DONTNEED);
		}
		
		transfer->written_bytes += write_block_size;
		
		sfio_print_stats(transfer);
		
		transfer->ooffset_last_block = transfer->ooffset;
		transfer->ooffset += write_block_size;
		
		transfer->osize_last_block = write_block_size;
	}
	
	return length;
}

static int write_helper_stream(struct sparse_fio_transfer *transfer, void *buffer, size_t length) {
	ssize_t r;
	
	transfer->ooffset_last_block = transfer->ooffset;
	
	r = write(transfer->ofd, buffer, length);
	if (r < length) {
		sfio_print(SFIO_L_ERR, "write failed: %s\n", strerror(errno));
		return -errno;
	}
	transfer->written_bytes += length;
	transfer->osize_last_block = length;
	
	sfio_print_stats(transfer);
	
	transfer->ooffset += length;
	
	return length;
}

static int read_helper_stream(struct sparse_fio_transfer *transfer, void *buffer, size_t length) {
	ssize_t bytes_read;
	ssize_t r;
	
	bytes_read = 0;
	while (length > 0) {
		r = read(transfer->ifd, buffer, length);
		if (r < 0) {
			sfio_print(SFIO_L_ERR, "read failed: %s\n", strerror(errno));
			return -errno;
		} else
		if (r == 0) {
			break;
		}
		
		bytes_read += r;
		buffer += r;
		length -= r;
	}
	
	return bytes_read;
}

int sfio_init() {
	char *verbosity;
	long long level;
	
	verbosity = getenv("SFIO_VERBOSITY");
	if (verbosity) {
		if (sfio_parse_llong(verbosity, &level) < 0) {
			sfio_print(SFIO_L_ERR, "cannot parse SFIO_VERBOSITY=\"%s\"\n", verbosity);
		} else {
			sfio_verbosity = level;
		}
	}
	
	return 0;
}

#ifdef WITH_UTIL_LINUX
#define DEV_CHECK_WARN -2
#define DEV_CHECK_QUERY_FAILED -1
#define DEV_CHECK_OK 0
int show_and_check_device_info(char *filepath) {
	int r, err, i, nparts;
	size_t size;
	blkid_probe probe;
	blkid_partlist partlist;
	blkid_parttable root_tab;
	char buf[256], disk_name[128];
	dev_t disk_number;
	struct libmnt_table *mtab;
	
	
	err = DEV_CHECK_OK;
	mtab = 0;
	
	probe = blkid_new_probe_from_filename(filepath);
	if (!probe) {
		sfio_print(SFIO_L_ERR, "failed to create new blkid probe from \"%s\" (%d)\n", filepath, errno);
		err = DEV_CHECK_QUERY_FAILED;
		goto done;
	}
	
	if (!blkid_probe_is_wholedisk(probe)) {
		sfio_print(SFIO_L_WARN, "warning: %s is not a disk\n", filepath);
		err = DEV_CHECK_WARN;
		goto done;
	}
	
	r = blkid_devno_to_wholedisk(blkid_probe_get_devno(probe), disk_name, sizeof(disk_name), &disk_number);
	if (r) {
		sfio_print(SFIO_L_ERR, "blkid_devno_to_wholedisk failed for \"%s\"\n", filepath);
	} else {
		FILE *f;
		
		snprintf(buf, sizeof(buf), "/sys/block/%s/device/model", disk_name);
		f = fopen(buf, "r");
		if (!f) {
			sfio_print(SFIO_L_ERR, "opening \"%s\" failed: %s\n", filepath, strerror(errno));
		} else {
			size = fread(buf, 1, sizeof(buf), f);
			if (size > 0)
				size -= 1;
			buf[size] = 0;
			
			sfio_print(SFIO_L_INFO, "Disk model:          %16s\n", buf);
			
			fclose(f);
		}
	}
	
	partlist = blkid_probe_get_partitions(probe);
	if (!partlist) {
		sfio_print(SFIO_L_ERR, "failed to read partitions from \"%s\" (%d)\n", filepath, errno);
		err = DEV_CHECK_QUERY_FAILED;
		goto done;
	}
	
	root_tab = blkid_partlist_get_table(partlist);
	if (!root_tab) {
		sfio_print(SFIO_L_ERR, "unknown partition table on \"%s\"\n", filepath);
		err = DEV_CHECK_QUERY_FAILED;
		goto done;
	}
	
	sfio_print(SFIO_L_INFO, "Disk Id:           %16s\n", blkid_parttable_get_id(root_tab));
	
	nparts = blkid_partlist_numof_partitions(partlist);
	if (!nparts) {
		sfio_print(SFIO_L_ERR, "could not get partitions on \"%s\"\n", filepath);
		err = DEV_CHECK_QUERY_FAILED;
		goto done;
	}
	
	
	#ifdef LIBMOUNT_DEBUG
	mnt_init_debug(0xffff);
	#endif
	
	mtab = mnt_new_table();
	
	r = mnt_table_parse_mtab(mtab, 0);
	if (r) {
		sfio_print(SFIO_L_ERR, "could not parse mtab\n");
		err = DEV_CHECK_QUERY_FAILED;
		mtab = 0;
	}
	
	sfio_print(SFIO_L_INFO, "Partitions:\n");
	
	for (i = 0; i < nparts; i++) {
		const char *p;
		blkid_partition par;
		blkid_parttable tab;
		struct libmnt_fs *fs;
		
		
		par = blkid_partlist_get_partition(partlist, i);
		tab = blkid_partition_get_table(par);
		
		snprintf(buf, sizeof(buf), "%s%u", blkid_devno_to_devname(disk_number), blkid_partition_get_partno(par));
		
		sfio_print(SFIO_L_INFO, "\t%s: %10llu %10llu  0x%02x",
				buf,
				(unsigned long long) blkid_partition_get_start(par),
				(unsigned long long) blkid_partition_get_size(par),
				blkid_partition_get_type(par)
			);
		
		if (root_tab != tab)
			/* subpartition (BSD, Minix, ...) */
			sfio_print(SFIO_L_INFO, " (%s)", blkid_parttable_get_type(tab));
		
		p = blkid_partition_get_name(par);
		if (p)
			sfio_print(SFIO_L_INFO, " name='%s'", p);
		p = blkid_partition_get_uuid(par);
		if (p)
			sfio_print(SFIO_L_INFO, " uuid='%s'", p);
		p = blkid_partition_get_type_string(par);
		if (p)
			sfio_print(SFIO_L_INFO, " type='%s'", p);
		
		sfio_print(SFIO_L_INFO, "\n");
		
		if (mtab) {
			fs = mnt_table_find_source(mtab, buf, MNT_ITER_FORWARD);
			if (fs) {
				sfio_print(SFIO_L_WARN, "warning: %s is mounted\n", buf);
				err = DEV_CHECK_WARN;
			}
		}
	}
	
done:
	if (mtab)
		mnt_free_table(mtab);
	
	if (probe)
		blkid_free_probe(probe);
	
	return err;
}
#endif

int sfio_transfer(struct sparse_fio_transfer *transfer) {
	struct fiemap *fiemap;
	void *input;
	int r, output_exists;
	struct stat stat;
	
	
	/*
	 * open and analyze input file
	 */
	
	if (transfer->ifd < 0) {
		if (!transfer->ifilepath) {
			sfio_print(SFIO_L_ERR, "error, no file descriptor or file path given\n");
			return -EINVAL;
		}
			
		transfer->ifd = open(transfer->ifilepath, O_RDONLY);
		if (transfer->ifd < 0) {
			sfio_print(SFIO_L_ERR, "open \"%s\" failed: %s\n", transfer->ifilepath, strerror(errno));
			return -errno;
		}
	}
	
	if ((transfer->iflags & SFIO_IS_STREAM) == 0) {
		char try_fiemap;
		size_t fiemap_size;
		
		if (fstat(transfer->ifd, &stat) == -1) {
			sfio_print(SFIO_L_ERR, "fstat for \"%s\" failed: %s\n", transfer->ifilepath, strerror(errno));
			return -errno;
		}
		
		if (S_ISBLK(stat.st_mode)) {
			long long dev_size;
			
			if (ioctl(transfer->ifd, BLKGETSIZE64, &dev_size) < 0) {
				sfio_print(SFIO_L_ERR, "ioctl(BLKGETSIZE64) failed: %s\n", strerror(errno));
				return -errno;
			}
			transfer->isize = dev_size;
			
			transfer->iflags |= SFIO_IS_BLOCKDEV;
			
			try_fiemap = 0;
		} else {
			try_fiemap = 1;
			
			transfer->isize = stat.st_size;
		}
		
		if (transfer->isize == 0) {
			sfio_print(SFIO_L_INFO, "empty input file, will do nothing.\n");
			return 0;
		}
		
		sfio_print(SFIO_L_INFO, "Input size:        %16zu (%6zu MB)\n", transfer->isize, transfer->isize / 1024 / 1024);
		
		// check if we can get a fiemap for the input file (fiemap contains information
		// about sparse/zero blocks in the file)
		if (try_fiemap) {
			fiemap_size = sizeof(struct fiemap);
			fiemap = (struct fiemap*) calloc(1, fiemap_size);
			if (!fiemap) {
				sfio_print(SFIO_L_ERR, "error while allocating %zu bytes of memory\n", fiemap_size);	
				return -ENOMEM;
			}
			
			memset(fiemap, 0, sizeof(struct fiemap));
			
			fiemap->fm_length = FIEMAP_MAX_OFFSET;
			
			if (ioctl(transfer->ifd, FS_IOC_FIEMAP, fiemap) < 0) {
				sfio_print(SFIO_L_DBG, "error ioctl(FS_IOC_FIEMAP) \"%s\", continue without fiemap\n", strerror(errno));
				free(fiemap);
				fiemap = 0;
			}
			
			if (fiemap->fm_mapped_extents == 0) {
			}
		} else {
			fiemap = 0;
		}
		
		// if available, get detailed information about sparse blocks
		if (fiemap) {
			int i, extents_size;
			
			extents_size = sizeof(struct fiemap_extent) * fiemap->fm_mapped_extents;
			
			if (fiemap_size < sizeof(struct fiemap) + extents_size) {
				fiemap_size = sizeof(struct fiemap) + extents_size;
				fiemap = (struct fiemap*) realloc(fiemap, fiemap_size);
				if (!fiemap) {
					sfio_print(SFIO_L_ERR, "error while allocating %zu bytes of memory\n", fiemap_size);
					return -ENOMEM;
				}
			}
			
			memset(fiemap->fm_extents, 0, extents_size);
			
			fiemap->fm_extent_count = fiemap->fm_mapped_extents;
			fiemap->fm_mapped_extents = 0;
			
			if (ioctl(transfer->ifd, FS_IOC_FIEMAP, fiemap) < 0) {
				sfio_print(SFIO_L_DBG, "ioctl(fiemap) 2 failed: %s, continue without fiemap\n", strerror(errno));
				free(fiemap);
				fiemap = 0;
			}
			
			if (fiemap) {
				transfer->isize_nonzero = 0;
				for (i=0; i < fiemap->fm_mapped_extents; i++) {
					transfer->isize_nonzero += fiemap->fm_extents[i].fe_length;
				}
				
				// it looks like extents are aligned and the sum of extents of
				// a file without holes may be larger than the actual file size
				if (transfer->isize_nonzero > transfer->isize)
					transfer->isize_nonzero = transfer->isize;
				
				sfio_print(SFIO_L_INFO, "Non-zero bytes:    %16zu (%6zu MB) (approx.)\n", transfer->isize_nonzero, transfer->isize_nonzero / 1024 / 1024);
			}
		} else {
			transfer->isize_nonzero = transfer->isize;
		}
		
		#ifdef WITH_UTIL_LINUX
		if (transfer->iflags & SFIO_IS_BLOCKDEV) {
			r = show_and_check_device_info(transfer->ifilepath);
			switch (r) {
				case DEV_CHECK_WARN:
				case DEV_CHECK_QUERY_FAILED:
					if (!transfer->ignore_warnings && transfer->ifilepath && transfer->ofilepath)
						transfer->ask = 1;
					break;
				case DEV_CHECK_OK:
					break;
			}
		}
		#endif
		
		sfio_print(SFIO_L_INFO, "\n");
	} else {
		fiemap = 0;
		
		// if we do not know isize_nonzero, we assume all data as non-zero
		if (transfer->isize && transfer->isize_nonzero == 0)
			transfer->isize_nonzero = transfer->isize;
	}
	
	/*
	 * open output file
	 */
	
	output_exists = 0;
	
	if (transfer->ofd < 0) {
		if (!transfer->ofilepath) {
			sfio_print(SFIO_L_ERR, "error, no file descriptor or file path given\n");
			return -EINVAL;
		}
		
		transfer->ofd = open(transfer->ofilepath, O_RDWR | O_CREAT | O_EXCL, 0777);
		if (transfer->ofd < 0) {
			if (errno == EEXIST) {
				transfer->ofd = open(transfer->ofilepath, O_RDWR | O_CREAT, 0777);
				output_exists = 1;
			} 
			if (transfer->ofd < 0) {
				sfio_print(SFIO_L_ERR, "open \"%s\" failed: %s\n", transfer->ofilepath, strerror(errno));
				return -errno;
			}
		}
	}
	
	sfio_print(SFIO_L_DBG, "ifd %d ofd %d (stdin %d stdout %d stderr %d)\n", transfer->ifd, transfer->ofd, STDIN_FILENO, STDOUT_FILENO, STDERR_FILENO);
	sfio_print(SFIO_L_DBG, "iflags %s, oflags %s\n", (transfer->iflags & SFIO_IS_STREAM) ? "stream": "nostream", (transfer->oflags & SFIO_IS_STREAM) ? "stream": "nostream");
	
	if (transfer->isize_nonzero > 0 || (transfer->iflags & SFIO_IS_STREAM) == 0) {
		// increase required file size if we use our own packed format for the output file
		if (transfer->oflags & SFIO_IS_PACKED) {
			transfer->bytes_to_write =
				transfer->isize_nonzero +
				sizeof(struct sparse_fio_header) + sizeof(struct sparse_fio_v1_header) + sizeof(struct sparse_fio_v1_block);
		} else {
			transfer->bytes_to_write = transfer->isize_nonzero;
		}
	}
	
	// test if output is seekable
	if ((transfer->oflags & SFIO_IS_STREAM) == 0) {
		off_t foff = lseek(transfer->ofd, 0, SEEK_CUR);
		if (foff == sizeof(off_t)-1) {
			if (errno == ESPIPE) {
				sfio_print(SFIO_L_DBG, "output fd is not seekable\n");
				transfer->oflags |= SFIO_IS_STREAM;
			} else {
				sfio_print(SFIO_L_ERR, "lseek check failed: %s\n", strerror(errno));
				return -errno;
			}
		}
	}
	
	if ((transfer->oflags & SFIO_IS_STREAM) == 0) {
		if (fstat(transfer->ofd, &stat) == -1) {
			sfio_print(SFIO_L_ERR, "fstat for \"%s\" failed: %s\n", transfer->ofilepath, strerror(errno));
			return -errno;
		}
		
		// check if output file is a block device
		if (S_ISBLK(stat.st_mode)) {
			long long dev_size;
			
			if (ioctl(transfer->ofd, BLKGETSIZE64, &dev_size) < 0) {
				sfio_print(SFIO_L_ERR, "ioctl(BLKGETSIZE64) failed: %s\n", strerror(errno));
				return -errno;
			}
			transfer->osize_max = dev_size;
			transfer->oflags |= SFIO_IS_BLOCKDEV;
			
			sfio_print(SFIO_L_INFO, "Output device size:%16zu (%6zu MB)\n", transfer->osize_max, transfer->osize_max / 1024 / 1024);
			
			if (transfer->osize_max < transfer->bytes_to_write) {
				sfio_print(SFIO_L_ERR, "error, device size is too small (%zu < %zu)\n", transfer->osize_max, transfer->bytes_to_write);
				return -EINVAL;
			}
			
			#ifdef WITH_UTIL_LINUX
			if (transfer->oflags & SFIO_IS_BLOCKDEV) {
				r = show_and_check_device_info(transfer->ofilepath);
				switch (r) {
					case DEV_CHECK_WARN:
					case DEV_CHECK_QUERY_FAILED:
						if (!transfer->ignore_warnings && transfer->ifilepath && transfer->ofilepath)
							transfer->ask = 1;
						break;
					case DEV_CHECK_OK:
						break;
				}
			}
			#endif
		} else {
			if (output_exists) {
				if (!transfer->ignore_warnings) {
					if (transfer->ifilepath && transfer->ofilepath) {
						sfio_print(SFIO_L_WARN, "warning: %s already exists, will overwrite file\n", transfer->ofilepath);
						transfer->ask = 1;
					} else {
						sfio_print(SFIO_L_ERR, "will not overwrite %s\n", transfer->ofilepath);
						return -EINVAL;
					}
				} else {
					sfio_print(SFIO_L_WARN, "warning: %s already exists, will overwrite file\n", transfer->ofilepath);
				}
			}
			
			// TODO test if free space is large enough?
			
			// erase content of the output file
			if (ftruncate(transfer->ofd, 0) < 0) {
				sfio_print(SFIO_L_ERR, "ftruncate (erase) failed: %s\n", strerror(errno));
				return -errno;
			}
		}
	} else {
		sfio_print(SFIO_L_DBG, "note: output is not seekable\n");
	}
	
	if (transfer->bytes_to_write > 0)
		sfio_print(SFIO_L_INFO, "\nTransfer size:     %16zu (%6zu MB)\n", transfer->bytes_to_write, transfer->bytes_to_write / 1024 / 1024);
	
	if ((transfer->iflags & SFIO_IS_STREAM) == 0) {
		// map the input file into memory
		input = mmap(0, transfer->isize, PROT_READ, MAP_SHARED, transfer->ifd, 0);
		if (input == MAP_FAILED) {
			sfio_print(SFIO_L_ERR, "error, mmap input failed: %s, continue without\n", strerror(errno));
			input = 0;
		}
	} else {
		input = 0;
	}
	
	if (transfer->ask) {
		int input;
		
		printf("\nStart? ([y]/n) ");
		input = getchar();
		if (input == 'y') {
			input = getchar();
			if (input != '\n')
				return 0;
		} else {
			if (input != '\n')
				return 0;
		}
	}
	
	transfer->written_bytes = 0;
	transfer->ooffset = 0;
	
	#ifndef NO_BENCHMARK
	struct timespec time_start, time_end;
	double time_diff;
	
	clock_gettime(CLOCK_MONOTONIC, &time_start);
	#endif
	
	if (transfer->oflags & SFIO_IS_BLOCKDEV) {
		// issue a discard command that tells the device controller to forget about
		// previously stored data
		if (transfer->discard) {
			uint64_t range[2];
			
			range[0] = 0;
			range[1] = transfer->osize_max;
			
			r = ioctl(transfer->ofd, BLKDISCARD, &range);
			if (r < 0)
				sfio_print(SFIO_L_INFO, "optional BLKDISCARD ioctl failed: %s\n", strerror(errno));
		}
	}
	
	// write the header of our packed format
	if (transfer->oflags & SFIO_IS_PACKED) {
		struct sparse_fio_header hdr;
		
		sfio_print(SFIO_L_DBG, "will write in packed format\n");
		
		memcpy(hdr.magic, "SPARSE_FIO", 10);
		hdr.version = 1;
		
		sfio_print(SFIO_L_DBG, "write sfio hdr %zu\n", sizeof(hdr));
		r = write_helper_stream(transfer, &hdr, sizeof(hdr));
		if (r < 0)
			return r;
	}
	
	// if input is in packed format, disable fiemap mode as we do not need
	// to skip holes. NOTE: if !fiemap, we will test if the header is there again later
	if (fiemap) {
		unsigned char ibuffer[10];
		r = read_helper_stream(transfer, ibuffer, 10);
		if (r < 0) {
			return r;
		}
		if (r == 10 && !memcmp(ibuffer, "SPARSE_FIO", 10)) {
			transfer->iflags |= SFIO_IS_PACKED;
			free(fiemap);
			fiemap = 0;
		}
		
		// rewind in any case
		transfer->ioffset = lseek(transfer->ifd, 0, SEEK_SET);
		if (transfer->ioffset == sizeof(off_t)-1) {
			sfio_print(SFIO_L_ERR, "lseek in input failed: %s\n", strerror(errno));
			return -errno;
		}
	}
	
	if (transfer->oflags & SFIO_IS_PACKED) {
		transfer->osize = transfer->bytes_to_write;
	} else {
		transfer->osize = transfer->isize;
	}
	
	unsigned char zero_block[4096];
	memset(zero_block, 0, sizeof(zero_block));
	
	// if we have a fiemap of our input file, we will only read and write the blocks with actual data
	// if not, we read every block and check ourselves if the block contains only zeros
	if (fiemap) {
		int i;
		struct sparse_fio_v1_block v1_block;
		
		
		if (transfer->oflags & SFIO_IS_PACKED) {
			struct sparse_fio_v1_header v1_hdr;
			
// 			v1_hdr.n_blocks = htole64(fiemap->fm_mapped_extents);
// 			v1_hdr.flags = SFIO_HDR_FLAG_HAS_TOC;
			v1_hdr.flags = 0;
			v1_hdr.unpacked_size = transfer->isize; // input is not packed
			v1_hdr.nonzero_size = transfer->isize_nonzero;
			
			sfio_print(SFIO_L_DBG, "write sfio v1 hdr %zu\n", sizeof(v1_hdr));
			r = write_helper_stream(transfer, &v1_hdr, sizeof(v1_hdr));
			if (r < 0)
				return r;
		}
		
		// now write actual data blocks
		for (i=0; i<fiemap->fm_mapped_extents; i++) {
			uint64_t ex_start;
			uint64_t ex_length;
			
			ex_start = fiemap->fm_extents[i].fe_logical;
			ex_length = fiemap->fm_extents[i].fe_length;
			
			if (ex_start + ex_length > transfer->isize)
				ex_length = transfer->isize - ex_start;
			
			if ((transfer->oflags & SFIO_IS_STREAM) == 0) {
				if (transfer->oflags & SFIO_IS_PACKED) {
					v1_block.start = htole64(sizeof(struct sparse_fio_header) +
						sizeof(struct sparse_fio_v1_block) * fiemap->fm_mapped_extents +
						ex_start);
					v1_block.size = htole64(ex_length);
					
					sfio_print(SFIO_L_DBG, "write sfio v1 block hdr %zu\n", sizeof(v1_block));
					r = write_helper_stream(transfer, &v1_block, sizeof(v1_block));
					if (r < 0)
						return r;
				} else {
					transfer->ooffset = lseek(transfer->ofd, ex_start, SEEK_SET);
					if (transfer->ooffset != ex_start) {
						sfio_print(SFIO_L_ERR, "lseek failed (%" PRIdMAX " != %llu): %s\n", transfer->ooffset, ex_start, strerror(errno));
						return -errno;
					}
				}
				
				write_helper_mmap(transfer, input + ex_start, ex_length);
			} else {
				if (transfer->oflags & SFIO_IS_PACKED) {
					v1_block.start = htole64(sizeof(struct sparse_fio_header) +
						sizeof(struct sparse_fio_v1_block) * fiemap->fm_mapped_extents +
						ex_start);
					v1_block.size = htole64(ex_length);
					
					sfio_print(SFIO_L_DBG, "write sfio v1 block hdr %zu\n", sizeof(v1_block));
					r = write_helper_stream(transfer, &v1_block, sizeof(v1_block));
					if (r < 0)
						return r;
					
					r = write_helper_stream(transfer, input + ex_start, ex_length);
					if (r < 0)
						return r;
				} else {
					if (transfer->ooffset < ex_start) {
						size_t n_zeros;
						
						// fill the gap with zeros
						n_zeros = ex_start - transfer->ooffset;
						transfer->written_bytes += n_zeros;
						transfer->ooffset += n_zeros;
						
						while (n_zeros > 0) {
							size_t left;
							if (n_zeros >= sizeof(zero_block))
								left = sizeof(zero_block);
							else
								left = n_zeros;
							r = write(transfer->ofd, zero_block, left);
							if (r < sizeof(zero_block)) {
								sfio_print(SFIO_L_ERR, "write failed: %s\n", strerror(errno));
								return -errno;
							}
							n_zeros -= sizeof(zero_block);
						}
					}
					
					r = write_helper_stream(transfer, input + ex_start, ex_length);
					if (r < 0)
						return r;
				}
			}
		}
		
		if (transfer->ooffset < transfer->osize) {
			sfio_print(SFIO_L_DBG, "handle trailing zeros\n");
			
			if (transfer->oflags & SFIO_IS_PACKED) {
				struct sparse_fio_v1_block ov1_block;
				
				ov1_block.start = transfer->ioffset;
				ov1_block.size = 0;
				
				sfio_print(SFIO_L_DBG, "write sfio v1 block hdr %zu\n", sizeof(ov1_block));
				r = write_helper_stream(transfer, &ov1_block, sizeof(ov1_block));
				if (r < 0)
					return r;
			} else {
				if ((transfer->oflags & SFIO_IS_BLOCKDEV) == 0) {
					r = ftruncate(transfer->ofd, transfer->osize);
					if (r < 0) {
						sfio_print(SFIO_L_ERR, "ftruncate (trailing zeros) failed: %s\n", strerror(errno));
						return r;
					}
				} else {
					// we assume we wiped the block device with "discard"
				}
			}
		}
	} else {
		size_t chunk_size, max_chunk_size, i;
		char *cur_block;
		char stop, last_block_empty;
		size_t add_hdr_bytes_to_block;
		ssize_t bytes_read;
		struct sparse_fio_header hdr;
		struct sparse_fio_v1_header v1_hdr;
		struct sparse_fio_v1_block iv1_block, ov1_block;
		
		
		bytes_read = read_helper_stream(transfer, &hdr, sizeof(struct sparse_fio_header));
		if (bytes_read < 0) {
			return r;
		}
		if (bytes_read == sizeof(struct sparse_fio_header) && !memcmp(hdr.magic, "SPARSE_FIO", 10)) {
			sfio_print(SFIO_L_INFO, "packed input detected (version %u)\n", hdr.version);
			
			transfer->iflags |= SFIO_IS_PACKED;
			add_hdr_bytes_to_block = 0;
			transfer->ioffset += sizeof(struct sparse_fio_header);
			
			if (hdr.version != 1) {
				sfio_print(SFIO_L_ERR, "unsupported version %u\n", hdr.version);
				return -EINVAL;
			}
			
			bytes_read = read_helper_stream(transfer, &v1_hdr, sizeof(struct sparse_fio_v1_header));
			if (bytes_read < 0) {
				return bytes_read;
			}
			if (bytes_read < sizeof(struct sparse_fio_v1_header)) {
				sfio_print(SFIO_L_ERR, "invalid packed format: v1 hdr (%zu < %zu)\n", bytes_read, sizeof(struct sparse_fio_v1_header));
				return -EINVAL;
			}
			
			transfer->ioffset += sizeof(struct sparse_fio_v1_header);
			
			if (v1_hdr.unpacked_size) {
				transfer->osize = v1_hdr.unpacked_size;
				sfio_print(SFIO_L_INFO, "Unpacked total size:    %16zu (%6zu MB)\n", v1_hdr.unpacked_size, v1_hdr.unpacked_size / 1024 / 1024);
			}
			if (v1_hdr.nonzero_size) {
				transfer->bytes_to_write = v1_hdr.nonzero_size;
				sfio_print(SFIO_L_INFO, "Unpacked non-zero size: %16zu (%6zu MB) (approx.)\n", v1_hdr.nonzero_size, v1_hdr.nonzero_size / 1024 / 1024);
			}
		} else {
			// we will put the read bytes into the output buffer later
			add_hdr_bytes_to_block = bytes_read;
		}
		
		if (transfer->oflags & SFIO_IS_PACKED) {
			struct sparse_fio_v1_header v1_hdr;
			
			v1_hdr.flags = 0;
			v1_hdr.unpacked_size = transfer->isize;
			v1_hdr.nonzero_size = 0; // unknown at this time
			
			sfio_print(SFIO_L_DBG, "write v1 hdr %zu\n", sizeof(v1_hdr));
			r = write_helper_stream(transfer, &v1_hdr, sizeof(v1_hdr));
			if (r < 0)
				return r;
		}
		
		// TODO best value?
		max_chunk_size = sysconf(_SC_PAGE_SIZE) * 1024;
		
		if (input == 0) {
			cur_block = malloc(max_chunk_size);
			if (!cur_block) {
				sfio_print(SFIO_L_ERR, "malloc failed\n");
				exit(1);
			}
		}
		
		last_block_empty = 0;
		stop = 0;
		while (!stop) {
			if ((transfer->iflags & SFIO_IS_PACKED) == 0) {
				iv1_block.start = transfer->ioffset;
				iv1_block.size = max_chunk_size;
				ov1_block.start = transfer->ioffset;
			} else {
				bytes_read = read_helper_stream(transfer, &ov1_block, sizeof(struct sparse_fio_v1_block));
				if (bytes_read < 0) {
					return bytes_read;
				}
				if (bytes_read == 0) {
					break;
				}
				if (bytes_read < sizeof(struct sparse_fio_v1_block)) {
					sfio_print(SFIO_L_ERR, "invalid packed format: v1 block (%zu < %zu)\n", bytes_read, sizeof(struct sparse_fio_v1_block));
					return -EINVAL;
				}
				transfer->ioffset += sizeof(struct sparse_fio_v1_block);
				
				iv1_block.start = transfer->ioffset;
				iv1_block.size = ov1_block.size;
			}
			sfio_print(SFIO_L_DBG, "will read block %"PRIu64" %"PRIu64" %"PRIu64" %"PRIu64"\n\n", iv1_block.start, iv1_block.size, ov1_block.start, ov1_block.size);
			sfio_print(SFIO_L_DBG, "\n");
			
			if ((transfer->iflags & SFIO_IS_STREAM) == 0) {
				// avoid the possibility of a negative assignment to iv1_block.size
				if (transfer->isize <= transfer->ioffset)
					break;
				
				if (transfer->ioffset + iv1_block.size >= transfer->isize)
					iv1_block.size = transfer->isize - transfer->ioffset;
			}
			
			while (!stop) {
				char only_zeros = 0;
				
				chunk_size = max_chunk_size;
				
				if (transfer->ioffset >= iv1_block.start + iv1_block.size)
					break;
				
				if (chunk_size > (iv1_block.size + iv1_block.start) - transfer->ioffset)
					chunk_size = (iv1_block.size + iv1_block.start) - transfer->ioffset;
				
				if (input) {
					cur_block = input + transfer->ioffset;
				} else {
					if (add_hdr_bytes_to_block) {
						memcpy(cur_block, &hdr, sizeof(struct sparse_fio_header));
						
						bytes_read = read_helper_stream(transfer,
								cur_block + sizeof(struct sparse_fio_header),
								chunk_size - sizeof(struct sparse_fio_header)
							);
						if (bytes_read < 0) {
							sfio_print(SFIO_L_ERR, "read failed: %s\n", strerror(errno));
							return -errno;
						}
						if (bytes_read == 0) {
							stop = 1;
						}
						
						bytes_read += add_hdr_bytes_to_block;
						add_hdr_bytes_to_block = 0;
					} else {
						bytes_read = read_helper_stream(transfer, cur_block, chunk_size);
					}
					
					sfio_print(SFIO_L_DBG, "read %zd %"PRIu64" \n", bytes_read, iv1_block.size);
					
					if (bytes_read < 0) {
						sfio_print(SFIO_L_ERR, "read failed: %s\n", strerror(errno));
						return -errno;
					}
					if (bytes_read == 0) {
						stop = 1;
						break;
					}
					transfer->ioffset += bytes_read;
					
					chunk_size = bytes_read;
				}
				ov1_block.size = chunk_size;
				
				if (transfer->osize > 0) {
					// avoid the possibility of a negative assignment to iv1_block.size
					if (transfer->osize <= transfer->ooffset)
						break;
					
					if (transfer->ooffset + ov1_block.size >= transfer->osize)
						ov1_block.size = transfer->osize - transfer->ooffset;
				}
				
				i = 0;
				while (i < chunk_size) {
					if (i + sizeof(zero_block) <= chunk_size)
						bytes_read = sizeof(zero_block);
					else
						bytes_read = chunk_size - i;
					
					if (memcmp(&cur_block[i], zero_block, bytes_read))
						break;
					i += bytes_read;
				}
				only_zeros = (i == chunk_size);
				
				if (only_zeros) {
					if (transfer->oflags & SFIO_IS_PACKED) {
						// we do not write blocks with only zeros in packed mode
						last_block_empty = 1;
					} else {
						sfio_print(SFIO_L_DBG, "will create gap with %zu bytes\n", chunk_size);
						
						if ((transfer->oflags & SFIO_IS_STREAM) == 0) {
							// create a gap in the output file
							if ((transfer->oflags & SFIO_IS_BLOCKDEV) == 0) {
								r = ftruncate(transfer->ofd, transfer->ooffset + chunk_size);
								if (r < 0) {
									sfio_print(SFIO_L_ERR, "ftruncate (gap) failed: %s\n", strerror(errno));
									return r;
								}
							}
							
							transfer->ooffset = lseek(transfer->ofd, chunk_size, SEEK_CUR);
							if (transfer->ooffset == sizeof(off_t)-1) {
								sfio_print(SFIO_L_ERR, "lseek failed: %s\n", strerror(errno));
								return -errno;
							}
						} else {
							// write zeros
							r = write_helper_stream(transfer, cur_block, chunk_size);
							if (r < 0)
								return r;
						}
					}
				} else {
					if (transfer->oflags & SFIO_IS_PACKED) {
						sfio_print(SFIO_L_DBG, "write sfio v1 block hdr %zu\n", sizeof(ov1_block));
						sfio_print(SFIO_L_DBG, "write v1 block (start %"PRIu64" size %"PRIu64") sizeof %zu\n", ov1_block.start, ov1_block.size, sizeof(ov1_block));
						
						if ((transfer->oflags & SFIO_IS_STREAM) == 0) {
							r = write_helper_mmap(transfer, &ov1_block, sizeof(ov1_block));
							if (r < 0)
								return r;
						} else {
							r = write_helper_stream(transfer, &ov1_block, sizeof(ov1_block));
							if (r < 0)
								return r;
						}
					}
					
					sfio_print(SFIO_L_DBG, "will write %zu data bytes\n", chunk_size);
					
					if ((transfer->oflags & SFIO_IS_STREAM) == 0) {
						r = write_helper_mmap(transfer, cur_block, chunk_size);
						if (r < 0)
							return r;
					} else {
						r = write_helper_stream(transfer, cur_block, chunk_size);
						if (r < 0)
							return r;
					}
				}
				
				transfer->ioffset += chunk_size;
			}
		}
		
		if ((transfer->oflags & SFIO_IS_PACKED) && last_block_empty) {
			sfio_print(SFIO_L_DBG, "handle trailing zeros\n");
			
			if (transfer->oflags & SFIO_IS_PACKED) {
				ov1_block.start = transfer->ioffset;
				ov1_block.size = 0;
				
				sfio_print(SFIO_L_DBG, "write sfio v1 block hdr %zu\n", sizeof(ov1_block));
				r = write_helper_stream(transfer, &ov1_block, sizeof(ov1_block));
				if (r < 0)
					return r;
			} else {
				sfio_print(SFIO_L_ERR, "err\n");
				exit(1);
			}
		}
	}
	
	if (input)
		munmap(input, transfer->isize);
	
	// fsync() will block until all data is written to the disk
	if (!transfer->no_fsync && (transfer->oflags & SFIO_IS_STREAM) == 0) {
		r = fsync(transfer->ofd);
		if (r < 0) {
			sfio_print(SFIO_L_ERR, "fsync failed: %s\n", strerror(errno));
			return -errno;
		}
	}
	
	close(transfer->ofd);
	close(transfer->ifd);
	
	free(fiemap);
	
	#ifndef NO_BENCHMARK
	
	if (getenv("SFIO_NO_STATS") == 0 || (
			transfer->print_stats == 1 || (transfer->print_stats == 2 && (transfer->oflags & SFIO_IS_STREAM) == 0) )
		)
	{
		clock_gettime(CLOCK_MONOTONIC, &time_end);
		time_diff = (double)(time_end.tv_sec - time_start.tv_sec)*TIMEVAL_FAC + (time_end.tv_nsec - time_start.tv_nsec);
		
		// erase line, return cursor to first column
		sfio_print(SFIO_L_INFO,"\33[2K\r");
		
		sfio_print(SFIO_L_INFO,"written %f MB in %f s -> %f MB/s\n", (float) transfer->written_bytes / 1024 / 1024, time_diff / TIMEVAL_FAC, ((float) transfer->written_bytes / 1024 / 1024) / (time_diff / TIMEVAL_FAC));
	}
	#endif
	
	return 0;
}

#endif // BUILD_APP_ONLY
#ifndef BUILD_LIB_ONLY

void print_help(int level) {
	sfio_print(level, "Usage: sparse-fio [args] [<inputfile> <outputfile>]\n");
	sfio_print(level, "\n");
	sfio_print(level, "sparse-fio will copy data from the input to the output file.\n");
	sfio_print(level, "If the input file is already sparse, it will only copy the non-zero\n");
	sfio_print(level, "blocks. If the input file is not sparse, sparse-fio will read the complete\n");
	sfio_print(level, "file and create a sparse output file.\n");
	sfio_print(level, "\n");
	sfio_print(level, "If no input or no output file is specified, sparse-fio will read from stdin\n");
	sfio_print(level, "or write to stdout, respectively. If sparse-fio writes to stdout, the data\n");
	sfio_print(level, "will be written in so-called packed format. As zero holes cannot be signaled\n");
	sfio_print(level, "through pipes or similar transports, sparse-fio uses an own in-band protocol to\n");
	sfio_print(level, "notify the other side about holes in order to avoid transferring blocks of\n");
	sfio_print(level, "zeros. This way, images can be transferred efficiently over the network, for\n");
	sfio_print(level, "example.\n");
	sfio_print(level, "\n");
	sfio_print(level, "If the output file is actually a block device, sparse-fio will issue a\n");
	sfio_print(level, "BLKDISCARD ioctl that instructs the disk controller to quickly forget ALL the\n");
	sfio_print(level, "previously stored data. This is useful for SSD or SD cards where an internal\n");
	sfio_print(level, "controller keeps track of used blocks for wear leveling. Sending a BLKDISCARD\n");
	sfio_print(level, "avoids explicitly erasing or overwriting the old data.\n");
	sfio_print(level, "\n");
	sfio_print(level, "Optional arguments:\n");
	sfio_print(level, " -A              do not ask for confirmation before writing\n");
	sfio_print(level, " -C              disable colored console output\n");
	sfio_print(level, " -D              do NOT discard ALL data on target device before writing\n");
	sfio_print(level, " -f              force (ignore warnings)\n");
	sfio_print(level, " -F              do not wait for completion using fsync()\n");
	sfio_print(level, " -i <inputfile>  \n");
	sfio_print(level, " -o <outputfile> \n");
	sfio_print(level, " -p <0|1|2>      write in packed format: 0=off, 1=on, 2=auto (default)\n");
	#ifndef NO_BENCHMARK
	sfio_print(level, " -P <0|1|2>      control stats output: 0=off, 1=on, 2=auto (default)\n");
	#endif
}

int main(int argc, char **argv) {
	int c, packed_setting;
	struct sparse_fio_transfer transfer;
	long long llopt;
	
	
	memset(&transfer, 0, sizeof(struct sparse_fio_transfer));
	
	transfer.ifd = -1;
	transfer.ofd = -1;
	transfer.discard = 1;
	transfer.ask = 1;
	#ifndef NO_BENCHMARK
	transfer.print_stats = 2;
	#endif
	packed_setting = 2;
	
	while ((c = getopt(argc, argv, "hACDFfi:o:p:P:")) != -1) {
		switch (c) {
			case 'A':
				transfer.ask = 0;
				break;
			case 'C':
				sfio_no_color_print = 1;
				break;
			case 'f':
				transfer.ignore_warnings = 1;
				break;
			case 'F':
				transfer.no_fsync = 1;
				break;
			case 'p':
				sfio_parse_llong(optarg, &llopt);
				packed_setting = (int) llopt;
				break;
			case 'P':
				#ifndef NO_BENCHMARK
				sfio_parse_llong(optarg, &llopt);
				transfer.print_stats = (int) llopt;
				#endif
				break;
			case 'D':
				transfer.discard = 0;
				break;
			case 'i':
				transfer.ifilepath = optarg;
				break;
			case 'o':
				transfer.ofilepath = optarg;
				break;
			case 'h':
				print_help(SFIO_L_INFO);
				return 1;
			default:
				print_help(SFIO_L_ERR);
				return 1;
		}
	}
	
	if (optind < argc && !transfer.ifilepath) {
		transfer.ifilepath = argv[optind];
		optind++;
	}
	if (optind < argc && !transfer.ofilepath) {
		transfer.ofilepath = argv[optind];
		optind++;
	}
	
	sfio_init();
	
	if (!transfer.ifilepath || !strcmp(transfer.ifilepath, "-")) {
		// input is stdin
		
		if (transfer.ask) {
			sfio_print(SFIO_L_ERR, "cannot use stdin for data and confirmation, use -A\n", packed_setting);
			print_help(SFIO_L_ERR);
			return 1;
		}
		
		transfer.ifilepath = 0;
		transfer.ifd = STDIN_FILENO;
		transfer.iflags = SFIO_IS_STREAM;
	}
	
	if (!transfer.ofilepath || !strcmp(transfer.ofilepath, "-")) {
		// output is stdout
		
		if (transfer.ask) {
			sfio_print(SFIO_L_ERR, "cannot use stdout for data and confirmation, use -A\n", packed_setting);
			print_help(SFIO_L_ERR);
			return 1;
		}
		
		transfer.ofilepath = 0;
		transfer.ofd = STDOUT_FILENO;
		transfer.oflags = SFIO_IS_STREAM | SFIO_IS_PACKED;
		
		sfio_no_stdout_print = 1;
	}
	
	switch (packed_setting) {
		case 0: transfer.oflags = transfer.oflags & ~(SFIO_IS_PACKED); break;
		case 1: transfer.oflags = transfer.oflags | SFIO_IS_PACKED; break;
		case 2: break;
		default:
			sfio_print(SFIO_L_ERR, "invalid value for -p: %d\n", packed_setting);
			print_help(SFIO_L_ERR);
			return 1;
	}
	
	c = sfio_transfer(&transfer);
	
	return c;
}

#endif // BUILD_LIB_ONLY
