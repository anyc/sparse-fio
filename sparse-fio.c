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

#ifndef NO_BENCHMARK
#include <time.h>

#define TIMEVAL_FAC 1000000000
#define SPEED_SLOTS 10
#endif

// header struct of a single block
struct sparse_fio_v1_block {
	uint64_t start;
	uint64_t size;
} __attribute__((__packed__));

// version-specific header struct 
struct sparse_fio_v1_header {
	uint8_t flags;
	uint64_t unpacked_size;
	uint64_t nonzero_size;
	
	struct sparse_fio_v1_block blocks[0];
} __attribute__((__packed__));

// header struct of the own sfio file format
struct sparse_fio_header {
	uint8_t magic[10];
	uint8_t version;
} __attribute__((__packed__));

#define SFIO_IS_STREAM   1<<0
#define SFIO_IS_PACKED   1<<1
#define SFIO_IS_BLOCKDEV 1<<2

struct sparse_fio_transfer {
	char *ifilepath;
	char *ofilepath;
	
	int ifd;
	int ofd;
	
	off_t ioffset;
	off_t ooffset;
	
	off_t ooffset_last_block;
	size_t osize_last_block;
	
	size_t written_bytes;
	
	size_t isize;
	size_t isize_unpacked;
	size_t isize_nonzero;
	
	size_t bytes_to_write;
	
	size_t osize;
	size_t osize_max;
	
	int iflags;
	int oflags;
	
	char no_fsync, overwrite_output;
	char discard;
	
	#ifndef NO_BENCHMARK
	char print_stats;
	size_t bm_written_in_slot;
	struct timespec bm_timestamp, bm_prev_timestamp;
	float bm_speed[SPEED_SLOTS];
	unsigned int bm_cur_slot;
	float bm_avg_speed;
	#endif
};

unsigned char sfio_verbosity = 1;
unsigned char sfio_no_stdout_print = 0;
#define L_ERR 0
#define L_INFO 1
#define L_DBG 2
static void print(char level, char *format, ...) {
	va_list args;
	int fd;
	
	
	va_start (args, format);
	
	if (level <= sfio_verbosity) {
		if (level == L_ERR || sfio_no_stdout_print)
			fd = STDERR_FILENO;
		else
			fd = STDOUT_FILENO;
		
		if (getenv("SFIO_PID_OUTPUT"))
			dprintf(fd, "%d: ", getpid());
		vdprintf(fd, format, args);
	}
	
	va_end (args);
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
			// print(L_INFO, "\033[A\33[2K\r");
			
			// erase line and return
			print(L_INFO, "\33[2K\r");
		}
		print(L_INFO, "written %5.3f MB %7.3f MB/s ",
				(float) transfer->written_bytes / 1024 / 1024,
				transfer->bm_avg_speed / 1024 / 1024
			);
		if (transfer->bytes_to_write && transfer->bm_avg_speed > 0) {
			print(L_INFO, "time left: %4.1f s",
					(transfer->bytes_to_write - transfer->written_bytes) / transfer->bm_avg_speed
				);
		} else {
			print(L_INFO, "");
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
			print(L_ERR, "write failed (%zd): %s\n", r, strerror(errno));
			return -errno;
		}
		
		// start "flush to disk" of the current blocks asynchronously
		r = sync_file_range(transfer->ofd, transfer->ooffset, write_block_size, SYNC_FILE_RANGE_WRITE);
		if (r < 0) {
			print(L_ERR, "sync_file_range 1 failed (%zd): %s\n", r, strerror(errno));
			return -errno;
		}
		
		if (transfer->osize_last_block > 0) {
			// wait until all writes of the previous blocks have finished
			r = sync_file_range(transfer->ofd, transfer->ooffset_last_block, transfer->osize_last_block,
				SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER);
			if (r < 0) {
				print(L_ERR, "sync_file_range 2 failed (%zd): %s\n", r, strerror(errno));
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
		print(L_ERR, "write failed: %s\n", strerror(errno));
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
			print(L_ERR, "read failed: %s\n", strerror(errno));
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

static int parse_llong(char *arg, long long *value) {
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


int sfio_init() {
	char *verbosity;
	long long level;
	
	verbosity = getenv("SFIO_VERBOSITY");
	if (verbosity) {
		if (parse_llong(verbosity, &level) < 0) {
			print(L_ERR, "cannot parse SFIO_VERBOSITY=\"%s\"\n", verbosity);
		} else {
			sfio_verbosity = level;
		}
	}
	
	return 0;
}

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
			print(L_ERR, "error, no file descriptor or file path given\n");
			return -EINVAL;
		}
			
		transfer->ifd = open(transfer->ifilepath, O_RDONLY);
		if (transfer->ifd < 0) {
			print(L_ERR, "open \"%s\" failed: %s\n", transfer->ifilepath, strerror(errno));
			return -errno;
		}
	}
	
	if ((transfer->iflags & SFIO_IS_STREAM) == 0) {
		char try_fiemap;
		size_t fiemap_size;
		
		
		if (fstat(transfer->ifd, &stat) == -1) {
			print(L_ERR, "fstat for \"%s\" failed: %s\n", transfer->ifilepath, strerror(errno));
			return -errno;
		}
		
		if (S_ISBLK(stat.st_mode)) {
			long long dev_size;
			
			if (ioctl(transfer->ifd, BLKGETSIZE64, &dev_size) < 0) {
				print(L_ERR, "ioctl(BLKGETSIZE64) failed: %s\n", strerror(errno));
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
			print(L_INFO, "empty input file, will do nothing.\n");
			return 0;
		}
		
		print(L_INFO, "Input size:     %16zu (%6zu MB)\n", transfer->isize, transfer->isize / 1024 / 1024);
		
		// check if we can get a fiemap for the input file (fiemap contains information
		// about sparse/zero blocks in the file)
		if (try_fiemap) {
			fiemap_size = sizeof(struct fiemap);
			fiemap = (struct fiemap*) calloc(1, fiemap_size);
			if (!fiemap) {
				print(L_ERR, "error while allocating %zu bytes of memory\n", fiemap_size);	
				return -ENOMEM;
			}
			
			memset(fiemap, 0, sizeof(struct fiemap));
			
			fiemap->fm_length = FIEMAP_MAX_OFFSET;
			
			if (ioctl(transfer->ifd, FS_IOC_FIEMAP, fiemap) < 0) {
				print(L_DBG, "error ioctl(FS_IOC_FIEMAP) \"%s\", continue without fiemap\n", strerror(errno));
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
					print(L_ERR, "error while allocating %zu bytes of memory\n", fiemap_size);
					return -ENOMEM;
				}
			}
			
			memset(fiemap->fm_extents, 0, extents_size);
			
			fiemap->fm_extent_count = fiemap->fm_mapped_extents;
			fiemap->fm_mapped_extents = 0;
			
			if (ioctl(transfer->ifd, FS_IOC_FIEMAP, fiemap) < 0) {
				print(L_DBG, "ioctl(fiemap) 2 failed: %s, continue without fiemap\n", strerror(errno));
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
				
				print(L_INFO, "Non-zero bytes: %16zu (%6zu MB) (approx.)\n", transfer->isize_nonzero, transfer->isize_nonzero / 1024 / 1024);
			}
		} else {
			transfer->isize_nonzero = transfer->isize;
		}
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
			print(L_ERR, "error, no file descriptor or file path given\n");
			return -EINVAL;
		}
		
		transfer->ofd = open(transfer->ofilepath, O_RDWR | O_CREAT | O_EXCL, 0777);
		if (transfer->ofd < 0) {
			if (errno == EEXIST) {
				transfer->ofd = open(transfer->ofilepath, O_RDWR | O_CREAT, 0777);
				output_exists = 1;
			} 
			if (transfer->ofd < 0) {
				print(L_ERR, "open \"%s\" failed: %s\n", transfer->ofilepath, strerror(errno));
				return -errno;
			}
		}
	}
	
	print(L_DBG, "ifd %d ofd %d (stdin %d stdout %d stderr %d)\n", transfer->ifd, transfer->ofd, STDIN_FILENO, STDOUT_FILENO, STDERR_FILENO);
	
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
				print(L_DBG, "output fd is not seekable\n");
				transfer->oflags |= SFIO_IS_STREAM;
			} else {
				print(L_ERR, "lseek check failed: %s\n", strerror(errno));
				return -errno;
			}
		}
	}
	
	if ((transfer->oflags & SFIO_IS_STREAM) == 0) {
		if (fstat(transfer->ofd, &stat) == -1) {
			print(L_ERR, "fstat for \"%s\" failed: %s\n", transfer->ofilepath, strerror(errno));
			return -errno;
		}
		
		// check if output file is a block device
		if (S_ISBLK(stat.st_mode)) {
			long long dev_size;
			
			if (ioctl(transfer->ofd, BLKGETSIZE64, &dev_size) < 0) {
				print(L_ERR, "ioctl(BLKGETSIZE64) failed: %s\n", strerror(errno));
				return -errno;
			}
			transfer->osize_max = dev_size;
			transfer->oflags |= SFIO_IS_BLOCKDEV;
			
			print(L_INFO,"Block device size:    %16zu (%6zu MB)\n", transfer->osize_max, transfer->osize_max / 1024 / 1024);
			
			if (transfer->osize_max < transfer->bytes_to_write) {
				print(L_ERR, "error, device size is too small (%zu < %zu)\n", transfer->osize_max, transfer->bytes_to_write);
				return -EINVAL;
			}
			
			// issue a discard command that tells the device controller to forget about
			// previously stored data
			if (transfer->discard) {
				uint64_t range[2];
				
				range[0] = 0;
				range[1] = transfer->osize_max;
				
				r = ioctl(transfer->ofd, BLKDISCARD, &range);
				if (r < 0)
					print(L_INFO, "optional BLKDISCARD ioctl failed: %s\n", strerror(errno));
			}
		} else {
			if (output_exists && !transfer->overwrite_output) {
				print(L_ERR, "will not overwrite output file, use -f to overwrite the file\n");
				return -EEXIST;
			}
			
			// TODO test if free space is large enough?
			
			// erase content of the output file
			if (ftruncate(transfer->ofd, 0) < 0) {
				print(L_ERR, "ftruncate failed: %s\n", strerror(errno));
				return -errno;
			}
		}
	} else {
		print(L_DBG, "note: output is not seekable\n");
	}
	
	if (transfer->bytes_to_write > 0)
		print(L_INFO, "Transfer size:    %16zu (%6zu MB)\n", transfer->bytes_to_write, transfer->bytes_to_write / 1024 / 1024);
	
	if ((transfer->iflags & SFIO_IS_STREAM) == 0) {
		// map the input file into memory
		input = mmap(0, transfer->isize, PROT_READ, MAP_SHARED, transfer->ifd, 0);
		if (input == MAP_FAILED) {
			print(L_ERR, "error, mmap input failed: %s, continue without\n", strerror(errno));
			input = 0;
		}
	} else {
		input = 0;
	}
	
	
	transfer->written_bytes = 0;
	transfer->ooffset = 0;
	
	#ifndef NO_BENCHMARK
	struct timespec time_start, time_end;
	double time_diff;
	
	clock_gettime(CLOCK_MONOTONIC, &time_start);
	#endif
	
	// write the header of our packed format
	if (transfer->oflags & SFIO_IS_PACKED) {
		struct sparse_fio_header hdr;
		
		print(L_DBG, "will write in packed format\n");
		
		memcpy(hdr.magic, "SPARSE_FIO", 10);
		hdr.version = 1;
		
		print(L_DBG, "write sfio hdr %zu\n", sizeof(hdr));
		r = write_helper_stream(transfer, &hdr, sizeof(hdr));
		if (r < 0)
			return r;
	}
	
	// if input is in packed format, disable fiemap mode as we do not need
	// to skip holes
	if (fiemap) {
		unsigned char ibuffer[10];
		r = read_helper_stream(transfer, ibuffer, 10);
		if (r < 0) {
			return r;
		}
		if (r != 10 || !memcmp(ibuffer, "SPARSE_FIO", 10)) {
			transfer->iflags |= SFIO_IS_PACKED;
			free(fiemap);
			fiemap = 0;
		}
		
		// rewind in any case
		transfer->ioffset = lseek(transfer->ifd, 0, SEEK_SET);
		if (transfer->ioffset == sizeof(off_t)-1) {
			print(L_ERR, "lseek in input failed: %s\n", strerror(errno));
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
			
			print(L_DBG, "write sfio v1 hdr %zu\n", sizeof(v1_hdr));
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
					
					print(L_DBG, "write sfio v1 block hdr %zu\n", sizeof(v1_block));
					r = write_helper_stream(transfer, &v1_block, sizeof(v1_block));
					if (r < 0)
						return r;
				} else {
					transfer->ooffset = lseek(transfer->ofd, ex_start, SEEK_SET);
					if (transfer->ooffset != ex_start) {
						print(L_ERR, "lseek failed (%" PRIdMAX " != %llu): %s\n", transfer->ooffset, ex_start, strerror(errno));
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
					
					print(L_DBG, "write sfio v1 block hdr %zu\n", sizeof(v1_block));
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
								print(L_ERR, "write failed: %s\n", strerror(errno));
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
			print(L_DBG, "handle trailing zeros\n");
			
			if (transfer->oflags & SFIO_IS_PACKED) {
				struct sparse_fio_v1_block ov1_block;
				
				ov1_block.start = transfer->ioffset;
				ov1_block.size = 0;
				
				print(L_DBG, "write sfio v1 block hdr %zu\n", sizeof(ov1_block));
				r = write_helper_stream(transfer, &ov1_block, sizeof(ov1_block));
				if (r < 0)
					return r;
			} else {
				if ((transfer->oflags & SFIO_IS_BLOCKDEV) == 0) {
					r = ftruncate(transfer->ofd, transfer->osize);
					if (r < 0) {
						print(L_ERR, "ftruncate failed: %s\n", strerror(errno));
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
		char stop, add_hdr_to_block, last_block_empty;
		ssize_t bytes_read;
		struct sparse_fio_header hdr;
		struct sparse_fio_v1_header v1_hdr;
		struct sparse_fio_v1_block iv1_block, ov1_block;
		
		
		add_hdr_to_block = 1;
		
		bytes_read = read_helper_stream(transfer, &hdr, sizeof(struct sparse_fio_header));
		if (bytes_read < 0) {
			return r;
		}
		if (bytes_read == sizeof(struct sparse_fio_header) && !memcmp(hdr.magic, "SPARSE_FIO", 10)) {
			print(L_INFO, "packed input detected (version %u)\n", hdr.version);
			
			transfer->iflags |= SFIO_IS_PACKED;
			add_hdr_to_block = 0;
			transfer->ioffset += sizeof(struct sparse_fio_header);
			
			if (hdr.version != 1) {
				print(L_ERR, "unsupported version %u\n", hdr.version);
				return -EINVAL;
			}
			
			bytes_read = read_helper_stream(transfer, &v1_hdr, sizeof(struct sparse_fio_v1_header));
			if (bytes_read < 0) {
				return bytes_read;
			}
			if (bytes_read < sizeof(struct sparse_fio_v1_header)) {
				print(L_ERR, "invalid packed format: v1 hdr (%zu < %zu)\n", bytes_read, sizeof(struct sparse_fio_v1_header));
				return -EINVAL;
			}
			
			transfer->ioffset += sizeof(struct sparse_fio_v1_header);
			
			if (v1_hdr.unpacked_size) {
				transfer->osize = v1_hdr.unpacked_size;
				print(L_INFO, "Unpacked total size:    %16zu (%6zu MB)\n", v1_hdr.unpacked_size, v1_hdr.unpacked_size / 1024 / 1024);
			}
			if (v1_hdr.nonzero_size) {
				transfer->bytes_to_write = v1_hdr.nonzero_size;
				print(L_INFO, "Unpacked non-zero size: %16zu (%6zu MB) (approx.)\n", v1_hdr.nonzero_size, v1_hdr.nonzero_size / 1024 / 1024);
			}
		}
		
		if (transfer->oflags & SFIO_IS_PACKED) {
			struct sparse_fio_v1_header v1_hdr;
			
			v1_hdr.flags = 0;
			v1_hdr.unpacked_size = transfer->isize;
			v1_hdr.nonzero_size = 0; // unknown at this time
			
			print(L_DBG, "write v1 hdr %zu\n", sizeof(v1_hdr));
			r = write_helper_stream(transfer, &v1_hdr, sizeof(v1_hdr));
			if (r < 0)
				return r;
		}
		
		// TODO best value?
		max_chunk_size = sysconf(_SC_PAGE_SIZE) * 1024;
		
		if (input == 0) {
			cur_block = malloc(max_chunk_size);
			if (!cur_block) {
				print(L_ERR, "malloc failed\n");
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
					print(L_ERR, "invalid packed format: v1 block (%zu < %zu)\n", bytes_read, sizeof(struct sparse_fio_v1_block));
					return -EINVAL;
				}
				transfer->ioffset += sizeof(struct sparse_fio_v1_block);
				
				iv1_block.start = transfer->ioffset;
				iv1_block.size = ov1_block.size;
			}
			print(L_DBG, "read block %"PRIu64" %"PRIu64" %"PRIu64" %"PRIu64"\n\n", iv1_block.start, iv1_block.size, ov1_block.start, ov1_block.size);print(L_DBG, "\n");
			
			if ((transfer->iflags & SFIO_IS_STREAM) == 0) {
				// avoid the possibility of a negative assignment to iv1_block.size
				if (transfer->isize <= transfer->ioffset)
					break;
				
				if (transfer->ioffset + iv1_block.size >= transfer->isize)
					iv1_block.size = transfer->isize - transfer->ioffset;
			}
// 			
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
					if (add_hdr_to_block) {
						add_hdr_to_block = 0;
						
						memcpy(cur_block, &hdr, sizeof(struct sparse_fio_header));
						
						bytes_read = read_helper_stream(transfer,
								cur_block + sizeof(struct sparse_fio_header),
								chunk_size - sizeof(struct sparse_fio_header)
							);
						if (bytes_read < 0) {
							print(L_ERR, "read failed: %s\n", strerror(errno));
							return -errno;
						}
						if (bytes_read == 0) {
							stop = 1;
							break;
						}
						
						bytes_read += sizeof(struct sparse_fio_header);
					} else {
						bytes_read = read_helper_stream(transfer, cur_block, chunk_size);
					}
					
					print(L_DBG, "read %zd %"PRIu64" \n", bytes_read, iv1_block.size);
					if (bytes_read < 0) {
						print(L_ERR, "read failed: %s\n", strerror(errno));
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
				
				i = chunk_size;
				while (i > 0) {
					if (i >= sizeof(zero_block))
						bytes_read = sizeof(zero_block);
					else
						bytes_read = i;
					if (memcmp(cur_block, zero_block, bytes_read))
						break;
					i -= bytes_read;
				}
				only_zeros = (i == 0);
				
				if (only_zeros) {
					if (transfer->oflags & SFIO_IS_PACKED) {
						// we do not write blocks with only zeros in packed mode
						last_block_empty = 1;
					} else {
						print(L_DBG, "will create %zu zeros\n", chunk_size);
						
						if ((transfer->oflags & SFIO_IS_STREAM) == 0) {
							// create a gap in the output file
							if ((transfer->oflags & SFIO_IS_BLOCKDEV) == 0) {
								r = ftruncate(transfer->ofd, transfer->ooffset + chunk_size);
								if (r < 0) {
									print(L_ERR, "ftruncate failed: %s\n", strerror(errno));
									return r;
								}
								transfer->ooffset += chunk_size;
							} else {
								transfer->ooffset = lseek(transfer->ofd, chunk_size, SEEK_CUR);
								if (transfer->ooffset == sizeof(off_t)-1) {
									print(L_ERR, "lseek failed: %s\n", strerror(errno));
									return -errno;
								}
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
						print(L_DBG, "write sfio v1 block hdr %zu\n", sizeof(ov1_block));
						print(L_DBG, "write v1 block (start %"PRIu64" size %"PRIu64") sizeof %zu\n", ov1_block.start, ov1_block.size, sizeof(ov1_block));
						
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
					
					print(L_DBG, "will write %zu data bytes\n", chunk_size);
					
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
			print(L_DBG, "handle trailing zeros\n");
			
			if (transfer->oflags & SFIO_IS_PACKED) {
				ov1_block.start = transfer->ioffset;
				ov1_block.size = 0;
				
				print(L_DBG, "write sfio v1 block hdr %zu\n", sizeof(ov1_block));
				r = write_helper_stream(transfer, &ov1_block, sizeof(ov1_block));
				if (r < 0)
					return r;
			} else {
				print(L_ERR, "err\n");
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
			print(L_ERR, "fsync failed: %s\n", strerror(errno));
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
		
		// move cursor up one line, erase line, return cursor to first column
		print(L_INFO,"\033[A\33[2K\r");
		
		print(L_INFO,"written %f MB in %f s -> %f MB/s\n", (float) transfer->written_bytes / 1024 / 1024, time_diff / TIMEVAL_FAC, ((float) transfer->written_bytes / 1024 / 1024) / (time_diff / TIMEVAL_FAC));
	}
	#endif
	
	return 0;
}

void print_help(int level) {
	print(level, "Usage: sparse-fio [args] [<inputfile> <outputfile>]\n");
	print(level, "\n");
	print(level, "sparse-fio will copy data from the input to the output file.\n");
	print(level, "If the input file is already sparse, it will only copy the non-zero\n");
	print(level, "blocks. If the input file is not sparse, sparse-fio will read the complete\n");
	print(level, "file and create a sparse output file.\n");
	print(level, "\n");
	print(level, "If no input or no output file is specified, sparse-fio will read from stdin\n");
	print(level, "or write to stdout, respectively. If sparse-fio writes to stdout, the data\n");
	print(level, "will be written in so-called packed format. As zero holes cannot be signaled\n");
	print(level, "through pipes or similar, sparse-fio uses an own in-band protocol to notify the\n");
	print(level, "other side about holes in order to avoid transferring blocks of zeros.\n");
	print(level, "This way, images can be transferred efficiently over the network, for example.\n");
	print(level, "\n");
	print(level, "If the output file is actually a block device, sparse-fio will issue a BLKDISCARD\n");
	print(level, "ioctl that instructs the disk controller to quickly forget ALL the previously\n");
	print(level, "stored data. This is useful for SSD or SD cards where an internal controller\n");
	print(level, "keeps track of used blocks for wear leveling. Sending a BLKDISCARD avoids\n");
	print(level, "explicitly erasing or overwriting the old data.\n");
	print(level, "\n");
	print(level, "Optional arguments:\n");
	print(level, " -D              do NOT discard ALL data on target device before writing\n");
	print(level, " -f              force (overwrite existing file)\n");
	print(level, " -F              do not wait for completion using fsync()\n");
	print(level, " -i <inputfile>  \n");
	print(level, " -o <outputfile> \n");
	print(level, " -p <0|1|2>      write in packed format: 0=off, 1=on, 2=auto (default)\n");
	print(level, " -P <0|1|2>      control stats output: 0=off, 1=on, 2=auto (default)\n");
}

int main(int argc, char **argv) {
	int c, packed_setting;
	struct sparse_fio_transfer transfer;
	long long llopt;
	
	
	memset(&transfer, 0, sizeof(struct sparse_fio_transfer));
	
	transfer.ifd = -1;
	transfer.ofd = -1;
	transfer.discard = 1;
	transfer.print_stats = 2;
	packed_setting = 2;
	
	while ((c = getopt(argc, argv, "hDFfi:o:p:P:")) != -1) {
		switch (c) {
			case 'F':
				transfer.no_fsync = 1;
				break;
			case 'p':
				parse_llong(optarg, &llopt);
				transfer.print_stats = (int) llopt;
				break;
			case 'P':
				parse_llong(optarg, &llopt);
				packed_setting = (int) llopt;
				break;
			case 'D':
				transfer.discard = 0;
				break;
			case 'f':
				transfer.overwrite_output = 1;
				break;
			case 'i':
				transfer.ifilepath = optarg;
				break;
			case 'o':
				transfer.ofilepath = optarg;
				break;
			case 'h':
			default:
				print_help(L_ERR);
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
	
	if (!transfer.ifilepath) {
		// input is stdin
		
		transfer.ifd = STDIN_FILENO;
		transfer.iflags = SFIO_IS_STREAM;
	}
	
	if (!transfer.ofilepath) {
		// output is stdout
		
		transfer.ofd = STDOUT_FILENO;
		transfer.oflags = SFIO_IS_STREAM | SFIO_IS_PACKED;
		
		sfio_no_stdout_print = 1;
	}
	
	switch (packed_setting) {
		case 0: transfer.oflags = transfer.oflags & (~SFIO_IS_PACKED); break;
		case 1: transfer.oflags = transfer.oflags | SFIO_IS_PACKED; break;
		case 2: break;
		default:
			print(L_ERR, "invalid value for -p: %d\n", packed_setting);
			print_help(L_ERR);
			return 1;
	}
	
	c = sfio_transfer(&transfer);
	if (c < 0)
		return c;
}
