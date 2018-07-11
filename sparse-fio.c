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
};

// version-specific header struct 
struct sparse_fio_v1_header {
	uint8_t flags;
	uint64_t unpacked_size;
// 	uint64_t n_blocks;
	struct sparse_fio_v1_block blocks[0];
};

// #define SFIO_HDR_FLAG_HAS_TOC 1<<0

// header struct of the own sfio file format
struct sparse_fio_header {
	uint8_t magic[10];
	uint8_t version;
};

#define SFIO_IS_STREAM 1<<0
#define SFIO_IS_PACKED 1<<1

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
	size_t total_bytes_to_write;
	
	size_t isize;
	size_t isize_nonnull;
	size_t osize;
	
	int iflags;
	int oflags;
	
	char no_fsync, write_packed, overwrite_output;
	char discard;
	
	#ifndef NO_BENCHMARK
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
		
		vdprintf(fd, format, args);
	}
	
	va_end (args);
}

void sfio_print_stats(struct sparse_fio_transfer *transfer) {
	#ifndef NO_BENCHMARK
	double time_diff;
	
	
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

	if (transfer->osize_last_block > 0) {
		// move cursor up one line, erase line, return cursor to first column
		print(L_INFO, "\033[A\33[2K\r");
	}
	print(L_INFO, "written %5.3f MB ",
		(float) transfer->written_bytes / 1024 / 1024
		);
	print(L_INFO, "%7.3f MB/s time left: %4.1f s\n",
			transfer->bm_avg_speed / 1024 / 1024,
			(transfer->total_bytes_to_write - transfer->written_bytes) / transfer->bm_avg_speed
		);
	
	#endif
}


// helper function to write a given amount of data to a file descriptor
// void write_helper_mmap(int fd, off_t *foff, char *buffer, size_t length, size_t *written, size_t total_bytes) {
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
			
			try_fiemap = 0;
		} else {
			try_fiemap = 1;
			
			transfer->isize = stat.st_size;
		}
		
		if (transfer->isize == 0) {
			print(L_INFO, "empty input file, will do nothing.\n");
			return 0;
		}
		
		print(L_INFO,"Input size:     %16zu (%6zu MB)\n", transfer->isize, transfer->isize / 1024 / 1024);
		
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
			
			fiemap->fm_length = ~0;
			
			if (ioctl(transfer->ifd, FS_IOC_FIEMAP, fiemap) < 0) {
				print(L_DBG, "error ioctl(FS_IOC_FIEMAP) \"%s\", continue without fiemap\n", strerror(errno));
				free(fiemap);
				fiemap = 0;
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
			
			transfer->isize_nonnull = 0;
			for (i=0;i<fiemap->fm_mapped_extents;i++) {
				transfer->isize_nonnull += fiemap->fm_extents[i].fe_length;
			}
			print(L_INFO,"Non-zero bytes: %16zu (%6zu MB)\n", transfer->isize_nonnull, transfer->isize_nonnull / 1024 / 1024);
		} else {
			transfer->isize_nonnull = transfer->isize;
		}
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
	
	// increase required file size if we use our own packed format for the output file
	if (transfer->write_packed) {
		transfer->total_bytes_to_write = 
			transfer->isize_nonnull +
			sizeof(struct sparse_fio_header) + sizeof(struct sparse_fio_v1_header) + sizeof(struct sparse_fio_v1_block);
		
			print(L_DBG, "will write in packed format\n");
	} else {
		if ((transfer->oflags & SFIO_IS_STREAM) == 0)
			transfer->total_bytes_to_write = transfer->isize_nonnull;
		else
			transfer->total_bytes_to_write = transfer->isize;
	}
	
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
			transfer->osize = dev_size;
			
			print(L_INFO,"Block device size:    %16zu (%6zu MB)\n", transfer->osize, transfer->osize / 1024 / 1024);
			
			if (transfer->osize < transfer->total_bytes_to_write) {
				print(L_ERR, "error, device size is too small (%zu < %zu)\n", transfer->osize, transfer->total_bytes_to_write);
				return -EINVAL;
			}
			
			// issue a discard command that tells the device controller to forget about
			// previously stored data
			if (transfer->discard) {
				uint64_t range[2];
				
				range[0] = 0;
				range[1] = transfer->osize;
				
				r = ioctl(transfer->ofd, BLKDISCARD, &range);
				if (r < 0)
					print(L_INFO, "optional BLKDISCARD ioctl failed: %s\n", strerror(errno));
			}
		} else {
			if (output_exists && !transfer->overwrite_output) {
				print(L_ERR, "will not overwrite output file, use -f to overwrite the file\n");
				return -EEXIST;
			}
			
			// erase content of the output file
			if (ftruncate(transfer->ofd, 0) < 0) {
				print(L_ERR, "ftruncate failed: %s\n", strerror(errno));
				return -errno;
			}
		}
	} else {
		print(L_INFO,"note: output is not seekable\n");
	}
	
	print(L_INFO,"Transfer size:    %16zu (%6zu MB)\n", transfer->total_bytes_to_write, transfer->total_bytes_to_write / 1024 / 1024);
	
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
	if (transfer->write_packed) {
		struct sparse_fio_header hdr;
		
		memcpy(hdr.magic, "SPARSE_FIO", 10);
		hdr.version = 1;
// 		hdr.flags = 0;
// 		
// 		if (fiemap)
// 			hdr.flags |= SFIO_HDR_FLAG_HAS_TOC;
		
		r = write_helper_stream(transfer, &hdr, sizeof(hdr));
		if (r < 0)
			return r;
// 		r = write(transfer->ofd, &hdr, sizeof(hdr));
// 		if (r < sizeof(hdr)) {
// 			print(L_ERR, "write failed: %s\n", strerror(errno));
// 			return -errno;
// 		}
// 		transfer->written_bytes += sizeof(hdr);
// 		transfer->ooffset += sizeof(hdr);
	}
	
	// if input is in packed format, disable fiemap
	if (fiemap) {
		unsigned char ibuffer[10];
		ssize_t bytes_read;
		
		bytes_read = read(transfer->ifd, ibuffer, 10);
		if (bytes_read < 0) {
			print(L_ERR, "read failed: %s\n", strerror(errno));
			return -errno;
		}
		if (!memcmp(ibuffer, "SPARSE_FIO", 10)) {
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
	
	// if we have a fiemap of our input file, we will only read and write the blocks with actual data
	// if not, we read every block and check ourselves if the block contains only zeros
	if (fiemap) {
		int i;
		struct sparse_fio_v1_block v1_block;
		
		// first write table of contents
		if (transfer->write_packed) {
			struct sparse_fio_v1_header v1_hdr;
			
// 			v1_hdr.n_blocks = htole64(fiemap->fm_mapped_extents);
// 			v1_hdr.flags = SFIO_HDR_FLAG_HAS_TOC;
			v1_hdr.flags = 0;
			v1_hdr.unpacked_size = transfer->isize;
			
// 			r = write(transfer->ofd, &v1_hdr, sizeof(v1_hdr));
// 			if (r < sizeof(v1_hdr)) {
// 				print(L_ERR, "write failed: %s\n", strerror(errno));
// 				return -errno;
// 			}
// 			transfer->written_bytes += sizeof(v1_hdr);
// 			transfer->ooffset += sizeof(v1_hdr);
			r = write_helper_stream(transfer, &v1_hdr, sizeof(v1_hdr));
			if (r < 0)
				return r;
			
// 			for (i=0; i<fiemap->fm_mapped_extents; i++) {
// 				v1_block.start = htole64(sizeof(struct sparse_fio_header) +
// 					sizeof(struct sparse_fio_v1_block) * fiemap->fm_mapped_extents +
// 					fiemap->fm_extents[i].fe_logical);
// 				v1_block.size = htole64(fiemap->fm_extents[i].fe_length);
// 				
// 				r = write_helper_stream(transfer, &v1_block, sizeof(v1_block));
// 				if (r < 0)
// 					return r;
// 			}
		}
		
		memset(zeros, 0, sizeof(zeros));
		
		// now write actual data blocks
		for (i=0; i<fiemap->fm_mapped_extents; i++) {
			if ((transfer->oflags & SFIO_IS_STREAM) == 0) {
				if (transfer->write_packed) {
					v1_block.start = htole64(sizeof(struct sparse_fio_header) +
						sizeof(struct sparse_fio_v1_block) * fiemap->fm_mapped_extents +
						fiemap->fm_extents[i].fe_logical);
					v1_block.size = htole64(fiemap->fm_extents[i].fe_length);
					
					r = write_helper_stream(transfer, &v1_block, sizeof(v1_block));
					if (r < 0)
						return r;
				} else {
					transfer->ooffset = lseek(transfer->ofd, fiemap->fm_extents[i].fe_logical, SEEK_SET);
					if (transfer->ooffset != fiemap->fm_extents[i].fe_logical) {
						print(L_ERR, "lseek failed (%" PRIdMAX " != %llu): %s\n", transfer->ooffset, fiemap->fm_extents[i].fe_logical, strerror(errno));
						return -errno;
					}
				}
				
				write_helper_mmap(transfer, input + fiemap->fm_extents[i].fe_logical, fiemap->fm_extents[i].fe_length);
			} else {
				if (transfer->write_packed) {
					v1_block.start = htole64(sizeof(struct sparse_fio_header) +
						sizeof(struct sparse_fio_v1_block) * fiemap->fm_mapped_extents +
						fiemap->fm_extents[i].fe_logical);
					v1_block.size = htole64(fiemap->fm_extents[i].fe_length);
					
					r = write_helper_stream(transfer, &v1_block, sizeof(v1_block));
					if (r < 0)
						return r;
					
					r = write_helper_stream(transfer, input + fiemap->fm_extents[i].fe_logical, fiemap->fm_extents[i].fe_length);
					if (r < 0)
						return r;
				} else {
					if (transfer->ooffset < fiemap->fm_extents[i].fe_logical) {
						size_t n_zeros;
						
						// fill the gap with zeros
						n_zeros = fiemap->fm_extents[i].fe_logical - transfer->ooffset;
						transfer->written_bytes += n_zeros;
						transfer->ooffset += n_zeros;
						
						while (n_zeros > 0) {
							r = write(transfer->ofd, zeros, sizeof(zeros));
							if (r < sizeof(zeros)) {
								print(L_ERR, "write failed: %s\n", strerror(errno));
								return -errno;
							}
							n_zeros -= sizeof(zeros);
						}
					}
					
					r = write_helper_stream(transfer, input + fiemap->fm_extents[i].fe_logical, fiemap->fm_extents[i].fe_length);
					if (r < 0)
						return r;
// 					r = write(transfer->ofd, input + fiemap->fm_extents[i].fe_logical, fiemap->fm_extents[i].fe_length);
// 					if (r < fiemap->fm_extents[i].fe_length) {
// 						print(L_ERR, "write failed: %s\n", strerror(errno));
// 						return -errno;
// 					}
// 					transfer->written_bytes += fiemap->fm_extents[i].fe_length;
// 					transfer->ooffset += fiemap->fm_extents[i].fe_length;
				}
			}
		}
	} else {
		size_t page_size, i, block_size;
		struct sparse_fio_v1_block v1_block;
		char *cur_block;
		char stop;
		ssize_t bytes_read;
		struct sparse_fio_header hdr;
		struct sparse_fio_v1_header v1_hdr;
		struct sparse_fio_v1_block v1_block;
		
		
		// TODO best value?
		page_size = sysconf(_SC_PAGE_SIZE * 1024);
		
		if (input == 0) {
			cur_block = malloc(page_size);
			if (!cur_block) {
				print(L_ERR, "malloc failed\n");
				exit(1);
			}
		}
		
		bytes_read = read(transfer->ifd, &hdr, sizeof(struct sparse_fio_header));
		if (bytes_read < 0) {
			print(L_ERR, "read failed: %s\n", strerror(errno));
			return -errno;
		}
		transfer->ioffset += sizeof(struct sparse_fio_header);
		if (bytes_read == sizeof(struct sparse_fio_header) && !memcmp(input, "SPARSE_FIO", 10)) {
			print(L_DBG, "packed input detected (version %u, flags %u)\n", hdr.version, hdr.flags);
			
			transfer->iflags |= SFIO_IS_PACKED;
			
			if (hdr.version != 1) {
				print(L_ERR, "unsupported version %u\n", hdr.version);
				return -EINVAL;
			}
			
			bytes_read = read(transfer->ifd, &v1_hdr, sizeof(struct sparse_fio_v1_header));
			if (bytes_read < 0) {
				print(L_ERR, "read failed: %s\n", strerror(errno));
				return -errno;
			}
			if (bytes_read < sizeof(struct sparse_fio_v1_header)) {
				print(L_ERR; "invalid packed format\n");
				return -EINVAL;
			}
			transfer->ioffset += sizeof(struct sparse_fio_v1_header);
			
			print(L_DBG, "Unpacked size: %zu\n", v1_hdr->unpacked_size);
// 			print(L_DBG, "#blocks: %zu\n", v1_hdr->n_blocks);
			
		}
		
		
		stop = 0;
		while (!stop) {
			block_size = page_size;
			
			if ((transfer->iflags & SFIO_IS_PACKED) == 0) {
				v1_block.start = htole64(transfer->ioffset);
				
				if (input) {
					if (transfer->isize && transfer->ioffset >= transfer->isize)
						break;
					
					if (block_size > transfer->isize - transfer->ioffset)
						block_size = transfer->isize - transfer->ioffset;
					
					cur_block = input + transfer->ioffset;
				} else {
					bytes_read = read(transfer->ifd, cur_block, block_size);
					if (bytes_read < 0) {
						print(L_ERR, "read failed: %s\n", strerror(errno));
						return -errno;
					}
					
					if (bytes_read < block_size)
						stop = 1;
					
					block_size = bytes_read;
				}
				
				transfer->ioffset += block_size;
				
				// look for first non-zero value, TODO: is strchr or memcmp faster?
				for (i=0; i < block_size; i++) {
					if (*((unsigned char *) cur_block + i) != 0)
						break;
				}
			} else {
				bytes_read = read(transfer->ifd, v1_block, sizeof(struct sparse_fio_v1_block));
				if (bytes_read < 0) {
					print(L_ERR, "read failed: %s\n", strerror(errno));
					return -errno;
				}
				if (bytes_read < sizeof(struct sparse_fio_v1_block)) {
					print(L_ERR; "invalid packed format\n");
					return -EINVAL;
				}
// 				htole64

// 				v1_block.start = v1_block.start;
// 				block_size = v1_block.size;
				// TODO loop through v1_block.size with block_size
				i = 0;
			}
			
			if (i < block_size) {
				// block contains non-zero data
				
				if (transfer->write_packed) {
					v1_block.size = htole64(block_size);
					
					r = write_helper_mmap(transfer, &v1_block, sizeof(v1_block));
					if (r < 0)
						return r;
				}
				
				if ((transfer->oflags & SFIO_IS_STREAM) == 0) {
					r = write_helper_mmap(transfer, cur_block, block_size);
					if (r < 0)
						return r;
				} else {
					r = write_helper_stream(transfer, cur_block, block_size);
					if (r < 0)
						return r;
				}
			} else {
				if (transfer->write_packed) {
				} else {
					if ((transfer->oflags & SFIO_IS_STREAM) == 0) {
						transfer->ooffset = lseek(transfer->ofd, block_size, SEEK_CUR);
						if (transfer->ooffset == sizeof(off_t)-1) {
							print(L_ERR, "lseek failed: %s\n", strerror(errno));
							return -errno;
						}
					} else {
						r = write_helper_stream(transfer, cur_block, block_size);
						if (r < 0)
							return r;
					}
				}
			}
		}
	}
	
	if (input)
		munmap(input, transfer->isize);
	
	// fsync() will block until all data is written to the disk
	if (!transfer->no_fsync) {
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
	
	clock_gettime(CLOCK_MONOTONIC, &time_end);
	time_diff = (double)(time_end.tv_sec - time_start.tv_sec)*TIMEVAL_FAC + (time_end.tv_nsec - time_start.tv_nsec);
	
	// move cursor up one line, erase line, return cursor to first column
	print(L_INFO,"\033[A\33[2K\r");
	
	print(L_INFO,"written %f MB in %f s -> %f MB/s\n", (float) transfer->written_bytes / 1024 / 1024, time_diff / TIMEVAL_FAC, ((float) transfer->written_bytes / 1024 / 1024) / (time_diff / TIMEVAL_FAC));
	#endif
	
	return 0;
}

int main(int argc, char **argv) {
	int c;
	struct sparse_fio_transfer transfer;
	
	
	memset(&transfer, 0, sizeof(struct sparse_fio_transfer));
	
	transfer.ifd = -1;
	transfer.ofd = -1;
	transfer.discard = 1;
	
	while ((c = getopt(argc, argv, "hDSpfi:o:")) != -1) {
		switch (c) {
			case 'S':
				transfer.no_fsync = 1;
				break;
			case 'p':
				transfer.write_packed = 1;
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
				print(L_ERR, "Usage: %s [args] [<inputfile> <outputfile>]\n", argv[0]);
				print(L_ERR, "\n");
				print(L_ERR, "Optional arguments:\n");
				print(L_ERR, " -f              force (overwrite existing file)\n");
				print(L_ERR, " -D              do not discard data on target device before writing\n");
				print(L_ERR, " -i <inputfile>  \n");
				print(L_ERR, " -o <outputfile> \n");
				print(L_ERR, " -p              write output in packed SFIO format\n");
				print(L_ERR, " -S              do not wait for completion using fsync\n");
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
		transfer.oflags = SFIO_IS_STREAM;
		transfer.write_packed = 1;
		
		sfio_no_stdout_print = 1;
	}
	
	sfio_transfer(&transfer);
}
