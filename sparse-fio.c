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

struct sparse_fio_v1_block {
	uint64_t start;
	uint64_t size;
};

struct sparse_fio_v1_header {
	uint64_t n_blocks;
	struct sparse_fio_v1_block blocks[0];
};

#define SPARSE_FIO_FLAG_HAS_TOC 1<<0

struct sparse_fio_header {
	uint8_t magic[10];
	uint8_t version;
	uint8_t flags;
};

void write_helper(int fd, off_t foff, char *buffer, size_t length, size_t *written, size_t total_bytes) {
	ssize_t r;
	size_t j;
	size_t write_block_size;
	
	// TODO create a session struct to avoid static variables
	static size_t prev_start = 0;
	static size_t prev_size = 0;
	
	#ifndef NO_BENCHMARK
	static size_t written_in_slot = 0;
	static struct timespec timestamp, prev_timestamp;
	static float speed[SPEED_SLOTS] = { 0 };
	static unsigned int cur_slot = 0;
	static float avg_speed = 0;
	#endif
	
	#ifndef NO_BENCHMARK
	if (prev_size == 0)
		clock_gettime(CLOCK_MONOTONIC, &prev_timestamp);
	#endif
	
	write_block_size = 8 * 1024 * 1024;
	for (j=0; j < length; j += write_block_size) {
		if (j + write_block_size > length)
			write_block_size = length - j;
		
		// copy data into the kernel's write buffer
		r = write(fd, buffer + j, write_block_size);
		if (r < write_block_size) {
			fprintf(stderr, "write failed (%zd): %s\n", r, strerror(errno));
			exit(errno);
		}
		
		// start "flush to disk" of the current blocks asynchronously
		r = sync_file_range(fd, foff, write_block_size, SYNC_FILE_RANGE_WRITE);
		if (r < 0) {
			fprintf(stderr, "sync_file_range 1 failed (%zd): %s\n", r, strerror(errno));
			exit(errno);
		}
		
		if (prev_size > 0) {
			// wait until all writes of the previous blocks have finished
			r = sync_file_range(fd, prev_start, prev_size, SYNC_FILE_RANGE_WAIT_BEFORE|SYNC_FILE_RANGE_WRITE|SYNC_FILE_RANGE_WAIT_AFTER);
			if (r < 0) {
				fprintf(stderr, "sync_file_range 2 failed (%zd): %s\n", r, strerror(errno));
				exit(errno);
			}
			
			// tell the kernel that we will not need the previous blocks anymore
			posix_fadvise(fd, prev_start, prev_size, POSIX_FADV_DONTNEED);
		}
		
		*written += write_block_size;
		
		#ifndef NO_BENCHMARK
		double time_diff;
		
		clock_gettime(CLOCK_MONOTONIC, &timestamp);
		
		if (prev_size > 0)
			time_diff = (double)(timestamp.tv_sec - prev_timestamp.tv_sec)*TIMEVAL_FAC + (timestamp.tv_nsec - prev_timestamp.tv_nsec);
		else
			time_diff = 0;
		
		written_in_slot += prev_size;
		
		if (time_diff >= 1000000000) {
			unsigned int k, weight;
			
			speed[cur_slot] = ((float) written_in_slot) / (time_diff / TIMEVAL_FAC);
			written_in_slot = 0;
			
			weight = 0;
			avg_speed = 0;
			for (k=0; k < SPEED_SLOTS; k++) {
				if (speed[k] > 0) {
					avg_speed += speed[k];
					weight += 1;
				}
			}
			avg_speed = avg_speed / weight;
			
// 			printf("%7.3f MB/s (avg: %7.3f MB/s) time left: %4u s\n", speed[cur_slot] / 1024 / 1024, avg_speed / 1024 / 1024, (unsigned int) ((total_bytes - *written) / avg_speed));
			
			if (cur_slot >= SPEED_SLOTS-1)
				cur_slot = 0;
			else
				cur_slot += 1;
			
			prev_timestamp = timestamp;
		} else {
// 			printf("\n");
		}
		
		if (prev_size > 0) {
			// move cursor up one line, erase line, return cursor to first column
			printf("\033[A\33[2K\r");
		}
		printf("written %5.3f MB ",
			  (float) *written / 1024 / 1024
			);
		printf("%7.3f MB/s time left: %4.1f s\n", avg_speed / 1024 / 1024, (total_bytes - *written) / avg_speed);
		#endif
		
		prev_start = foff;
		foff += write_block_size;
		
		prev_size = write_block_size;
	}
}


int main(int argc, char **argv) {
	char *infile, *outfile;
	int ifd, ofd, ret, c;
	struct fiemap *fiemap;
	size_t fiemap_size, isize, isize_alloc, osize, written;
	int extents_size;
	void *input, *output;
	struct stat stat;
	char try_fiemap, no_fsync, write_packed, force, output_exists;
	char output_is_block, discard;
	
	
	infile = outfile = 0;
	no_fsync = write_packed = force = 0;
	discard = 1;
	while ((c = getopt(argc, argv, "hDSpfi:o:")) != -1) {
		switch (c) {
			case 'S':
				no_fsync = 1;
				break;
			case 'p':
				write_packed = 1;
				break;
			case 'D':
				discard = 0;
				break;
			case 'f':
				force = 1;
				break;
			case 'i':
				infile = optarg;
				break;
			case 'o':
				outfile = optarg;
				break;
			case 'h':
			default:
				fprintf(stderr, "Usage: %s [args] [<inputfile> <outputfile>]\n", argv[0]);
				fprintf(stderr, "\n");
				fprintf(stderr, "Optional arguments:\n");
				fprintf(stderr, " -f              force (overwrite existing file)\n");
				fprintf(stderr, " -D              do not discard data on target device before writing\n");
				fprintf(stderr, " -i <inputfile>  \n");
				fprintf(stderr, " -o <outputfile> \n");
				fprintf(stderr, " -p              write output in own packed format\n");
				fprintf(stderr, " -S              do not wait for completion using fsync\n");
				return 1;
		}
	}
	
	if (optind < argc && !infile) {
		infile = argv[optind];
		optind++;
	}
	if (optind < argc && !outfile) {
		outfile = argv[optind];
		optind++;
	}
	
	if (!infile || !outfile) {
		fprintf(stderr, "error while parsing parameters\n");
		return 1;
	}
	
	/*
	 * open and analyze input file
	 */
	
	ifd = open(infile, O_RDONLY);
	if (ifd < 0) {
		fprintf(stderr, "open \"%s\" failed: %s\n", infile, strerror(errno));
		return 1;
	}
	
	if (fstat(ifd, &stat) == -1) {
		fprintf(stderr, "fstat for \"%s\" failed: %s\n", infile, strerror(errno));
		return 1;
	}
	
	if (S_ISBLK(stat.st_mode)) {
		long long dev_size;
		
		if (ioctl(ifd, BLKGETSIZE64, &dev_size) < 0) {
			fprintf(stderr, "ioctl(BLKGETSIZE64) failed: %s\n", strerror(errno));
			return 1;
		}
		isize = dev_size;
		
		try_fiemap = 0;
		fiemap = 0;
	} else {
		try_fiemap = 1;
		
		isize = stat.st_size;
	}
	
	printf("Input size:     %16zu (%6zu MB)\n", isize, isize / 1024 / 1024);
	
	// check if we can get a fiemap for the input file (fiemap contains information
	// about sparse/zero blocks in the file)
	if (try_fiemap) {
		fiemap_size = sizeof(struct fiemap);
		fiemap = (struct fiemap*) calloc(1, fiemap_size);
		if (!fiemap) {
			fprintf(stderr, "error while allocating %zu bytes of memory\n", fiemap_size);	
			return 1;
		}
		
		memset(fiemap, 0, sizeof(struct fiemap));
		
		fiemap->fm_length = ~0;
		
		if (ioctl(ifd, FS_IOC_FIEMAP, fiemap) < 0) {
			free(fiemap);
			fiemap = 0;
		}
	}
	
	// if available, get detailed information about sparse blocks
	if (fiemap) {
		int i;
		
		extents_size = sizeof(struct fiemap_extent) * fiemap->fm_mapped_extents;
		
		if (fiemap_size < sizeof(struct fiemap) + extents_size) {
			fiemap_size = sizeof(struct fiemap) + extents_size;
			fiemap = (struct fiemap*) realloc(fiemap, fiemap_size);
			if (!fiemap) {
				fprintf(stderr, "error while allocating %zu bytes of memory\n", fiemap_size);
				return 1;
			}
		}
		
		memset(fiemap->fm_extents, 0, extents_size);
		
		fiemap->fm_extent_count = fiemap->fm_mapped_extents;
		fiemap->fm_mapped_extents = 0;
		
		if (ioctl(ifd, FS_IOC_FIEMAP, fiemap) < 0) {
			fprintf(stderr, "ioctl(fiemap) 2 failed: %s\n", strerror(errno));
			return 1;
		}
		
		isize_alloc = 0;
		for (i=0;i<fiemap->fm_mapped_extents;i++) {
			isize_alloc += fiemap->fm_extents[i].fe_length;
		}
		printf("Non-zero bytes: %16zu (%6zu MB)\n", isize_alloc, isize_alloc / 1024 / 1024);
	} else {
		isize_alloc = isize;
	}
	
	
	/*
	 * open output file
	 */
	
	output_exists = 0;
	ofd = open(outfile, O_RDWR | O_CREAT | O_EXCL, 0777);
	if (ofd < 0) {
		if (errno == EEXIST) {
			ofd = open(outfile, O_RDWR | O_CREAT, 0777);
			output_exists = 1;
		} 
		if (ofd < 0) {
			fprintf(stderr, "open \"%s\" failed: %s\n", outfile, strerror(errno));
			return 1;
		}
	}
	
	if (fstat(ofd, &stat) == -1) {
		fprintf(stderr, "fstat for \"%s\" failed: %s\n", outfile, strerror(errno));
		return 1;
	}
	
	// check if output file is a block device
	if (S_ISBLK(stat.st_mode)) {
		long long dev_size;
		
		if (ioctl(ofd, BLKGETSIZE64, &dev_size) < 0) {
			fprintf(stderr, "ioctl(BLKGETSIZE64) failed: %s\n", strerror(errno));
			return 1;
		}
		osize = dev_size;
		
		output_is_block = 1;
		
		printf("Target size:    %16zu (%6zu MB)\n", osize, osize / 1024 / 1024);
	} else {
		if (output_exists && !force) {
			fprintf(stderr, "will not overwrite output file, use -f to overwrite the file\n");
			return 1;
		}
		
		if (write_packed)
			osize = isize_alloc;
		else
			osize = isize;
		
		output_is_block = 0;
		
		printf("Output size:    %16zu (%6zu MB)\n", osize, osize / 1024 / 1024);
	}
	
	if (osize < isize_alloc) {
		fprintf(stderr, "error, target size is smaller than source (%zu < %zu)\n", osize, isize_alloc);
		return 1;
	}
	
	// increase required file size if we use our own packed format for the output file
	if (write_packed) {
		osize += sizeof(struct sparse_fio_header) + sizeof(struct sparse_fio_v1_header) +
			sizeof(struct sparse_fio_v1_block);
	}
	
	if (!output_is_block) {
		// erase content of the output file
		if (ftruncate(ofd, 0) < 0) {
			fprintf(stderr, "ftruncate failed: %s\n", strerror(errno));
			return 1;
		}
	} else {
		// issue a discard command that tells the device controller to forget about
		// previously stored data
		if (discard) {
			uint64_t range[2];
			
			range[0] = 0;
			range[1] = osize;
			
			ret = ioctl(ofd, BLKDISCARD, &range);
			if (ret < 0)
				fprintf(stderr, "optional BLKDISCARD ioctl failed: %s\n", strerror(errno));
		}
	}
	
	
	// map the files into memory
	input =  mmap(0, isize, PROT_READ, MAP_SHARED, ifd, 0);
	output = mmap(0, osize, PROT_WRITE, MAP_SHARED, ofd, 0);
	
	if (input == MAP_FAILED || output == MAP_FAILED) {
		fprintf(stderr, "error, mmap failed: %s\n", strerror(errno));
		return 1;
	}
	
	written = 0;
	
	#ifndef NO_BENCHMARK
	struct timespec time_start, time_end;
	double time_diff;
	
	clock_gettime(CLOCK_MONOTONIC, &time_start);
	#endif
	
	// write the header of our packed format
	if (write_packed) {
		struct sparse_fio_header hdr = { "SPARSE_FIO", 1, 0 };
		
		if (fiemap)
			hdr.flags |= SPARSE_FIO_FLAG_HAS_TOC;
		
		write(ofd, &hdr, sizeof(hdr));
		output += sizeof(hdr);
	}
	
	// if we have a fiemap of our input file, we will only read and write the blocks with actual data
	// if not, we read every block and check ourselves if the block contains only zeros
	if (fiemap) {
		int i;
		
		// first write table of contents
		if (write_packed) {
			struct sparse_fio_v1_header v1_hdr;
			struct sparse_fio_v1_block v1_block;
			
			v1_hdr.n_blocks = htole64(fiemap->fm_mapped_extents);
			
			write(ofd, &v1_hdr, sizeof(v1_hdr));
			output += sizeof(v1_hdr);
			
			for (i=0; i<fiemap->fm_mapped_extents; i++) {
				v1_block.start = htole64(sizeof(struct sparse_fio_header) +
						sizeof(struct sparse_fio_v1_block) * fiemap->fm_mapped_extents +
						fiemap->fm_extents[i].fe_logical);
				v1_block.size = htole64(fiemap->fm_extents[i].fe_length);
				
				write(ofd, &v1_block, sizeof(v1_block));
				output += sizeof(v1_block);
			}
		}
		
		// now write actual data blocks
		for (i=0; i<fiemap->fm_mapped_extents; i++) {
			off_t foff;
			
			if (!write_packed) {
				foff = lseek(ofd, fiemap->fm_extents[i].fe_logical, SEEK_SET);
				if (foff != fiemap->fm_extents[i].fe_logical) {
					fprintf(stderr, "lseek failed (%" PRIdMAX " != %llu): %s\n", foff, fiemap->fm_extents[i].fe_logical, strerror(errno));
				}
			} else {
				foff = lseek(ofd, 0, SEEK_CUR);
			}
			
			write_helper(ofd, foff, input + fiemap->fm_extents[i].fe_logical, fiemap->fm_extents[i].fe_length, &written, isize_alloc);
		}
	} else {
		size_t page_size, i, ioffset, block_size;
		struct sparse_fio_v1_block *last_block;
		struct sparse_fio_v1_block v1_block;
		off_t foff;
		
		// use page size for now
		page_size = sysconf(_SC_PAGE_SIZE);
		
		last_block = 0;
		ioffset = 0;
		while (ioffset < isize) {
			block_size = page_size;
			
			if (block_size > isize - ioffset)
				block_size = isize - ioffset;
			
			// look for first non-zero value, TODO: is strchr or memcmp faster?
			for (i=0; i < block_size; i++) {
				if (*((unsigned char *) input + ioffset + i) != 0)
					break;
			}
			
			// write only if block is not empty
			if (i < block_size) {
				if (write_packed) {
					if (last_block) {
						last_block->size = htole64(le64toh(last_block->size) + block_size);
					} else {
						v1_block.start = htole64(ioffset);
						v1_block.size = htole64(block_size);
						
						last_block = output;
						write(ofd, &v1_block, sizeof(v1_block));
						output += sizeof(v1_block);
					}
				}
				
				// TODO check if write_helper() makes sense here if we keep working
				// with page-sized blocks
				write(ofd, input + ioffset, block_size);
				written += block_size;
				
// 				foff = lseek(ofd, 0, SEEK_CUR);
// 				write_helper(ofd, foff, input + ioffset, block_size, &written);
				
				output += block_size;
			} else {
				last_block = 0;
				
				foff = lseek(ofd, block_size, SEEK_CUR);
				if (foff < 0) {
					fprintf(stderr, "lseek failed: %s\n", strerror(errno));
					return 1;
				}
			}
			
			#ifndef NO_BENCHMARK
			if (ioffset > 0) {
				// move cursor up one line, erase line, return cursor to first column
				printf("\033[A\33[2K\r");
			}
			printf("read %.3f MB, written %.3f MB\n",
				  (float) ioffset / 1024 / 1024,
				  (float) written / 1024 / 1024
				 );
			#endif
			
			ioffset += block_size;
		}
	}
	
	munmap(output, osize);
	munmap(input, isize);
	
	// fsync() will block until all data is written to the disk
	if (!no_fsync) {
		ret = fsync(ofd);
		if (ret < 0) {
			fprintf(stderr, "fsync failed: %s\n", strerror(errno));
		}
	}
	
	close(ofd);
	close(ifd);
	
	free(fiemap);
	
	#ifndef NO_BENCHMARK
	
	clock_gettime(CLOCK_MONOTONIC, &time_end);
	time_diff = (double)(time_end.tv_sec - time_start.tv_sec)*TIMEVAL_FAC + (time_end.tv_nsec - time_start.tv_nsec);
	
	// move cursor up one line, erase line, return cursor to first column
	printf("\033[A\33[2K\r");
	
	printf("written %f MB in %f s -> %f MB/s\n", (float) written / 1024 / 1024, time_diff / TIMEVAL_FAC, ((float) written / 1024 / 1024) / (time_diff / TIMEVAL_FAC));
	#endif
}
