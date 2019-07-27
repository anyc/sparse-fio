#ifndef _SPARSE_FIO_H
#define _SPARSE_FIO_H

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
	
	char ask;
	char no_fsync, ignore_warnings;
	char discard;
	char write_zeroes_on_block_dev;
	
	#ifndef NO_BENCHMARK
	char print_stats;
	size_t bm_written_in_slot;
	struct timespec bm_timestamp, bm_prev_timestamp;
	float bm_speed[SPEED_SLOTS];
	unsigned int bm_cur_slot;
	float bm_avg_speed;
	#endif
};

#define SFIO_L_ERR 0
#define SFIO_L_WARN 1
#define SFIO_L_INFO 2
#define SFIO_L_DBG 3
extern unsigned char sfio_verbosity;
extern unsigned char sfio_no_stdout_print;
extern unsigned char sfio_no_color_print;

int sfio_init();
int sfio_transfer(struct sparse_fio_transfer *transfer);
void sfio_print(char level, char *format, ...);
int sfio_parse_llong(char *arg, long long *value);

#endif
