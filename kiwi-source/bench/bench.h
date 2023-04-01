#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>

#include "../engine/db.h"
#include "../engine/variant.h"

#define KSIZE (16)
#define VSIZE (1000)

#define LINE "+-----------------------------+----------------+------------------------------+-------------------+\n"
#define LINE1 "---------------------------------------------------------------------------------------------------\n"

long long get_ustime_sec(void);
void parallelize_write(long int count, int r);
void parallelize_read(long int count, int r);
void parallelize_read_write(long int count, int r, int write_percentage);

void _random_key(char *key,int length);
void* _write_test(void* args);
void* _read_test(void* args);

typedef struct t_args {
    DB* db;
    long int offset;// The start of the requests
    long int load;  // The load of the thread
    int r;          // If we use random keys
}t_args;
