#include <pthread.h>
#include <string.h>
#include "bench.h"

#define DATAS ("testdb")
#define THREAD_NUM 10

// Prepares the writer threads
// count - The number of requests
// r - True/False use random key
void parallelize_write(long int count, int r)
{
    long int threads_load = count/THREAD_NUM; // what happens if count < TH_N?
    pthread_t threads[THREAD_NUM];
    DB* db = db_open(DATAS);

    long long start = get_ustime_sec();
    for(int i = 0; i < THREAD_NUM; i++) {
        t_args* thread_args_p = (t_args*)malloc(sizeof(t_args));
        thread_args_p->db = db;
        thread_args_p->offset = i*threads_load;
        thread_args_p->load = threads_load;
        thread_args_p->r = r;

        if (i == THREAD_NUM - 1) {
            // Add the remaining load to the last thread
            thread_args_p->load += count % THREAD_NUM; 
        }
        pthread_create(&threads[i], NULL, _write_test, (void*) thread_args_p);
    }
    for(int i = 0; i < THREAD_NUM; i++) {
        pthread_join(threads[i], NULL);
    }
    long long end = get_ustime_sec();
    db_close(db);

    double cost = end - start;
	printf(LINE);
	printf("|Random-Write	(done:%ld): %.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec);\n"
		,count, (double)(cost / count)
		,(double)(count / cost)
		,cost);	
}

void* _write_test(void* args) 
{
	t_args* thread_args = (t_args *) args;
	Variant sk, sv;

	char key[KSIZE + 1];
	char val[VSIZE + 1];
	char sbuf[1024];

	memset(key, 0, KSIZE + 1);
	memset(val, 0, VSIZE + 1);
	memset(sbuf, 0, 1024);
	
	for (int i = thread_args->offset; i < thread_args->offset + thread_args->load; i++)  
	{
		if (thread_args->r)
			_random_key(key, KSIZE);
		else
			snprintf(key, KSIZE, "key-%d", i);
		fprintf(stderr, "%d adding %s\n", i, key);
		snprintf(val, VSIZE, "val-%d", i);

		sk.length = KSIZE;
		sk.mem = key;
		sv.length = VSIZE;
		sv.mem = val;

        pthread_mutex_lock(&thread_args->db->lock);
		db_add(thread_args->db, &sk, &sv);
        pthread_mutex_unlock(&thread_args->db->lock);
		if ((i % 10000) == 0) {
			fprintf(stderr,"random write finished %d ops%30s\r", i, "");
			fflush(stderr);
		}
	}
    return NULL;
}

void linear_read_test(long int count, int r) {
    int i;
	int ret;
	int found = 0;
	double cost;
	long long start,end;
	Variant sk;
	Variant sv;
	DB* db;
	char key[KSIZE + 1];

	db = db_open(DATAS);
	start = get_ustime_sec();
	for (i = 0; i < count; i++) {
		memset(key, 0, KSIZE + 1);

		/* if you want to test random write, use the following */
		if (r)
			_random_key(key, KSIZE);
		else
			snprintf(key, KSIZE, "key-%d", i);
		fprintf(stderr, "%d searching %s\n", i, key);
		sk.length = KSIZE;
		sk.mem = key;
		ret = db_get(db, &sk, &sv);
		if (ret) {
			//db_free_data(sv.mem);
			found++;
		} else {
			INFO("not found key#%s", 
					sk.mem);
    	}

		if ((i % 10000) == 0) {
			fprintf(stderr,"random read finished %d ops%30s\r", 
					i, 
					"");

			fflush(stderr);
		}
	}

	db_close(db);

	end = get_ustime_sec();
	cost = end - start;
	printf(LINE);
	printf("|Random-Read	(done:%ld, found:%d): %.6f sec/op; %.1f reads /sec(estimated); cost:%.3f(sec)\n",
		count, found,
		(double)(cost / count),
		(double)(count / cost),
		cost);
}

void parallelize_read(long int count, int r)
{
    long int threads_load = count/THREAD_NUM;
    int found = 0;
    DB* db = db_open(DATAS);
    pthread_t threads[THREAD_NUM];

	long long start = get_ustime_sec();
    for(int i = 0; i < THREAD_NUM; i++) {
        t_args* thread_args_p = (t_args*)malloc(sizeof(t_args));
        thread_args_p->db = db;
        thread_args_p->offset = i*threads_load;
        thread_args_p->load = threads_load;
        thread_args_p->r = r;

        if(i == THREAD_NUM - 1) {
            thread_args_p->load += count % THREAD_NUM;
        }
        pthread_create(&threads[i], NULL, _read_test, (void*) thread_args_p);
    }
    for(int i = 0; i < THREAD_NUM; i++) {
        void* thread_found;
        pthread_join(threads[i], &thread_found); // Replace with a found which gets summed
        found += *(long *) thread_found;
    }
    // Run the read procedure here
	long long end = get_ustime_sec();
    db_close(db);

	double cost = end - start;
	printf(LINE);
	printf("|Random-Read	(done:%ld, found:%d): %.6f sec/op; %.1f reads /sec(estimated); cost:%.3f(sec)\n",
		count, found,
		(double)(cost / count),
		(double)(count / cost),
		cost);
}

void* _read_test(void* args)
{
    t_args* thread_args = (t_args *) args;
    long int* found = (long int*)malloc(sizeof(long int));
    int missed = 0;
	char key[KSIZE + 1];
    Variant sk, sv;

    // This is the read loop
	for (int i = thread_args->offset; i < thread_args->offset + thread_args->load; i++) 
	{
		memset(key, 0, KSIZE + 1);
		/* if you want to test random write, use the following */
		if (thread_args->r)
			_random_key(key, KSIZE);
		else
			snprintf(key, KSIZE, "key-%d", i);
		fprintf(stderr, "%d searching %s\n", i, key);
		sk.length = KSIZE;
		sk.mem = key;
		if (db_get(thread_args->db, &sk, &sv)) {
			//db_free_data(sv.mem);
			(*found)++;
		} else {
			INFO("not found key#%s", sk.mem);
    	}

		if ((i % 10000) == 0) {
			fprintf(stderr,"random read finished %d ops%30s\r", i, "");
			fflush(stderr);
		}
	}
    return found;
}
