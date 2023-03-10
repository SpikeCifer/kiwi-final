#include <string.h>
#include "bench.h"

#define DATAS ("testdb")
#define THREAD_NUM 4

void parallelize_write(int count, int r)
{
	DB* db = db_open(DATAS);

	int t_load = count / THREAD_NUM;
	pthread_t t_id[THREAD_NUM];
	
	for(int i = 0; i < THREAD_NUM; i++)
	{
		t_args thread_args;
		thread_args.db = db;
		thread_args.id = i;
		thread_args.count = t_load;

		//Last thread gets the remaining load
		if(i == THREAD_NUM-1)
		{
			thread_args.count += count % THREAD_NUM; // if count is not devidable by THREAD_NUM some requests are not served. // Leonidas ahh!!
		}

		pthread_create(&t_id[i], NULL, test_loop, (void*)thread_args);
	}

	for (int i = 0; i < THREAD_NUM; i++)
	{
		pthread_join(&t_id[i], NULL);
	}

	db_close(db);// No matter the result, close the DB
}


void *_write_test(void* args) 
{
	t_args* thread_args = (t_args *) args;

	int i;
	double cost;
	long long start,end;

	char key[KSIZE + 1];
	char val[VSIZE + 1];
	char sbuf[1024];

	memset(key, 0, KSIZE + 1);
	memset(val, 0, VSIZE + 1);
	memset(sbuf, 0, 1024);

	start = get_ustime_sec();
	
    // This is the write loop
	for (int i = offset; i < offset + thread_args->count; i++)  //TODO to be figured out
	{
	    Variant sk, sv;
		if (r)
			_random_key(key, KSIZE);
		else
			snprintf(key, KSIZE, "key-%d", i);
		fprintf(stderr, "%d adding %s\n", i, key);
		snprintf(val, VSIZE, "val-%d", i);

		sk.length = KSIZE;
		sk.mem = key;
		sv.length = VSIZE;
		sv.mem = val;

		db_add(thread_args->db, &sk, &sv);
		if ((i % 10000) == 0) {
			fprintf(stderr,"random write finished %d ops%30s\r", 
					i, 
					"");

			fflush(stderr);
		}
	}

	end = get_ustime_sec();
	cost = end -start;

	printf(LINE);
	printf("|Random-Write	(done:%ld): %.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec);\n"
		,count, (double)(cost / count)
		,(double)(count / cost)
		,cost);	
}

void _read_test(DB* db, long int count, int r)
{
	int i;
	int ret;
	int found = 0;
	double cost;
	long long start,end;
	char key[KSIZE + 1];

	start = get_ustime_sec();

    // This is the read loop
	for (i = 0; i < count; i++) 
	{
        Variant sk;
        Variant sv;
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

	end = get_ustime_sec();
	cost = end - start;
	printf(LINE);
	printf("|Random-Read	(done:%ld, found:%d): %.6f sec/op; %.1f reads /sec(estimated); cost:%.3f(sec)\n",
		count, found,
		(double)(cost / count),
		(double)(count / cost),
		cost);
}
