#include "glob.h"
#include "list.h"
#include "OIP.h"
#include <stdio.h>
#include "rel.h"
#include <math.h>
#include "timing.h"
#include "btree.h"
#include "SeqScan.h"
#include "RIT.h"
#include "QTREE.h"
#include "SEGTREE.h"
#include <getopt.h>
#include <string.h>
#include "File.h"
#include "Buffer.h"
#include "nodeHeapAm.h"
#include "nodePartitionAM.h"
#include "nodeSort.h"
#include "partition.h"
#include <limits.h>
#include <unistd.h>
#include <stdint.h>

struct params {

	/*
	 * options:
	 *
	 * 1 = random
	 * 2 = from file
	 * 3 = random with Gaussian random interval duration
	 */
	int opt;

	/*
	 * random parameters
	 */
	long on;
	long ous;
	long oue;
	long ol;
	long in;
	long ius;
	long iue;
	long il;

	long omean;
	long ostdev;
	long imean;
	long istdev;

	/*
	 * from file parameters
	 */
	char *ofn;
	char *ifn;

	/*
	 * algorithms
	 */
	int oip; /* OIP algo */
	int rit; /* RIT algo */
	int qtree; /* QTREE algo */
	int lqtree; /* LQTREE algo */
	int mjoin; /* merge join for equality */
	int eJoin; /* Equijoin */
	int eJoinMM; /* Equijoin Main Memory*/
	int antiJoin; /* Equijoin */
	int aggregation; /* Aggregation */
	int normalization; /* Aggregation */
	int intersection; /*  */
	int granPartition; /* Partitioning Francesco */
	int readFile; /* Read Binary File */
	int nestloop; /* nested loop */
	int segtree; /* index nested loop using segment tree */

	/*
	 * oip parameters
	 */
	int tight;
	int ok;
	int ik;
	int k_idependent;
	int threads;

	int cpu_io_cost;

	/*
	 * lqt parameters
	 */

	int T; /* split factor */


	/*
	 * rit parameters
	 */

	bool rit_clustered;

	/*
	 * storage
	 */
	int on_disk;
	int on_mem_disk;
};

/*static int
calculate_k(long p_outer, long ni, long bi, double fi, long no, double io_cost, double cpu_cost);*/
static int
calculate_k_outer(long ni, long bi, double fi, timestmp ui, long no, double fo, timestmp uo, double io_cost, double cpu_cost);

static void
run_OIP_MM(List *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		List *irel, long in, timestmp ius, timestmp iue, timestmp il, int tight, int ok, int ik, int k_independent);
static void
run_OIP_MM_parallel(List *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		List *irel, long in, timestmp ius, timestmp iue, timestmp il, int tight, int ok, int ik, int k_independent, int threads);
static void
run_OIP_DISK(HeapAm *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		HeapAm *irel, long in, timestmp ius, timestmp iue, timestmp il, int tight, int ok, int ik, int k_independent, int cpu_io_cost);
static void
run_RIT_MM(List *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		List *irel, long in, timestmp ius, timestmp iue, timestmp il);
static void
run_RIT_DISK(HeapAm *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		HeapAm *irel, long in, timestmp ius, timestmp iue, timestmp il, bool clustered);
static void
run_QTREE_MM(List *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		List *irel, long in, timestmp ius, timestmp iue, timestmp il, int T);
static void
run_QTREE_DISK(HeapAm *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		HeapAm *irel, long in, timestmp ius, timestmp iue, timestmp il, int T);
static void
run_LQTREE_MM(List *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		List *irel, long in, timestmp ius, timestmp iue, timestmp il, int T);
static void
run_LQTREE_DISK(HeapAm *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		HeapAm *irel, long in, timestmp ius, timestmp iue, timestmp il, int T);

static void
run_MERGEJOIN_DISK(HeapAm *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		HeapAm *irel, long in, timestmp ius, timestmp iue, timestmp il);
static void
run_EQUIJOIN_DISK(HeapAm *orel,	char *ofn,
		char *ifn);
static void
run_EQUIJOIN_MM(HeapAm *orel,	char *ofn,
		char *ifn);

static void
run_ANTIJOIN_DISK(HeapAm *orel,	char *ofn,
		char *ifn);
static void
run_INTERSECTION_DISK(HeapAm *orel,	char *ofn,
		char *ifn);
static void
run_AGGREGATION_DISK(HeapAm *orel,	char *ofn);

static void
run_NORMALIZATION_DISK(HeapAm *orel,	char *ofn);

static void
run_readFile_Fra_DISK(char *fname);
static void
reorderParitions(ListCell *lc);


static void
run_Partition_Fra_DISK(HeapAm *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		HeapAm *irel, long in, timestmp ius, timestmp iue, timestmp il, char *ofn);

static List **
run_Partition_Fra_MM(List *rel, long on, timestmp ous, timestmp oue, timestmp ol
		);


static void
run_DIP_MM(List *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		List *irel, long in, timestmp ius, timestmp iue, timestmp il, char *ofn);


static int overlapFRA(ttup *tup1, ttup *tup);
static int intervalLen(ttup *tup1);

static void
run_NESTLOOP_DISK(HeapAm *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		HeapAm *irel, long in, timestmp ius, timestmp iue, timestmp il);

static void
run_SEGTREE_MM(List *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		List *irel, long in, timestmp ius, timestmp iue, timestmp il);
static void run_SEGTREE_DISK(HeapAm *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		HeapAm *irel, long in, timestmp ius, timestmp iue, timestmp il);

static int cmp_timestmp_start(const void *a, const void *b);


static void
getParams( int argc, char *argv[], struct params *params );

int main(int argc, char *argv[])
{

	struct params params;
	getParams(argc, argv, &params);

	long n_outer, l_outer, n_inner, l_inner;
	timestmp us_outer, ue_outer, us_inner, ue_inner;

	void *outer_rel, *inner_rel;


	if(params.opt == 1)
	{
		/* random */
		n_outer = params.on;
		l_outer = params.ol;
		us_outer = params.ous;
		ue_outer = params.oue;
		n_inner = params.in;
		l_inner = params.il;
		us_inner = params.ius;
		ue_inner = params.iue;

		if(n_outer < 1 || l_outer < 1 || us_outer < 1 || ue_outer < 2 || n_inner < 1 || l_inner < 1 || us_inner < 1 || ue_inner < 2)
		{
			fprintf(stderr, "ERROR: parameters incorrect!\n");
			exit(-1);
		}

		if(params.on_disk || params.on_mem_disk)
		{
			FILE_TYPE type = TYPE_DISK;
			if(params.on_mem_disk)
				type = TYPE_MEM;
			outer_rel = makeRandDiskRel("orel.dat", sizeof(ttup)+50, n_outer, l_outer, us_outer, ue_outer, type);
			inner_rel = makeRandDiskRel("irel.dat", sizeof(ttup)+50, n_inner, l_inner, us_inner, ue_inner, type);
		}
		else
		{
			outer_rel = makeRandRel(n_outer, l_outer, us_outer, ue_outer);
			inner_rel = makeRandRel(n_inner, l_inner, us_inner, ue_inner);
		}

	}
	else if(params.opt == 2)
	{
		/* from file */

		if(!params.ofn || !params.ifn)
		{
			fprintf(stderr, "ERROR: parameters incorrect!\n");
			exit(-1);
		}

		if(params.on_disk || params.on_mem_disk)
		{
			FILE_TYPE type = TYPE_DISK;
			if(params.on_mem_disk)
				type = TYPE_MEM;
			outer_rel = makeDiskRelFromFile("orel.dat", sizeof(ttup)+50, params.ofn, type);
			inner_rel = makeDiskRelFromFile("irel.dat", sizeof(ttup)+50, params.ifn, type);
		}
		else
		{
			outer_rel = makeRelFromFile(params.ofn);
			inner_rel = makeRelFromFile(params.ifn);
		}
	}
	else if(params.opt == 3)
	{
		/* random with Gaussian random interval duration */
		n_outer = params.on;
		us_outer = params.ous;
		ue_outer = params.oue;
		n_inner = params.in;
		us_inner = params.ius;
		ue_inner = params.iue;

		if(n_outer < 1 || params.omean < 1 || params.ostdev < 1 || us_outer < 1 || ue_outer < 2 || n_inner < 1 || params.imean < 1 || params.istdev < 1 || us_inner < 1 || ue_inner < 2)
		{
			fprintf(stderr, "ERROR: parameters incorrect!\n");
			exit(-1);
		}

		if(params.on_disk || params.on_mem_disk)
		{
			FILE_TYPE type = TYPE_DISK;
			if(params.on_mem_disk)
				type = TYPE_MEM;
			outer_rel = makeGaussRandDiskRel("orel.dat", sizeof(ttup)+50, n_outer, params.omean, params.ostdev, us_outer, ue_outer, type);
			inner_rel = makeGaussRandDiskRel("irel.dat", sizeof(ttup)+50, n_inner, params.imean, params.istdev, us_inner, ue_inner, type);
		}
		else
		{
			outer_rel = makeGaussRandRel(n_outer, params.omean, params.ostdev, us_outer, ue_outer);
			inner_rel = makeGaussRandRel(n_inner, params.imean, params.istdev, us_inner, ue_inner);
		}
	}
	else
	{
		fprintf(stderr, "ERROR: invalid option\n");
		exit(-1);
	}


	/* evaluate dataset characteristics */
	n_outer = 0;
	l_outer = 0;
	us_outer = LONG_MAX;
	ue_outer = LONG_MIN;
	n_inner = 0;
	l_inner = 0;
	us_inner = LONG_MAX;
	ue_inner = LONG_MIN;

	if(params.on_disk)
	{
		ttup *tup;
		ExecHeapAmScanInit((HeapAm *)outer_rel);
		while((tup = ExecHeapAmScan((HeapAm *)outer_rel)) != NULL)
		{
			n_outer++;
			l_outer = MAX(l_outer, tup->te-tup->ts+1);
			us_outer = MIN(us_outer, tup->ts);
			ue_outer = MAX(ue_outer, tup->te);
		}
		ExecHeapAmScanInit((HeapAm *)inner_rel);
		while((tup = ExecHeapAmScan((HeapAm *)inner_rel)) != NULL)
		{
			n_inner++;
			l_inner = MAX(l_inner, tup->te-tup->ts+1);
			us_inner = MIN(us_inner, tup->ts);
			ue_inner = MAX(ue_inner, tup->te);
		}
	}
	else
	{
		ListCell *c;
		ttup *tup;
		foreach(c, (List *)outer_rel)
		{
			tup = (ttup *)lfirst(c);
			n_outer++;
			l_outer = MAX(l_outer, tup->te-tup->ts+1);
			us_outer = MIN(us_outer, tup->ts);
			ue_outer = MAX(ue_outer, tup->te);
		}
		foreach(c, (List *)inner_rel)
		{
			tup = (ttup *)lfirst(c);
			n_inner++;
			l_inner = MAX(l_inner, tup->te-tup->ts+1);
			us_inner = MIN(us_inner, tup->ts);
			ue_inner = MAX(ue_inner, tup->te);
		}
	}
	/* end evaluate dataset characteristics */


	if(us_outer < 1 || us_inner < 1)
	{
		fprintf(stderr, "ERROR: starting timestamps must be larger than 0!\n");
		fflush(stdout);
	}

	printf("# 1:outer_n \t 2:outer_l \t 3:outer_u \t 4:inner_n \t 5:inner_l \t 6:inner_u \t 7:k_oip_outer \t 8:k_oip_inner \t 9:p_oip_outer \t 10:p_oip_inner \t 11:t_oip_create \t 12:t_oip_join \t 13:io_oip_join \t 14:oip_match \t 15:oip_fhits \t 16:oip_memusage \t 17:t_rit_create \t 18:t_rit_join \t 19:io_rit_join \t 20:rit_match \t 21:rit_fhits \t 22:rit_memusage \t 23:t_qtree_create \t 24:t_qtree_join \t 25:io_qtree_join \t 26:qtree_match \t 27:qtree_fhits \t 28:qtree_memusage \t 29:t_lqtree_create \t 30:t_lqtree_join \t 31:io_lqtree_join \t 32:lqtree_match \t 33:lqtree_fhits \t 34:p_lqt_outer \t 35:p_lqt_inner \t 36:lqtree_memusage \t 37:t_merge_create \t 38:t_merge_join \t 39:io_merge_join \t 40:merge_match \t 41:merge_fhits \t 42:merge_memusage  \t 43:t_segtree_create \t 44:t_segtree_join \t 45:io_segtree_join \t 46:segtree_match \t 47:segtree_fhits \t 48:segtree_memusage \n");

	printf(" %ld \t %ld \t %ld \t %ld \t %ld \t %ld \t", n_outer, l_outer, ue_outer-us_outer+1, n_inner, l_inner, ue_inner-us_inner+1);


	/* OIP */
	if(params.oip && params.on_disk)
		run_OIP_DISK((HeapAm *)outer_rel, n_outer, us_outer, ue_outer, l_outer, (HeapAm *)inner_rel, n_inner, us_inner, ue_inner, l_inner, params.tight, params.ok, params.ik, params.k_idependent, params.cpu_io_cost);
	else if(params.oip && params.threads)
		run_OIP_MM_parallel((List *)outer_rel, n_outer, us_outer, ue_outer, l_outer, (List *)inner_rel, n_inner, us_inner, ue_inner, l_inner, params.tight, params.ok, params.ik, params.k_idependent, params.threads);
	else if(params.oip)
		run_OIP_MM((List *)outer_rel, n_outer, us_outer, ue_outer, l_outer, (List *)inner_rel, n_inner, us_inner, ue_inner, l_inner, params.tight, params.ok, params.ik, params.k_idependent);
	else
		printf(" x \t x \t x \t x \t x \t x \t x \t x \t x \t x \t ");

	/* RIT */
	if(params.rit && params.on_disk)
		run_RIT_DISK((HeapAm *)outer_rel, n_outer, us_outer, ue_outer, l_outer, (HeapAm *)inner_rel, n_inner, us_inner, ue_inner, l_inner, params.rit_clustered);
	else if(params.rit)
		run_RIT_MM((List *)outer_rel, n_outer, us_outer, ue_outer, l_outer, (List *)inner_rel, n_inner, us_inner, ue_inner, l_inner);
	else
		printf(" x \t x \t x \t x \t x \t x \t ");

	/* QTREE */
	if(params.qtree && params.on_disk)
		run_QTREE_DISK((HeapAm *)outer_rel, n_outer, us_outer, ue_outer, l_outer, (HeapAm *)inner_rel, n_inner, us_inner, ue_inner, l_inner, params.T);
	else if(params.qtree)
		run_QTREE_MM((List *)outer_rel, n_outer, us_outer, ue_outer, l_outer, (List *)inner_rel, n_inner, us_inner, ue_inner, l_inner, params.T);
	else
		printf(" x \t x \t x \t x \t x \t x \t ");

	/* LQTREE */
	if(params.lqtree && params.on_disk)
		run_LQTREE_DISK((HeapAm *)outer_rel, n_outer, us_outer, ue_outer, l_outer, (HeapAm *)inner_rel, n_inner, us_inner, ue_inner, l_inner, params.T);
	else if(params.lqtree)
		run_LQTREE_MM((List *)outer_rel, n_outer, us_outer, ue_outer, l_outer, (List *)inner_rel, n_inner, us_inner, ue_inner, l_inner, params.T);
	else
		printf(" x \t x \t x \t x \t x \t x \t x \t \t x \t");

	/* MERGEJOIN */
	if(params.mjoin && params.on_disk)
		run_MERGEJOIN_DISK((HeapAm *)outer_rel, n_outer, us_outer, ue_outer, l_outer, (HeapAm *)inner_rel, n_inner, us_inner, ue_inner, l_inner);
	else if(params.mjoin)
	{
		fprintf(stderr, "ERROR: Merge Join only implemented on disk!");
		exit(-1);
	}
	else
	{
		printf(" x \t x \t x \t x \t x \t x \t");
	}

	/* read File FRA */
	if(params.readFile && params.on_disk)
		run_readFile_Fra_DISK(params.ofn);
	else if(params.readFile)
	{
		fprintf(stderr, "ERROR: Read file only implemented on disk!");
		exit(-1);
	}
	else
	{
		printf(" x \t x \t x \t x \t x \t x \t");
	}

	if(params.eJoin && params.on_disk)
	{

		run_EQUIJOIN_DISK((HeapAm *)outer_rel, params.ofn, params.ifn);

	}
	else if(params.eJoin)
	{
		fprintf(stderr, "ERROR: Equijoin only implemented on disk!");
		exit(-1);
	}
	else
	{
		printf(" x \t x \t x \t x \t x \t x \t");
	}


	if(params.eJoinMM && params.on_disk)

		run_EQUIJOIN_MM((HeapAm *)outer_rel, params.ofn, params.ifn);
	else if(params.eJoinMM)
	{
		exit(-1);
	}
	else
	{
		printf(" x \t x \t x \t x \t x \t x \t");
	}
	if(params.antiJoin && params.on_disk)
		run_ANTIJOIN_DISK((HeapAm *)outer_rel, params.ofn, params.ifn);
	else if(params.antiJoin)
	{
		fprintf(stderr, "ERROR: AntiJoin only implemented on disk!");
		exit(-1);
	}
	else
	{
		printf(" x \t x \t x \t x \t x \t x \t");
	}

	if(params.aggregation && params.on_disk)
	{
		run_Partition_Fra_DISK((HeapAm *)outer_rel, n_outer, us_outer, ue_outer, l_outer, (HeapAm *)inner_rel, n_inner, us_inner, ue_inner, l_inner, params.ofn);
		run_AGGREGATION_DISK((HeapAm *)outer_rel, params.ofn);
	}
		else if(params.aggregation)
	{
		fprintf(stderr, "ERROR: Aggregation only implemented on disk!");
		exit(-1);
	}
	else
	{
		printf(" x \t x \t x \t x \t x \t x \t");
	}

	if(params.normalization && params.on_disk)
		run_NORMALIZATION_DISK((HeapAm *)outer_rel, params.ofn);
	else if(params.normalization)
	{
		fprintf(stderr, "ERROR: Normalization only implemented on disk!");
		exit(-1);
	}
	else
	{
		printf(" x \t x \t x \t x \t x \t x \t");
	}


	if(params.intersection && params.on_disk)

		run_INTERSECTION_DISK((HeapAm *)outer_rel,params.ofn, params.ifn);
	else if(params.intersection)
	{
		fprintf(stderr, "ERROR: Intersection only implemented on disk!");
		exit(-1);
	}
	else
	{
		printf(" x \t x \t x \t x \t x \t x \t");
	}


	/* Partitions not overlapping FRA */
	if(params.granPartition && params.on_disk)
	{
		run_Partition_Fra_DISK((HeapAm *)outer_rel, n_outer, us_outer, ue_outer, l_outer, (HeapAm *)inner_rel, n_inner, us_inner, ue_inner, l_inner, params.ofn);}
	else if(params.granPartition)
	{
		printf("on %d \n", params.on);
		fflush(stdout);


		run_DIP_MM((List *)outer_rel, n_outer, us_outer, ue_outer, l_outer, (List *)inner_rel, n_inner, us_inner, ue_inner, l_inner, params.ofn);
	}
	else
	{
		printf(" x \t x \t x \t x \t x \t x \t");
	}
	/* NESTLOOPJOIN */
	if(params.nestloop && params.on_disk)
		run_NESTLOOP_DISK((HeapAm *)outer_rel, n_outer, us_outer, ue_outer, l_outer, (HeapAm *)inner_rel, n_inner, us_inner, ue_inner, l_inner);
	else if(params.nestloop)
	{
		fprintf(stderr, "ERROR: Nested Loop Join only implemented on disk!");
		exit(-1);
	}

	/* SEGTREEJOIN */
	if(params.segtree && params.on_disk)
		run_SEGTREE_DISK((HeapAm *)outer_rel, n_outer, us_outer, ue_outer, l_outer, (HeapAm *)inner_rel, n_inner, us_inner, ue_inner, l_inner);
	else if(params.segtree)
		run_SEGTREE_MM((List *)outer_rel, n_outer, us_outer, ue_outer, l_outer, (List *)inner_rel, n_inner, us_inner, ue_inner, l_inner);
	else
	{
		printf(" x \t x \t x \t x \t x \t x \t");
	}

	printf("\n");

	if(params.on_disk)
	{
		freeDiskRel((HeapAm *)outer_rel);
		freeDiskRel((HeapAm *)inner_rel);
	}
	else
	{
		freeRel((List *)outer_rel);
		freeRel((List *)inner_rel);
	}

	printf("# mallocs: %ld, frees: %ld (mem_sze: %ld, curr_size: %ld, peak_size: %ld)\n", mallocs, frees, mem_size, cur_size, peak_size);
	return 0;
}


static void initParams(struct params *params)
{
	int i;
	int null = 0;
	for(i = 0; i < sizeof(struct params); i++)
		memcpy(((void*)params)+i, &null, 1);
}

static void
getParams( int argc, char *argv[], struct params *params ) {

	int c;
	static char short_options[] = "o:h:a:1:2:3:4:5:6:7:8:z:y:x:w:v:u:t:s:r:q:p:n:m:l:k:j:i";
	static struct option long_options[] =
	{
			{ "opt", required_argument, NULL, 'o' },
			{ "help", no_argument, NULL, 'h' },
			{ "algo", required_argument, NULL, 'a' },
			{ "on", required_argument, NULL, '1' },
			{ "ol", required_argument, NULL, '2' },
			{ "ous", required_argument, NULL, '3' },
			{ "oue", required_argument, NULL, '4' },
			{ "in", required_argument, NULL, '5' },
			{ "il", required_argument, NULL, '6' },
			{ "ius", required_argument, NULL, '7' },
			{ "iue", required_argument, NULL, '8' },
			{ "of", required_argument, NULL, 'z' },
			{ "if", required_argument, NULL, 'y' },
			{ "on-disk", no_argument, NULL, 'x' },
			{ "tight", no_argument, NULL, 'w' },
			{ "omean", required_argument, NULL, 'v' },
			{ "ostddev", required_argument, NULL, 'u' },
			{ "imean", required_argument, NULL, 't' },
			{ "istddev", required_argument, NULL, 's' },
			{ "ok", required_argument, NULL, 'r' },
			{ "ik", required_argument, NULL, 'q' },
			{ "k-independent", no_argument, NULL, 'p' },
			{ "density-based", required_argument, NULL, 'n' },
			{ "cpu_cost", required_argument, NULL, 'm'},
			{ "io_cost", required_argument, NULL, 'l'},
			{ "rit-clustered", no_argument, NULL, 'k'},
			{ "threads", required_argument, NULL, 'j'},
			{ "on-mem-disk", no_argument, NULL, 'i' },
			{NULL, 0, 0, 0}
	};

	initParams(params);

	while ( ( c = getopt_long( argc, argv, short_options, long_options, NULL ) )
			!= -1 ) {

		switch ( c ) {
		case 'o':
			params->opt = atoi( optarg );
			break;
		case 'h':
			printf("usage: %s -o num\n", argv[0]);
			printf("num:\n1 ... random\n2 ... from file\n");
			exit( 0 );
			break;
		case 'a':
			if ( strcasecmp( optarg, "oip" ) == 0 )
				params->oip = 1;
			else if ( strcasecmp( optarg, "rit" ) == 0 )
				params->rit = 1;
			else if ( strcasecmp( optarg, "qt" ) == 0 )
				params->qtree = 1;
			else if ( strcasecmp( optarg, "lqt" ) == 0 )
				params->lqtree = 1;
			else if ( strcasecmp( optarg, "mjoin" ) == 0 )
				params->mjoin = 1;
			else if ( strcasecmp( optarg, "eJoin" ) == 0 )
				params->eJoin = 1;
			else if ( strcasecmp( optarg, "eJoinMM" ) == 0 )
				params->eJoinMM = 1;
			else if ( strcasecmp( optarg, "antiJoin" ) == 0 )
				params->antiJoin = 1;
			else if ( strcasecmp( optarg, "aggregation" ) == 0 )
				params->aggregation = 1;
			else if ( strcasecmp( optarg, "normalization" ) == 0 )
				params->normalization = 1;
			else if ( strcasecmp( optarg, "intersection" ) == 0 )
				params->intersection = 1;
			else if ( strcasecmp( optarg, "DIP" ) == 0 )
				params->granPartition = 1;
			else if ( strcasecmp( optarg, "readFile" ) == 0 )
				params->readFile = 1;
			else if ( strcasecmp( optarg, "nestloop" ) == 0 )
				params->nestloop = 1;
			else if ( strcasecmp( optarg, "segtree" ) == 0 )
				params->segtree = 1;
			else
			{
				fprintf(stderr, "ERROR: Unrecognized option for -a \n");
				exit(-1);
			}
			break;
		case '1':
			params->on = atol( optarg );
			break;
		case '2':
			params->ol = atol( optarg );
			break;
		case '3':
			params->ous = atol( optarg );
			break;
		case '4':
			params->oue = atol( optarg );
			break;
		case '5':
			params->in = atol( optarg );
			break;
		case '6':
			params->il = atol( optarg );
			break;
		case '7':
			params->ius = atol( optarg );
			break;
		case '8':
			params->iue = atol( optarg );
			break;
		case 'z':
			params->ofn = strdup( optarg );
			break;
		case 'y':
			params->ifn = strdup( optarg );
			break;
		case 'x':
			params->on_disk = 1;
			break;
		case 'w':
			params->tight = 1;
			break;
		case 'v':
			params->omean = atol( optarg );
			break;
		case 'u':
			params->ostdev = atol( optarg );
			break;
		case 't':
			params->imean = atol( optarg );
			break;
		case 's':
			params->istdev = atol( optarg );
			break;
		case 'r':
			params->ok = atoi( optarg );
			break;
		case 'q':
			params->ik = atoi( optarg );
			break;
		case 'p':
			params->k_idependent = 1;
			break;
		case 'n':
			params->T = atoi( optarg );
			break;
		case 'm':
			params->cpu_io_cost = 1;
			cpu_cost = atof( optarg );
			break;
		case 'l':
			params->cpu_io_cost = 1;
			io_cost = atof( optarg );
			break;
		case 'k':
			params->rit_clustered = true;
			break;
		case 'j':
			params->threads = atoi( optarg );
			break;
		case 'i':
			params->on_mem_disk = 1;
			break;
		case '?':
			printf("usage: %s -o num\n", argv[0]);
			printf("num:\n1 ... random\n2 ... from file\n");
			exit( 0 );
			break;
		}
	}
}

static void
run_QTREE_MM_helper(List *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		List *irel, long in, timestmp ius, timestmp iue, timestmp il, size_t loose, int T);
static void
run_QTREE_helper_disk(HeapAm *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		HeapAm *irel, long in, timestmp ius, timestmp iue, timestmp il, size_t loose, int T);

static void
run_QTREE_MM(List *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		List *irel, long in, timestmp ius, timestmp iue, timestmp il, int T)
{
	run_QTREE_MM_helper(orel, on, ous, oue, ol, irel, in, ius, iue, il, 1, T);
}

static void
run_QTREE_DISK(HeapAm *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		HeapAm *irel, long in, timestmp ius, timestmp iue, timestmp il, int T)
{
	run_QTREE_helper_disk(orel, on, ous, oue, ol, irel, in, ius, iue, il, 1, T);
}

static void
run_LQTREE_MM(List *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		List *irel, long in, timestmp ius, timestmp iue, timestmp il, int T)
{
	run_QTREE_MM_helper(orel, on, ous, oue, ol, irel, in, ius, iue, il, 2, T);
}

static void
run_LQTREE_DISK(HeapAm *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		HeapAm *irel, long in, timestmp ius, timestmp iue, timestmp il, int T)
{
	run_QTREE_helper_disk(orel, on, ous, oue, ol, irel, in, ius, iue, il, 2, T);
}

static void
run_QTREE_MM_helper(List *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		List *irel, long in, timestmp ius, timestmp iue, timestmp il, size_t loose, int T)
{
	QTREE *outer_qtree, *inner_qtree;
	size_t mem_usage;
	void *thandle;
	long long time;
	long match, fhits;

	if(loose == 1)
		fprintf(stderr, "Creating QTREE ... ");
	else
		fprintf(stderr, "Creating LQTREE ... ");
	fflush(stderr);
	mem_usage = cur_size;
	thandle = time_start();
	outer_qtree = qtree_create(orel, ous, oue, loose, T);
	inner_qtree = qtree_create(irel, ius, iue, loose, T);
	time = time_end(thandle);
	mem_usage = cur_size-mem_usage;
	fprintf(stderr, "done.\n");
	fflush(stderr);

	printf(" %lld \t", time);

	if(loose == 1)
		fprintf(stderr, "Joining QTREE ... ");
	else
		fprintf(stderr, "Joining LQTREE ... ");
	fflush(stderr);
	match = 0;
	fhits = 0;
	thandle = time_start();
	qtree_Join(outer_qtree, inner_qtree, &match, &fhits);
	time = time_end(thandle);
	fprintf(stderr, "done.\n");
	fflush(stderr);

	printf(" %lld \t", time);
	printf(" %ld \t", 0L);
	printf(" %ld \t", match);
	printf(" %ld \t", fhits);
	printf(" %ld \t", qtree_num_partitions(outer_qtree));
	printf(" %ld \t", qtree_num_partitions(inner_qtree));
	printf(" %ld \t", mem_usage);
	fflush(stdout);

	qtree_close(outer_qtree);
	qtree_close(inner_qtree);
}

static void
run_QTREE_helper_disk(HeapAm *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		HeapAm *irel, long in, timestmp ius, timestmp iue, timestmp il, size_t loose, int T)
{
	QTREE *outer_qtree, *inner_qtree;
	size_t mem_usage;
	void *thandle;
	long long time;
	long match, fhits, ios;

	if(loose == 1)
		fprintf(stderr, "Creating QTREE ... ");
	else
		fprintf(stderr, "Creating LQTREE ... ");
	fflush(stderr);
	mem_usage = cur_size;
	thandle = time_start();

	File *oStorageFile = file_create("qtree_outer_storage.dat", orel->f->f->type);
	Buffer *oStorageBuffer = buffer_create(oStorageFile, WORK_MEM, POLICY_LRU);
	PartitionAm *oStorage = ExecInitPartitionAm(oStorageBuffer, orel->htupsze);
	File *iStorageFile = file_create("qtree_inner_storage.dat", irel->f->f->type);
	Buffer *iStorageBuffer = buffer_create(iStorageFile, WORK_MEM, POLICY_LRU);
	PartitionAm *iStorage = ExecInitPartitionAm(iStorageBuffer, irel->htupsze);

	outer_qtree = qtree_create_disk(orel, oStorage, ous, oue, loose, T);
	inner_qtree = qtree_create_disk(irel, iStorage, ius, iue, loose, T);
	buffer_flush(oStorageBuffer);
	buffer_flush(iStorageBuffer);

	time = time_end(thandle);
	mem_usage = cur_size-mem_usage;
	fprintf(stderr, "done.\n");
	fflush(stderr);

	printf(" %lld \t", time);

	buffer_clear(iStorageBuffer);
	buffer_clear(oStorageBuffer);

	if(loose == 1)
		fprintf(stderr, "Joining QTREE ... ");
	else
		fprintf(stderr, "Joining LQTREE ... ");
	fflush(stderr);
	match = 0;
	fhits = 0;
	ios=0;
	buffer_clearIOStat(oStorageBuffer);
	buffer_clearIOStat(iStorageBuffer);
	thandle = time_start();
	qtree_Join_disk(outer_qtree, oStorage, inner_qtree, iStorage, &match, &fhits);
	time = time_end(thandle);
	fprintf(stderr, "done.\n");
	fflush(stderr);

	ios = buffer_numReads(oStorageBuffer) + buffer_numReads(iStorageBuffer);

	printf(" %lld \t", time);
	printf(" %ld \t", ios);
	printf(" %ld \t", match);
	printf(" %ld \t", fhits);
	printf(" %ld \t", qtree_num_partitions(outer_qtree));
	printf(" %ld \t", qtree_num_partitions(inner_qtree));
	printf(" %ld \t", mem_usage);
	fflush(stdout);

	qtree_close_disk(outer_qtree);
	qtree_close_disk(inner_qtree);

	ExecEndPartitionAm(oStorage);
	ExecEndPartitionAm(iStorage);
	buffer_close(oStorageBuffer);
	buffer_close(iStorageBuffer);
	file_close(oStorageFile);
	file_close(iStorageFile);
}

static void
run_RIT_MM(List *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		List *irel, long in, timestmp ius, timestmp iue, timestmp il)
{
	RIT *outer_rit, *inner_rit;
	size_t mem_usage;
	void *thandle;
	long long time;
	long match, fhits;

	fprintf(stderr, "Creating RIT ... ");
	fflush(stderr);
	mem_usage = cur_size;
	thandle = time_start();
	outer_rit = rit_create(orel);
	inner_rit = rit_create(irel);
	time = time_end(thandle);
	mem_usage = cur_size - mem_usage;
	fprintf(stderr, "done.\n");
	fflush(stderr);

	printf(" %lld \t", time);

	fprintf(stderr, "Joining RIT ... ");
	fflush(stderr);
	match = 0;
	fhits = 0;
	thandle = time_start();
	rit_UpDownJoin(outer_rit, inner_rit, &match, &fhits);
	time = time_end(thandle);
	fprintf(stderr, "done.\n");
	fflush(stderr);

	printf(" %lld \t", time);
	printf(" %ld \t", 0L);
	printf(" %ld \t", match);
	printf(" %ld \t", fhits);
	printf(" %ld \t", mem_usage);
	fflush(stdout);

	rit_close(outer_rit);
	rit_close(inner_rit);
}

static void
run_RIT_DISK(HeapAm *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		HeapAm *irel, long in, timestmp ius, timestmp iue, timestmp il, bool clustered)
{
	RIT *outer_rit, *inner_rit;
	size_t mem_usage;
	void *thandle;
	long long time;
	long match, fhits, ios;

	fprintf(stderr, "Creating RIT ... ");
	fflush(stderr);
	mem_usage = cur_size;
	thandle = time_start();

	/* create outer RIT */
	File *oStorageFile = NULL;
	Buffer *oStorageBuffer = NULL;
	HeapAm *oStorage = NULL;
	if(clustered)
	{
		oStorageFile = file_create("rit_outer_storage.dat", orel->f->f->type);
		oStorageBuffer = buffer_create(oStorageFile, WORK_MEM, POLICY_LRU);
		oStorage = ExecInitHeapAm(oStorageBuffer, orel->htupsze);
	}
	outer_rit = rit_create_disk(orel, oStorage);

	/* create inner RIT */
	File *iStorageFile = NULL;
	Buffer *iStorageBuffer = NULL;
	HeapAm *iStorage = NULL;
	if(clustered)
	{
		iStorageFile = file_create("rit_inner_storage.dat", irel->f->f->type);
		iStorageBuffer = buffer_create(iStorageFile, WORK_MEM, POLICY_LRU);
		iStorage = ExecInitHeapAm(iStorageBuffer, irel->htupsze);
	}
	inner_rit = rit_create_disk(irel, iStorage);

	if(outer_rit->isClusetered)
		buffer_clear(oStorageBuffer);
	if(inner_rit->isClusetered)
		buffer_clear(iStorageBuffer);
	time = time_end(thandle);
	mem_usage = cur_size - mem_usage;
	fprintf(stderr, "done.\n");
	fflush(stderr);

	printf(" %lld \t", time);

	if(outer_rit->isClusetered)
		buffer_clearIOStat(oStorageBuffer);
	if(inner_rit->isClusetered)
		buffer_clearIOStat(iStorageBuffer);

	buffer_clear(orel->f);
	buffer_clear(irel->f);
	buffer_clearIOStat(orel->f);
	buffer_clearIOStat(irel->f);

	fprintf(stderr, "Joining RIT ... ");
	fflush(stderr);
	match = 0;
	fhits = 0;
	thandle = time_start();
	rit_UpDownJoin(outer_rit, inner_rit, &match, &fhits);
	time = time_end(thandle);
	fprintf(stderr, "done.\n");
	fflush(stderr);

	ios = 0;

	if(outer_rit->isClusetered)
		ios += buffer_numReads(oStorageBuffer);
	if(inner_rit->isClusetered)
		ios += buffer_numReads(iStorageBuffer);
	ios += buffer_numReads(orel->f);
	ios += buffer_numReads(irel->f);

	printf(" %lld \t", time);
	printf(" %ld \t", ios);
	printf(" %ld \t", match);
	printf(" %ld \t", fhits);
	printf(" %ld \t", mem_usage);
	fflush(stdout);

	rit_close(outer_rit);
	rit_close(inner_rit);

	if(clustered)
	{
		ExecEndHeapAM(oStorage);
		ExecEndHeapAM(iStorage);
		buffer_close(oStorageBuffer);
		buffer_close(iStorageBuffer);
		file_close(oStorageFile);
		file_close(iStorageFile);
	}
}

static void
run_OIP_MM(List *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		List *irel, long in, timestmp ius, timestmp iue, timestmp il, int tight, int ok, int ik, int k_independent)
{
	int k_outer, k_inner;
	timestmp d_outer, d_inner;
	timestmp o_outer, o_inner;
	OIP *outer_oip, *inner_oip;

	size_t mem_usage;
	void *thandle;
	long long time;
	long match, fhits;

	long n_outer = on;
	long n_inner = in;

	double outer_thightening_factor = 1.0;
	double inner_thightening_factor = 1.0;
	if(tight)
		outer_thightening_factor = ((double)ol)/(oue-ous+1);
	if(tight)
		inner_thightening_factor = ((double)il)/(iue-ius+1);

	fprintf(stderr, "Creating OIP MEM... ");
	fflush(stderr);
	mem_usage = cur_size;
	thandle = time_start();

	// create outer OIP
	if(ok > 0)
		k_outer = ok;
	else
	{
		k_outer = calculate_k_outer(n_inner, 1, inner_thightening_factor, iue-ius+1, n_outer, outer_thightening_factor, oue-ous+1, 0.0, 1.0);
	}

	d_outer = (timestmp) ceill((long double) (oue-ous+1) / k_outer);
	o_outer = ous;
	outer_oip = oip_create(orel, k_outer, d_outer, o_outer);

	// create inner oip
	if(ik > 0)
		k_inner = ik;
	else if(k_independent)
		k_inner = (int) ceil(pow(((double) 1.5 * n_inner), 1.0 / 3.0) / pow(inner_thightening_factor, 1.0 / 3.0));
	else
	{
		k_inner = k_outer;
	}
	d_inner = (timestmp) ceill((long double) (iue-ius+1) / k_inner);
	o_inner = ius;
	inner_oip = oip_create(irel, k_inner, d_inner, o_inner);

	time = time_end(thandle);
	mem_usage = cur_size - mem_usage;
	fprintf(stderr, " done.\n");
	fflush(stderr);

	printf(" %d \t", k_outer);
	printf(" %d \t", k_inner);
	printf("oip_num_partitions %ld \t", oip_num_partitions(outer_oip));
	printf("oip_num_partitions %ld \t", oip_num_partitions(inner_oip));
	printf("TIME OIP %lld \t", time);

	fprintf(stderr, "Joining OIP ... ");
	fflush(stderr);
	match = 0;
	fhits = 0;
	thandle = time_start();
	oip_join(outer_oip, inner_oip, &match, &fhits);
	time = time_end(thandle);
	fprintf(stderr, "done.\n");
	fflush(stderr);

	printf("TIME JOIN %lld \t", time);
	printf(" %ld \t", 0L);
	printf("MATCHES %ld \t", match);
	printf("FHITS %ld \t", fhits);
	printf("COMPARISONS %ld \t", match+fhits);
	printf(" %ld \t", mem_usage);
	fflush(stdout);

	oip_close(outer_oip);
	oip_close(inner_oip);
}

static void
run_OIP_MM_parallel(List *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		List *irel, long in, timestmp ius, timestmp iue, timestmp il, int tight, int ok, int ik, int k_independent, int threads)
{
	int k_outer, k_inner;
	timestmp d_outer, d_inner;
	timestmp o_outer, o_inner;
	OIP *outer_oip, *inner_oip;
	size_t outer_p_count = 0;
	size_t inner_p_count = 0;

	fprintf(stderr, "LOG: OIP PARALLEL %d threads\n", threads);

	size_t mem_usage;
	void *thandle;
	long long time;
	long match, fhits;

	long n_outer = on;
	long n_inner = in;

	double outer_thightening_factor = 1.0;
	double inner_thightening_factor = 1.0;
	if(tight)
		outer_thightening_factor = ((double)ol)/(oue-ous+1);
	if(tight)
		inner_thightening_factor = ((double)il)/(iue-ius+1);

	fprintf(stderr, "Creating OIP ... ");
	fflush(stderr);
	mem_usage = cur_size;
	thandle = time_start();

	/* OLD */
	// create outer OIP
	if(ok > 0)
		k_outer = ok;
	else
	{
		k_outer = calculate_k_outer(n_inner, 1, inner_thightening_factor, iue-ius+1, n_outer, outer_thightening_factor, oue-ous+1, 0.0, 1.0);
	}

	d_outer = (timestmp) ceill((long double) (oue-ous+1) / k_outer);
	o_outer = ous;
	outer_oip = oip_create(orel, k_outer, d_outer, o_outer);
	/* get effective number of outer partitions */
	outer_p_count = oip_num_partitions(outer_oip);

	// create inner oip
	if(ik > 0)
		k_inner = ik;
	else if(k_independent)
		k_inner = (int) ceil(pow(((double) 1.5 * n_inner), 1.0 / 3.0) / pow(inner_thightening_factor, 1.0 / 3.0));
	else
	{
		k_inner = k_outer;//calculate_k(outer_p_count, n_inner, 1, inner_thightening_factor, n_outer, 0, 1.0);
	}
	d_inner = (timestmp) ceill((long double) (iue-ius+1) / k_inner);
	o_inner = ius;
	inner_oip = oip_create(irel, k_inner, d_inner, o_inner);
	inner_p_count = oip_num_partitions(inner_oip);

	time = time_end(thandle);
	mem_usage = cur_size - mem_usage;
	fprintf(stderr, " done.\n");
	fflush(stderr);

	printf(" %d \t", k_outer);
	printf(" %d \t", k_inner);
	printf(" %ld \t", outer_p_count);
	printf(" %ld \t", inner_p_count);
	printf(" %lld \t", time);

	fprintf(stderr, "Joining OIP ... ");
	fflush(stderr);
	match = 0;
	fhits = 0;
	thandle = time_start();
	oip_join_parallel(outer_oip, inner_oip, &match, &fhits, threads);
	time = time_end(thandle);
	fprintf(stderr, "done.\n");
	fflush(stderr);

	printf(" %lld \t", time);
	printf(" %ld \t", 0L);
	printf(" %ld \t", match);
	printf(" %ld \t", fhits);
	printf(" %ld \t", mem_usage);
	fflush(stdout);

	oip_close(outer_oip);
	oip_close(inner_oip);
}

static void
run_OIP_DISK(HeapAm *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		HeapAm *irel, long in, timestmp ius, timestmp iue, timestmp il, int tight, int ok, int ik, int k_independent, int cpu_io_cost)
{
	int k_outer, k_inner;
	timestmp d_outer, d_inner;
	timestmp o_outer, o_inner;
	OIP *outer_oip, *inner_oip;

	size_t mem_usage;
	void *thandle;
	long long time;
	long match, fhits, ios;

	long n_outer = on;
	long n_inner = in;

	double outer_thightening_factor = 1.0;
	double inner_thightening_factor = 1.0;
	if(tight)
		outer_thightening_factor = ((double)ol)/(oue-ous+1);
	if(tight)
		inner_thightening_factor = ((double)il)/(iue-ius+1);

	fprintf(stderr, "Creating OIP DISK... ");
	fflush(stderr);
	mem_usage = cur_size;
	thandle = time_start();

	/* create outer OIP */
	File *oStorageFile = file_create("oip_outer_storage.dat", orel->f->f->type);
	//Buffer *oStorageBuffer = buffer_create(oStorageFile, WORK_MEM, POLICY_LRU);
	Buffer *oStorageBuffer;
	if(orel->f->f->type == TYPE_MEM)
		oStorageBuffer = buffer_create_void(oStorageFile);
	else
		oStorageBuffer = buffer_create(oStorageFile, WORK_MEM, POLICY_LRU);
	PartitionAm *oStorage = ExecInitPartitionAm(oStorageBuffer, orel->htupsze);

	if(ok > 0)
		k_outer = ok;
	else if(cpu_io_cost)
	{
		k_outer = calculate_k_outer(n_inner, irel->tups_per_page, inner_thightening_factor, iue-ius+1, n_outer, outer_thightening_factor, oue-ous+1, io_cost, cpu_cost);
	}
	else
	{
		fprintf(stderr, "ERROR: specify a k option for OIP!\n");
		exit(-1);
	}

	d_outer = (timestmp) ceill((long double) (oue-ous+1) / k_outer);
	o_outer = ous;
	outer_oip = oip_create_disk(orel, oStorage, k_outer, d_outer, o_outer);
	buffer_flush(oStorageBuffer);

	/* create inner oip */
	File *iStorageFile = file_create("oip_inner_storage.dat", irel->f->f->type);
	//Buffer *iStorageBuffer = buffer_create(iStorageFile, WORK_MEM, POLICY_LRU);
	Buffer *iStorageBuffer;
	if(irel->f->f->type == TYPE_MEM)
		iStorageBuffer = buffer_create_void(iStorageFile);
	else
		iStorageBuffer = buffer_create(iStorageFile, WORK_MEM, POLICY_LRU);

	PartitionAm *iStorage = ExecInitPartitionAm(iStorageBuffer, irel->htupsze);

	if(ik > 0)
		k_inner = ik;
	else if(k_independent)
		k_inner = (int) ceil(pow((double) 1.5 * (n_outer / (orel->tups_per_page * inner_thightening_factor)), 1.0 / 3.0));
	else if(cpu_io_cost)
	{
		k_inner = k_outer;
	}
	else
	{
		fprintf(stderr, "ERROR: specify a k option for OIP!\n");
		exit(-1);
	}

	d_inner = (timestmp) ceill((long double) (iue-ius+1) / k_inner);
	o_inner = ius;
	inner_oip = oip_create_disk(irel, iStorage, k_inner, d_inner, o_inner);
	buffer_flush(iStorageBuffer);
	time = time_end(thandle);
	mem_usage = cur_size - mem_usage;
	fprintf(stderr, " done.\n");
	fflush(stderr);

	printf(" %d \t", k_outer);
	printf(" %d \t", k_inner);
	printf("OPartitions %ld \t", oip_num_partitions(outer_oip));
	printf("IPartitions %ld \t", oip_num_partitions(inner_oip));
	printf("TIME OIP %lld \t", time);

	buffer_clear(iStorageBuffer);
	buffer_clear(oStorageBuffer);

	fprintf(stderr, "Joining OIP ... ");
	fflush(stderr);
	match = 0;
	fhits = 0;
	ios=0;
	buffer_clearIOStat(oStorageBuffer);
	buffer_clearIOStat(iStorageBuffer);
	thandle = time_start();
	oip_join_disk(outer_oip, oStorage, inner_oip, iStorage, &match, &fhits);
	time = time_end(thandle);
	fprintf(stderr, "done.\n");
	fflush(stderr);

	ios = buffer_numReads(oStorageBuffer) + buffer_numReads(iStorageBuffer);

	printf("TIME JOIN %lld \t", time);
	printf(" %ld \t", ios);
	printf("Matches %ld \t", match);
	printf("False Hits %ld \t", fhits);
	printf(" %ld \t", mem_usage);
	fflush(stdout);

	oip_close_disk(outer_oip);
	oip_close_disk(inner_oip);

	ExecEndPartitionAm(oStorage);
	ExecEndPartitionAm(iStorage);
	buffer_close(oStorageBuffer);
	buffer_close(iStorageBuffer);
	file_close(oStorageFile);
	file_close(iStorageFile);
}

/*
static int calculate_k(long p_outer, long ni, long bi, double fi, long no, double io_cost, double cpu_cost)
{
  int k;
  k = (int) pow((3.0 * (((double) p_outer * ni) * io_cost/((double)bi) + ((double) 2.0 * no * ni) * cpu_cost)) / (2.0 * ((double)p_outer) * (io_cost+cpu_cost) * fi), 1.0/3.0);
  // avoid over tightening
  if(fi < 1)
  {
    k = MIN(k, sqrt((3*(((double) p_outer * ni) * io_cost/((double)bi) + ((double) 2.0 * no * ni) * cpu_cost)) / (2.0 * ((double)p_outer) * (io_cost+cpu_cost))));
  }
  return k;
}

static int calculate_k_outer_old(long ni, long bi, double fi, long no, double fo, long uo, double io_cost, double cpu_cost)
{
  int k, k_prev, i;
  long p_outer = no;
  long l, d, used_p;

  l = (long) ceil(fo * uo);

  k = calculate_k(p_outer, ni, bi, fi, no, io_cost, cpu_cost);
  for(i = 0; i < 50; i++)
  {
    k_prev = k;
    d = (long) ceil(((double) uo) / k);
    used_p = k * ceil(((double) l) / d) + k - ceil(((double) l) / d) * ceil(((double) l) / d) / 2 - ceil(((double) l) / d) / 2;
    used_p = MIN(used_p, (((double)k)*k+k)/2);
    p_outer = MIN(used_p, no);
    k = calculate_k(p_outer, ni, bi, fi, no, io_cost, cpu_cost);
  }

  if(abs(k - k_prev) > 1)
  {
    fprintf(stderr, "WARNING: k outer not converged!\n");
    fprintf(stderr, "no=%ld, ni=%ld, bi=%ld, fi=%f, fo=%f, io_cost=%f, cpu_cost=%f\n", no, ni, bi, fi, fo, io_cost, cpu_cost);
    fprintf(stderr, "k=%d, k_rev=%d\n", k, k_prev);
    k = (int)(((double)k) + k_prev) / 2;
    fprintf(stderr, "taking k=%d\n", k);
  }
  else
  {
    fprintf(stderr, "LOG: k outer converged!\n");
  }

  return k;
}*/

/*
 * returns a tightening factor between (0,1] according to the ratio of existing partitions
 */
static double calculate_tightening_factor(int k, timestmp l, timestmp u)
{
	double d = ceil(((double)u)/k);
	double covered_granules = ceil(((double)l)/d);
	double k_double = (double) k;

	double f = (2*k_double*covered_granules + 2*k_double - covered_granules*covered_granules - covered_granules)
            						 /
									 (k_double*k_double+k_double);
	return f;
}

static int calculate_k_outer(long ni, long bi, double fi, timestmp ui, long no, double fo, timestmp uo, double io_cost, double cpu_cost)
{
	long po = no;
	int i, niter;
	int k, k_prev;

	timestmp lo, li;

	lo = (timestmp)ceil(fo*uo);
	li = (timestmp)ceil(fi*ui);

	/* normalize costs to cpu_cost + io_cost = 1 */
	double normal_io_cost = 1.0/(io_cost+cpu_cost) * io_cost;
	double normal_cpu_cost = 1.0 - normal_io_cost;

	niter = 50;
	k = 1;
	for(i = 0; i < niter; i++)
	{
		k_prev = k;
		po = MIN((long)(((((double)k)*k+k)/2) * calculate_tightening_factor(k, lo, uo)), no);
		k = (int) pow((3.0 * ((double)ni))/(2.0 * calculate_tightening_factor(k, li, ui)) * (normal_io_cost/((double)bi) + ((2.0 * ((double)no) * normal_cpu_cost) / ((double)po))) , 1.0/3.0);
		if(k == k_prev)
			break;
	}

	if(abs(k - k_prev) > 1)
	{
		fprintf(stderr, "WARNING: k-bound not well converged after %d steps!\n", i);
		fprintf(stderr, "Details:\n");
		fprintf(stderr, "no: \t%ld \n", no);
		fprintf(stderr, "ni: \t%ld \t", ni);
		fprintf(stderr, "bi: \t%ld \n", bi);
		fprintf(stderr, "uo: \t%ld \t", uo);
		fprintf(stderr, "lo: \t%ld \t(%f) \n", lo, fo);
		fprintf(stderr, "ui: \t%ld \t", ui);
		fprintf(stderr, "li: \t%ld \t(%f) \n", li, fi);
		fprintf(stderr, "cpu_cost: \t%f\t io_cost: \t%f \n", cpu_cost, io_cost);
		fprintf(stderr, "normal_cpu_cost: \t%f\t normal_io_cost: \t%f \n", normal_cpu_cost, normal_io_cost);

		fprintf(stderr, "k_prev: \t%d \t", k_prev);
		fprintf(stderr, "fo: \t%f \t", calculate_tightening_factor(k_prev, lo, uo));
		fprintf(stderr, "fi: \t%f \n", calculate_tightening_factor(k_prev, li, ui));

		fprintf(stderr, "k: \t%d \t", k);
		fprintf(stderr, "fo: \t%f \t", calculate_tightening_factor(k, lo, uo));
		fprintf(stderr, "fi: \t%f \n", calculate_tightening_factor(k, li, ui));

		k = (int)(((double)k) + k_prev) / 2;
		fprintf(stderr, "Taking k=%d\n", k);
	}
	else
	{
		fprintf(stderr, "LOG: k-bound converged in %d steps!\n", i);
		fprintf(stderr, "Details:\n");
		fprintf(stderr, "no: \t%ld \n", no);
		fprintf(stderr, "ni: \t%ld \t", ni);
		fprintf(stderr, "bi: \t%ld \n", bi);
		fprintf(stderr, "uo: \t%ld \t", uo);
		fprintf(stderr, "lo: \t%ld \t(%f) \n", lo, fo);
		fprintf(stderr, "ui: \t%ld \t", ui);
		fprintf(stderr, "li: \t%ld \t(%f) \n", li, fi);
		fprintf(stderr, "cpu_cost: \t%f\t io_cost: \t%f \n", cpu_cost, io_cost);
		fprintf(stderr, "normal_cpu_cost: \t%f\t normal_io_cost: \t%f \n", normal_cpu_cost, normal_io_cost);

		fprintf(stderr, "k_prev: \t%d \t", k_prev);
		fprintf(stderr, "fo: \t%f \t", calculate_tightening_factor(k_prev, lo, uo));
		fprintf(stderr, "fi: \t%f \n", calculate_tightening_factor(k_prev, li, ui));

		fprintf(stderr, "k: \t%d \t", k);
		fprintf(stderr, "fo: \t%f \t", calculate_tightening_factor(k, lo, uo));
		fprintf(stderr, "fi: \t%f \n", calculate_tightening_factor(k, li, ui));
	}

	return k;
}

/* Calculation of k without normalization to cpu_cost + io_cost = 1
static int calculate_k_outer(long ni, long bi, double fi, timestmp ui, long no, double fo, timestmp uo, double io_cost, double cpu_cost)
{
  long po = no;
  int i, niter;
  int k, k_prev;

  timestmp lo, li;

  lo = (timestmp)ceil(fo*uo);
  li = (timestmp)ceil(fi*ui);

  niter = 50;
  k = 1;
  for(i = 0; i < niter; i++)
  {
    //printf("k\t%d\n", k);
    k_prev = k;
    po = MIN((long)(((((double)k)*k+k)/2) * calculate_tightening_factor(k, lo, uo)), no);
    k = (int) pow(3.0/2.0 * (((double)ni)*io_cost / (bi * (cpu_cost+io_cost)  * calculate_tightening_factor(k, li, ui)) + 2.0*((double)ni)*((double)no)*cpu_cost / (((double)po) *(io_cost+cpu_cost) * calculate_tightening_factor(k, li, ui))), 1.0/3.0);
    if(k == k_prev)
      break;
  }
  //printf("k\t%d\n", k);

  if(abs(k - k_prev) > 1)
    {
      fprintf(stderr, "WARNING: k-bound not well converged after %d steps!\n", i);
      fprintf(stderr, "Details:\n");
      fprintf(stderr, "no: \t%ld \n", no);
      fprintf(stderr, "ni: \t%ld \t", ni);
      fprintf(stderr, "bi: \t%ld \n", bi);
      fprintf(stderr, "uo: \t%ld \t", uo);
      fprintf(stderr, "lo: \t%ld \t(%f) \n", lo, fo);
      fprintf(stderr, "ui: \t%ld \t", ui);
      fprintf(stderr, "li: \t%ld \t(%f) \n", li, fi);
      fprintf(stderr, "cpu_cost: \t%f\t io_cost: \t%f \n", cpu_cost, io_cost);

      fprintf(stderr, "k_prev: \t%d \t", k_prev);
      fprintf(stderr, "fo: \t%f \t", calculate_tightening_factor(k_prev, lo, uo));
      fprintf(stderr, "fi: \t%f \n", calculate_tightening_factor(k_prev, li, ui));

      fprintf(stderr, "k: \t%d \t", k);
      fprintf(stderr, "fo: \t%f \t", calculate_tightening_factor(k, lo, uo));
      fprintf(stderr, "fi: \t%f \n", calculate_tightening_factor(k, li, ui));

      k = (int)(((double)k) + k_prev) / 2;
      fprintf(stderr, "Taking k=%d\n", k);
    }
    else
    {
      fprintf(stderr, "LOG: k-bound converged in %d steps!\n", i);
      fprintf(stderr, "Details:\n");
      fprintf(stderr, "no: \t%ld \n", no);
      fprintf(stderr, "ni: \t%ld \t", ni);
      fprintf(stderr, "bi: \t%ld \n", bi);
      fprintf(stderr, "uo: \t%ld \t", uo);
      fprintf(stderr, "lo: \t%ld \t(%f) \n", lo, fo);
      fprintf(stderr, "ui: \t%ld \t", ui);
      fprintf(stderr, "li: \t%ld \t(%f) \n", li, fi);
      fprintf(stderr, "cpu_cost: \t%f\t io_cost: \t%f \n", cpu_cost, io_cost);

      fprintf(stderr, "k_prev: \t%d \t", k_prev);
      fprintf(stderr, "fo: \t%f \t", calculate_tightening_factor(k_prev, lo, uo));
      fprintf(stderr, "fi: \t%f \n", calculate_tightening_factor(k_prev, li, ui));

      fprintf(stderr, "k: \t%d \t", k);
      fprintf(stderr, "fo: \t%f \t", calculate_tightening_factor(k, lo, uo));
      fprintf(stderr, "fi: \t%f \n", calculate_tightening_factor(k, li, ui));
    }

  return k;
}*/

int mainX(int argc, char *argv[])
{

	long no = 10000000;
	long ni = 100000000;
	double fi = ((double)16777)/16777215;
	double fo = ((double)16777)/16777215;
	double io_cost = 1.0;
	double cpu_cost = 0.1;
	long bi = 14;

	printf("%d\n", calculate_k_outer(ni, bi, fi, 16777215, no, fo, 16777215, io_cost, cpu_cost));

	return 0;

}



static int cmp_timestmp_timestmp(const void *a, const void *b)
{
  int ret = (*(timestmp*)a > *(timestmp*)b) - (*(timestmp*)a < *(timestmp*)b);
  if(ret == 0)
    ret = (*(timestmp*)(a+sizeof(timestmp)) < *(timestmp*)(b+sizeof(timestmp))) - (*(timestmp*)(a+sizeof(timestmp)) > *(timestmp*)(b+sizeof(timestmp)));
  return ret;
}

static int cmp_timestmp_start(const void *a, const void *b)
{
	return (*(timestmp*)a > *(timestmp*)b) - (*(timestmp*)a < *(timestmp*)b);
}

static int cmp_timestmp_end(const void *a, const void *b)
{
	return (*(timestmp*)(a+sizeof(timestmp)) > *(timestmp*)(b+sizeof(timestmp))) - (*(timestmp*)(a+sizeof(timestmp)) < *(timestmp*)(b+sizeof(timestmp)));
}


static void
run_MERGEJOIN_DISK(HeapAm *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		HeapAm *irel, long in, timestmp ius, timestmp iue, timestmp il)
{
	size_t mem_usage;
	void *thandle;
	long long time;
	long match, fhits, ios;

	fprintf(stderr, "Creating BLOCK MERGEJOIN ... ");
	fflush(stderr);
	mem_usage = cur_size;
	thandle = time_start();

	File *oStorageFile = file_create("mergejoin_outer_storage.dat", orel->f->f->type);
	Buffer *oStorageBuffer = buffer_create(oStorageFile, WORK_MEM, POLICY_LRU);
	HeapAm *oStorage = ExecInitHeapAm(oStorageBuffer, orel->htupsze);
	Sort *sort1 = ExecInitSort(orel, cmp_timestmp_end, orel->htupsze, WORK_MEM, orel->f->f->type);
	ttup *tup;
	while((tup = ExecSort(sort1)) != NULL)
		ExecHeapAmInsert(oStorage, tup);
	ExecEndSort(sort1);

	File *iStorageFile = file_create("mergejoin_inner_storage.dat", irel->f->f->type);
	Buffer *iStorageBuffer = buffer_create(iStorageFile, WORK_MEM, POLICY_LRU);
	HeapAm *iStorage = ExecInitHeapAm(iStorageBuffer, irel->htupsze);
	Sort *sort2 = ExecInitSort(irel, cmp_timestmp_start, irel->htupsze, WORK_MEM, irel->f->f->type);
	while((tup = ExecSort(sort2)) != NULL)
		ExecHeapAmInsert(iStorage, tup);
	ExecEndSort(sort2);

	time = time_end(thandle);
	mem_usage = cur_size - mem_usage;
	fprintf(stderr, " done.\n");
	fflush(stderr);

	printf("TIME Sorting  %lld \t", time);

	buffer_clear(iStorageBuffer);
	buffer_clear(oStorageBuffer);

	fprintf(stderr, "Joining BLOCK MERGEJOIN ... ");
	fflush(stderr);
	match = 0;
	fhits = 0;
	ios=0;
	buffer_clearIOStat(oStorageBuffer);
	buffer_clearIOStat(iStorageBuffer);
	thandle = time_start();

	/* JOIN */
	size_t nmemtups = 1; //How many outer tupes process contemporaneously
	List *otups;
	ttup *otup, *itup, *itup_marked;
	ListCell *otupcell;

	ExecHeapAmScanInit(oStorage);
	ExecHeapAmScanInit(iStorage);
	itup = ExecHeapAmScan(iStorage);
	if(itup != NULL)
	{
		itup_marked = ttupdup(itup, iStorage->htupsze);
		ExecMarkPos(iStorage);
		do
		{
			otups = NIL;
			while(list_length(otups) <= nmemtups && (otup = ExecHeapAmScan(oStorage)) != NULL)
				otups = lappend(otups, ttupdup(otup, oStorage->htupsze));

			if(list_length(otups) < 1)
				break;
			/* restore position */
			ExecRestorPos(iStorage);
			itup = itup_marked;
			bool marked_in_round = false;
			/* join outer block with inner tuples */
			do
			{
				/* check the marked position */
				if(!marked_in_round)
				{
					otup = (ttup *)lfirst(list_head(otups));
					if(otup->te <= itup->ts + il + ol - 2)
					{
						/* needs to be marked or is already marked */
						if(itup != itup_marked)
						{
							freeTTup(itup_marked);
							itup_marked = ttupdup(itup, iStorage->htupsze);
							ExecMarkPos(iStorage);
						}
						marked_in_round = true;
					}
				}
				/* if the last tuple in the block has otup->te < itup->ts we can stop */
				otup = (ttup *)lfirst(list_tail(otups));
				if(otup->te < itup->ts)
					break;
				foreach(otupcell, otups)
				{
					otup = (ttup *)lfirst(otupcell);
					if (itup->ts < otup->te
							&& itup->te > otup->ts)
						match++;
					else
						fhits++;
				}
			} while((itup = ExecHeapAmScan(iStorage)) != NULL);
			foreach(otupcell, otups)
			freeTTup(lfirst(otupcell));
			list_free(otups);

		} while (1);
		freeTTup(itup_marked);
	}

	time = time_end(thandle);
	fprintf(stderr, "done.\n");
	fflush(stderr);

	ios = buffer_numReads(oStorageBuffer) + buffer_numReads(iStorageBuffer);

	printf("TIME JOIN %lld \t", time);
	printf(" %ld \t", ios);
	printf("MATCHES %ld \t", match);
	printf("FALSE HITS %ld \t", fhits);
	printf(" %ld \t", mem_usage);
	fflush(stdout);

	ExecEndHeapAM(oStorage);
	ExecEndHeapAM(iStorage);
	buffer_close(oStorageBuffer);
	buffer_close(iStorageBuffer);
	file_close(oStorageFile);
	file_close(iStorageFile);

}



static void
run_ANTIJOIN_DISK(HeapAm *orel,	char *ofn,
		char *ifn)
{
	size_t mem_usage;
	void *thandle;
	long long time;
	long match=0, fhits=0, ios;

	mem_usage = cur_size;
	void *outer_rel, *inner_rel;
	int numOuter=1;
	int numInner=1;
	fprintf(stderr, "Entered Antijoin ... ");
	fflush(stderr);

	char buf[0x100];
	char bufRes[0x100];

	snprintf(buf, sizeof(buf), "%s.part%d.txt", ofn, numOuter);
	fprintf(stderr, "%s \n", buf);

	fprintf(stderr, "%s \n", buf);
	fflush(stderr);
	thandle = time_start();

	File *outerFile= file_open(buf);

	Buffer *oStorageBuffer = buffer_create(outerFile, WORK_MEM, POLICY_LRU);
	HeapAm *oStorage = ExecInitHeapAm(oStorageBuffer, sizeof(ttup)+50);

	ExecHeapAmScanInit(oStorage);

	/*tup = ExecHeapAmScan(oStorage);
	while(tup != NULL)
	{
			fprintf(stderr, "%d   [%ld,%ld]  \n",++c, tup->ts, tup->te);
		tup = ExecHeapAmScan(oStorage);
	}*/

	snprintf(buf, sizeof(buf),  "%s.part%d.txt",  ifn, numInner);


	File *innerFile= file_open(buf);
	Buffer *iStorageBuffer;
	HeapAm *iStorage;

	File *outputFile;

	ttup *otup, *itup;
	ttup *T= private_malloc(sizeof(ttup));
	ttup *tup= private_malloc(sizeof(ttup)); ;

	T->data=private_malloc(50);

	while (true){ //outer partition
		while (true){ //Inner partition
			iStorageBuffer = buffer_create(innerFile, WORK_MEM, POLICY_LRU);
			iStorage = ExecInitHeapAm(iStorageBuffer, sizeof(ttup)+50);
		//		fprintf(stderr, "O: %d  I: %d \n", numOuter, numInner);
		//	fflush(stderr);

			if(numInner%2==0)
			{
				snprintf(bufRes, sizeof(bufRes),  "%s.TEMPa.txt",  ofn);
	//			fprintf(stderr, "%s \n", bufRes);
				outputFile= file_create(bufRes, orel->f->f->type);
			}
			else
			{
				snprintf(bufRes, sizeof(bufRes),  "%s.TEMPb.txt",  ofn);
	//			fprintf(stderr, "%s \n", bufRes);

				outputFile= file_create(bufRes, orel->f->f->type);
			}


			Buffer *outputBuffer = buffer_create(outputFile, WORK_MEM, POLICY_LRU);
			HeapAm *outputStorage = ExecInitHeapAm(outputBuffer, orel->htupsze);


			ExecHeapAmScanInit(oStorage);
			ExecHeapAmScanInit(iStorage);


			otup=ExecHeapAmScan(oStorage);
			itup = ExecHeapAmScan(iStorage);

			if(otup!=NULL)
			{
				T->ts=otup->ts;
				T->data=strdup(otup->data);
			}
			if(itup!=NULL)
				T->te=itup->ts;


			while(otup!=NULL)
			{
//				fprintf(stderr, "Outer: [%ld,%ld) %s,  Inner: [%ld,%ld) %s, T:[%ld,%ld) \n", otup->ts, otup->te, otup->data, itup->ts, itup->te, itup->data, T->ts, T->te);
				fflush(stderr);

				if (intervalLen(T)>0 && overlapFRA(otup,T))
				{
					//OUTPUT
					tup->ts = MAX(otup->ts,T->ts);
					tup->te=MIN(otup->te, T->te);
					tup->data=strdup(otup->data);
			//		fprintf(stderr, "OUTPUT: [%ld,%ld) %s \n", tup->ts, tup->te, tup->data);
			//		fflush(stderr);
					match++;
					ExecHeapAmInsert(outputStorage, tup);

				}
				else
					fhits++;

				if(itup==NULL || otup->te<itup->te)
				{
					otup=ExecHeapAmScan(oStorage);
				}
				else
				{
					T->ts=itup->te;
					itup = ExecHeapAmScan(iStorage);
					if (itup != NULL)
					{
						T->te=itup->ts;
					}
					else
						T->te=otup->te;
				}


			}

			ExecEndHeapAM(outputStorage);
			buffer_close(outputBuffer);
			file_close(outputFile);
			ExecEndHeapAM(oStorage);
			buffer_close(oStorageBuffer);
			file_close(outerFile);


			outerFile= file_open(bufRes);
			oStorageBuffer = buffer_create(outerFile, WORK_MEM, POLICY_LRU);
			oStorage = ExecInitHeapAm(oStorageBuffer, sizeof(ttup)+50);

			ExecEndHeapAM(iStorage);
			buffer_close(iStorageBuffer);
			file_close(innerFile);
			numInner++;
			snprintf(buf, sizeof(buf),  "%s.part%d.txt", ifn, numInner);
			if( access( buf, F_OK )== -1 )
				break;
			innerFile= file_open(buf);
		}

		//run_readFile_Fra_DISK(bufRes);
		numInner=1;
		snprintf(buf, 199,  "%s.part%d.txt", ifn, numInner);
		innerFile= file_open(buf);
		iStorageBuffer = buffer_create(innerFile, WORK_MEM, POLICY_LRU);
		iStorage = ExecInitHeapAm(iStorageBuffer, sizeof(ttup)+50);



		numOuter++;
		snprintf(buf, 199, "%s.part%d.txt", ofn, numOuter);
		if( access( buf, F_OK ) == -1 )
			break;
		outerFile= file_open(buf);
		oStorageBuffer = buffer_create(outerFile, WORK_MEM, POLICY_LRU);
		oStorage = ExecInitHeapAm(oStorageBuffer, sizeof(ttup)+50);


	}

			fprintf(stderr, "Macthes: %d   FHits: %d \n", match, fhits);
			fflush(stderr);

}

static void
run_AGGREGATION_DISK(HeapAm *orel,	char *ofn)
{
	size_t mem_usage;
	void *thandle;
	long long time;
	long match, fhits, ios;
	match=0;
	mem_usage = cur_size;
	thandle = time_start();
	void *outer_rel, *inner_rel;
	int numOuter=1;
	fprintf(stderr, "Entered Aggregation ... ");
	fflush(stderr);

	char buf[0x200];
	char bufRes[0x200];
	ttup *itup;
	ttup *otup;

	snprintf(buf, 199, "%s.part%d.txt", ofn, numOuter);
	fprintf(stderr, "%s \n", buf);
	fflush(stderr);

	File *outerFile= file_open(buf);
	Buffer *oStorageBuffer = buffer_create(outerFile, WORK_MEM, POLICY_LRU);
	HeapAm *oStorage = ExecInitHeapAm(oStorageBuffer, sizeof(ttup)+50);

	ExecHeapAmScanInit(oStorage);

	numOuter++;
	snprintf(buf, 199,  "%s.part%d.txt",  ofn, numOuter);

	if( access( buf, F_OK ) == -1 )
	{
		fprintf(stderr, "ONLY ONE PARTITION. ...\n ");
		otup=ExecHeapAmScan(oStorage);
		while(otup != NULL)
		{
			fprintf(stderr, " [%ld,%ld], %s  \n", otup->ts, otup->te, otup->data);
			otup=ExecHeapAmScan(oStorage);
		}
		fflush(stderr);
		exit(1);
	}
	File *innerFile= file_open(buf);
	Buffer *iStorageBuffer;
	HeapAm *iStorage;

	File *outputFile;

	ttup *X= private_malloc(sizeof(ttup));
	ttup *Y= private_malloc(sizeof(ttup));
	ttup *tup= private_malloc(sizeof(ttup));

	otup=ExecHeapAmScan(oStorage);
	tup->data = private_malloc(50);//strdup(otup->data);
	X->data= private_malloc(50);//strdup(otup->data);
	Y->data= private_malloc(50);//strdup(otup->data);


	while (true){ //outer partition
		iStorageBuffer = buffer_create(innerFile, WORK_MEM, POLICY_LRU);
		iStorage = ExecInitHeapAm(iStorageBuffer, sizeof(ttup)+50);
		//	fprintf(stderr, "O: %d  I: %d \n", numOuter, numInner);
		fflush(stderr);

		if(numOuter%2==0)
		{
			snprintf(bufRes, 199,  "%s.AGGa.txt",  ofn);
			outputFile= file_create(bufRes, orel->f->f->type);
		}
		else
		{
			snprintf(bufRes, 199,  "%s.AGGb.txt",  ofn);
			outputFile= file_create(bufRes, orel->f->f->type);
		}
		fprintf(stderr, "OutputFile %s \n", bufRes);

		Buffer *outputBuffer = buffer_create(outputFile, WORK_MEM, POLICY_LRU);
		HeapAm *outputStorage = ExecInitHeapAm(outputBuffer, orel->htupsze);


		ExecHeapAmScanInit(oStorage);
		ExecHeapAmScanInit(iStorage);
		otup=ExecHeapAmScan(oStorage);
		itup = ExecHeapAmScan(iStorage);
		X->ts=otup->ts;
		X->te=itup->ts;
		strcpy(X->data,otup->data);
		Y->ts=itup->ts;
		Y->te=otup->ts;
		strcpy(Y->data,itup->data);


		match=0;

		while(otup!=NULL || itup!=NULL)
		{



			/*     	if(otup!=NULL)
            		fprintf(stderr, "Outer: [%ld,%ld) %s,", otup->ts, otup->te, otup->data);
            	else
            		fprintf(stderr, "Outer: NULL, " );
            	if(itup!=NULL)
            		fprintf(stderr, "Inner: [%ld,%ld) %s \n", itup->ts, itup->te, itup->data);
            	else
            		fprintf(stderr, "Inner: NULL  \n" );

            	fprintf(stderr, "X: [%ld,%ld),  Y: [%ld,%ld) \n", X->ts, X->te, Y->ts, Y->te);
            	fflush(stderr);
			 */



			if (otup!=NULL && intervalLen(X)>0 && overlapFRA(otup,X))
			{
				//OUTPUT
				tup->ts = MAX(otup->ts,X->ts);
				tup->te=MIN(otup->te, X->te);
				strcpy(tup->data,otup->data);
				strcat(tup->data,  "+NULL\0");
				//	fprintf(stderr, "OUTPUT1: [%ld,%ld) %s \n", tup->ts, tup->te, tup->data);
				//	fflush(stderr);
				ExecHeapAmInsert(outputStorage, tup);
				match++;
			}


			if (itup!=NULL && intervalLen(Y)>0 && overlapFRA(itup,Y))
			{
				//OUTPUT
				tup->ts = MAX(itup->ts,Y->ts);
				tup->te=MIN(itup->te, Y->te);
				strcpy(tup->data,"NULL+");
				tup->data=strcat(tup->data, itup->data);
				//	fprintf(stderr, "OUTPUT2: [%ld,%ld) %s \n", tup->ts, tup->te, tup->data);
				//	fflush(stderr);
				ExecHeapAmInsert(outputStorage, tup);
				match++;
			}
			if (itup!=NULL && otup!=NULL && overlapFRA(otup, itup))
			{
				//OUTPUT
				tup->ts = MAX(itup->ts,otup->ts);
				tup->te=MIN(itup->te, otup->te);
				strcpy(tup->data,otup->data);
				strcat(tup->data,  " + ");
				tup->data=strcat(tup->data, itup->data);
				//	fprintf(stderr, "OUTPUT3: [%ld,%ld) %s \n", tup->ts, tup->te, tup->data);
				//	fflush(stderr);
				ExecHeapAmInsert(outputStorage, tup);
				match++;
			}



			if((itup==NULL && otup!=NULL) || (otup!=NULL && otup->te<itup->te))
			{
				Y->ts=otup->te;
				otup=ExecHeapAmScan(oStorage);
				if (otup != NULL)
				{
					Y->te=otup->ts;
				}else
				{
					Y->te=INT32_MAX;
				}
			}
			else
			{
				X->ts=itup->te;
				itup = ExecHeapAmScan(iStorage);
				if (itup != NULL)
				{
					X->te=itup->ts;
				}
				else
					X->te=INT32_MAX;
			}
		}

		ExecEndHeapAM(outputStorage);
		buffer_close(outputBuffer);
		file_close(outputFile);

		ExecEndHeapAM(oStorage);
		buffer_close(oStorageBuffer);
		file_close(outerFile);

		outerFile= file_open(bufRes);
		oStorageBuffer = buffer_create(outerFile, WORK_MEM, POLICY_LRU);
		oStorage = ExecInitHeapAm(oStorageBuffer, sizeof(ttup)+50);

		ExecEndHeapAM(iStorage);
		buffer_close(iStorageBuffer);
		file_close(innerFile);

		numOuter++;
		snprintf(buf, 199,  "%s.part%d.txt", ofn, numOuter);
		if( access( buf, F_OK )!=0 )
			break;
		innerFile= file_open(buf);


	}

	time = time_end(thandle);
	fprintf(stderr,"TIME AGG %lld \t", time);
	fprintf(stderr," %ld \t", ios);
	fprintf(stderr,"MATCHES: %ld \t", match);
	fprintf(stderr, "%ld \t", fhits);
	fprintf(stderr, "%ld \t", mem_usage);
	fflush(stdout);

	ExecHeapAmScanInit(oStorage);

	//PRINT OUTPUT TUPLES
/*	while((otup=ExecHeapAmScan(oStorage))!=NULL)
	{
		fprintf(stderr, "OUTPUT: [%ld,%ld) %s \n", otup->ts, otup->te, otup->data);
	} */
	buffer_close(oStorageBuffer);
	ExecEndHeapAM(oStorage);
	file_close(outerFile);

}


static void
run_NORMALIZATION_DISK(HeapAm *orel,	char *ofn)
{
	size_t mem_usage;
	void *thandle;
	long long time;
	long match, fhits, ios;
	match=0;
	mem_usage = cur_size;
	thandle = time_start();
	void *outer_rel, *inner_rel;
	int numOuter=1;
	int numInner=2;
	fprintf(stderr, "Entered Normalization ... ");
	fflush(stderr);

	char buf[0x200];
	char bufRes[0x200];
	char bufTmp[0x200];
	ttup *itup;
	ttup *otup;

	snprintf(buf, 199, "%s.part%d.txt", ofn, numOuter);
	fprintf(stderr, "%s \n", buf);
	fflush(stderr);

	File *outerFile= file_open(buf);
	Buffer *outerStorageBuffer = buffer_create(outerFile, WORK_MEM, POLICY_LRU);
	HeapAm *outerStorage = ExecInitHeapAm(outerStorageBuffer, sizeof(ttup)+50);

	ExecHeapAmScanInit(outerStorage);

	snprintf(buf, 199,  "%s.part%d.txt",  ofn, numInner);

	if( access( buf, F_OK ) == -1 )
	{
		fprintf(stderr, "ONLY ONE PARTITION. ...\n ");
		otup=ExecHeapAmScan(outerStorage);
		while(otup != NULL)
		{
			fprintf(stderr, " [%ld,%ld], %s  \n", otup->ts, otup->te, otup->data);
			otup=ExecHeapAmScan(outerStorage);
		}
		fflush(stderr);
		exit(1);
	}

	File *outputFile;

	ttup *X= private_malloc(sizeof(ttup));
	ttup *Y= private_malloc(sizeof(ttup));
	ttup *tup= private_malloc(sizeof(ttup));

	otup=ExecHeapAmScan(outerStorage);
	tup->data = private_malloc(50);//strdup(otup->data);
	X->data= private_malloc(50);//strdup(otup->data);
	Y->data= private_malloc(50);//strdup(otup->data);


	while(true){
		numInner=1;
		snprintf(buf, 199,  "%s.part%d.txt", ofn, numInner);
		File *innerFile= file_open(buf);
		Buffer *iStorageBuffer;
		HeapAm *iStorage;

		while (true){
			snprintf(bufTmp, 199,  "%s.part%dTmp.txt",  ofn, numOuter);
			snprintf(bufRes, 199,  "%s.part%dNormalized.txt",  ofn, numOuter);
			rename (bufRes, bufTmp);

			iStorageBuffer = buffer_create(innerFile, WORK_MEM, POLICY_LRU);
			iStorage = ExecInitHeapAm(iStorageBuffer, sizeof(ttup)+50);

			outputFile= file_create(bufRes, orel->f->f->type);

			Buffer *outputBuffer = buffer_create(outputFile, WORK_MEM, POLICY_LRU);
			HeapAm *outputStorage = ExecInitHeapAm(outputBuffer, orel->htupsze);


			ExecHeapAmScanInit(outerStorage);
			ExecHeapAmScanInit(iStorage);
			otup=ExecHeapAmScan(outerStorage);
			itup = ExecHeapAmScan(iStorage);
			X->ts=otup->ts;
			X->te=itup->ts;
			strcpy(X->data,otup->data);
			Y->ts=itup->ts;
			Y->te=otup->ts;
			strcpy(Y->data,itup->data);


			match=0;

			while(otup!=NULL || itup!=NULL)
			{


				if(otup!=NULL)
					fprintf(stderr, "Outer: [%ld,%ld) %s,", otup->ts, otup->te, otup->data);
					else
						fprintf(stderr, "Outer: NULL, " );
				if(itup!=NULL)
					fprintf(stderr, "Inner: [%ld,%ld) %s \n", itup->ts, itup->te, itup->data);
				else
					fprintf(stderr, "Inner: NULL  \n" );

				fprintf(stderr, "X: [%ld,%ld),  Y: [%ld,%ld) \n", X->ts, X->te, Y->ts, Y->te);
				fflush(stderr);


				if (otup!=NULL && intervalLen(otup)>0 && intervalLen(X)>0 && overlapFRA(otup,X) && (itup==NULL || strcmp(otup->data,itup->data)==0))
				{
					fprintf(stderr, "00");
					fflush(stderr);

					//OUTPUT
					tup->ts = MAX(otup->ts,X->ts);
					tup->te=MIN(otup->te, X->te);
					strcpy(tup->data,otup->data);
					fprintf(stderr, "OUTPUT1: [%ld,%ld) %s \n", tup->ts, tup->te, tup->data);
					fflush(stderr);
					ExecHeapAmInsert(outputStorage, tup);
					match++;
					otup->ts=X->te;
				}


				if (itup!=NULL && otup!=NULL && intervalLen(otup)>0  && overlapFRA(otup, itup) && strcmp(otup->data,itup->data)==0)
				{
					fprintf(stderr, "11");
					fflush(stderr);

					//OUTPUT
					tup->ts = MAX(itup->ts,otup->ts);
					tup->te=MIN(itup->te, otup->te);
					strcpy(tup->data,otup->data);
					fprintf(stderr, "OUTPUT2: [%ld,%ld) %s \n", tup->ts, tup->te, tup->data);
					fflush(stderr);
					ExecHeapAmInsert(outputStorage, tup);
					match++;
					otup->ts=tup->te;
				}



				if((itup==NULL && otup!=NULL) || (otup!=NULL && otup->te<itup->te))
				{
					fprintf(stderr, "22");
					fflush(stderr);

					Y->ts=otup->te;
					if(intervalLen(otup)>0)
					{
						fprintf(stderr, "OUTPUT3: [%ld,%ld) %s \n", otup->ts, otup->te, otup->data);
						fflush(stderr);
						ExecHeapAmInsert(outputStorage, otup);
						match++;
					}

					otup=ExecHeapAmScan(outerStorage);
					if (otup != NULL)
					{
						Y->te=otup->ts;
					}else
					{
						Y->te=INT32_MAX;
					}
					fprintf(stderr, "22a");
					fflush(stderr);

				}
				else
				{
					fprintf(stderr, "33");
					fflush(stderr);

					if(otup!=NULL && strcmp(otup->data,itup->data)==0)
						X->ts=itup->te;
					itup = ExecHeapAmScan(iStorage);
					if (itup != NULL)
					{
						X->te=itup->ts;
					}
					else
						X->te=INT32_MAX;
				}


			}

			ExecEndHeapAM(outputStorage);
			buffer_close(outputBuffer);
			file_close(outputFile);

			ExecEndHeapAM(outerStorage);
			buffer_close(outerStorageBuffer);
			file_close(outerFile);


			ExecEndHeapAM(iStorage);
			buffer_close(iStorageBuffer);
			file_close(innerFile);

			numInner++;
			snprintf(buf, 199,  "%s.part%d.txt", ofn, numInner);
			if( access( buf, F_OK )!=0 )
				break;
			innerFile= file_open(buf);

			outerFile= file_open(bufRes);
			outerStorageBuffer = buffer_create(outerFile, WORK_MEM, POLICY_LRU);
			outerStorage = ExecInitHeapAm(outerStorageBuffer, sizeof(ttup)+50);

		}

		numOuter++;
		snprintf(buf, 199, "%s.part%d.txt", ofn, numOuter);
		fprintf(stderr, "FINE1");
		fflush(stderr);

		if( access( buf, F_OK )!=0 )
			break;
		fprintf(stderr, "FINE2");
		fflush(stderr);

outerFile= file_open(buf);
		outerStorageBuffer = buffer_create(outerFile, WORK_MEM, POLICY_LRU);
		outerStorage = ExecInitHeapAm(outerStorageBuffer, sizeof(ttup)+50);
	}
	fprintf(stderr, "FINE3");
	fflush(stderr);

	time = time_end(thandle);
	/*fprintf(stderr," %lld \t", time);
	fprintf(stderr," %ld \t", ios);
	fprintf(stderr,"MATCHES: %ld \t", match);
	fprintf(stderr, "%ld \t", fhits);
	fprintf(stderr, "%ld \t", mem_usage);
	fflush(stdout);*/


}


static void
run_INTERSECTION_DISK(HeapAm *orel,	char *ofn,
		char *ifn)
{
	size_t mem_usage;
	void *thandle;
	long long time;
	long match=0;
	long fhits, ios;

	mem_usage = cur_size;
	thandle = time_start();
	void *outer_rel, *inner_rel;
	int numOuter=1;
	int numInner=1;
	fprintf(stderr, "Entered Intersection ... ");
	fflush(stderr);

	char buf[0x200];
	char bufOld[0x200];
	char cmd[0x400];
	snprintf(buf, 199, "%s.part%dNormalized.txt", ofn, numOuter);

	while(access( buf, F_OK ) != -1)
	{
		fprintf(stderr, "COPY %s \n", buf);
		fflush(stderr);

		snprintf(cmd, 399, "cp %s %sIntermediate", buf, buf);
		int result = system(cmd); //
		snprintf(buf, 199, "%s.part%dNormalized.txt", ofn, ++numOuter);
	}

	snprintf(buf, 199, "%s.part%dNormalized.txt", ifn, numInner);
	while(access( buf, F_OK ) != -1)
	{
		fprintf(stderr, "COPY %s \n", buf);
		fflush(stderr);

		snprintf(cmd, 399, "cp %s %sIntermediate", buf, buf);
		int result = system(cmd); //
		snprintf(buf, 199, "%s.part%dNormalized.txt", ifn, ++numInner);
	}

	numOuter=1;
	numInner=1;
	int a=1;
	int b=1;

	snprintf(buf, 199, "%s.part%dNormalized.txtIntermediate", ofn, numOuter);

	File *outerFile= file_open(buf);
	Buffer *oStorageBuffer = buffer_create(outerFile, WORK_MEM, POLICY_LRU);
	HeapAm *oStorage = ExecInitHeapAm(oStorageBuffer, sizeof(ttup)+50);
	ExecHeapAmScanInit(oStorage);


	snprintf(buf, 199,  "%s.part%dNormalized.txtIntermediate",  ifn, numInner);

	File *innerFile= file_open(buf);
	Buffer *iStorageBuffer;
	HeapAm *iStorage;


	ttup *otup, *itup;
	ttup *outputItup= private_malloc(sizeof(ttup));
	ttup *X= private_malloc(sizeof(ttup));
	ttup *Y= private_malloc(sizeof(ttup));
	ttup *tup= private_malloc(sizeof(ttup));
	tup->data = private_malloc(50);//strdup(otup->data);
		X->data= private_malloc(50);//strdup(otup->data);
		Y->data= private_malloc(50);//strdup(otup->data);

	while (true){ //outer partition
		while (true){ //Inner partition
			iStorageBuffer = buffer_create(innerFile, WORK_MEM, POLICY_LRU);
			iStorage = ExecInitHeapAm(iStorageBuffer, sizeof(ttup)+50);


			snprintf(buf, 199, "%s.part%dNormalized.txtFinal", ofn, numOuter);
			File *outputOuterFile=file_create(buf, orel->f->f->type);

			Buffer *outputOuterBuffer = buffer_create(outputOuterFile, WORK_MEM, POLICY_LRU);
			HeapAm *outputOuterStorage = ExecInitHeapAm(outputOuterBuffer, sizeof(ttup)+50);

			snprintf(buf, 199, "%s.part%dNormalized.txtFinal", ifn, numInner);
			File *outputInnerFile= file_create(buf, orel->f->f->type);
			Buffer *outputInnerBuffer = buffer_create(outputInnerFile, WORK_MEM, POLICY_LRU);
			HeapAm *outputInnerStorage = ExecInitHeapAm(outputInnerBuffer, sizeof(ttup)+50);

			fprintf(stderr, "Partitions %d and %d \n", numOuter,numInner);
			fflush(stderr);


			ExecHeapAmScanInit(oStorage);
			ExecHeapAmScanInit(iStorage);
			otup=ExecHeapAmScan(oStorage);
			itup = ExecHeapAmScan(iStorage);


			while(otup!=NULL && itup!=NULL )
			{

				if(otup!=NULL)
					fprintf(stderr, "Outer: [%ld,%ld) %s,", otup->ts, otup->te, otup->data);
					else
						fprintf(stderr, "Outer: NULL, " );
				if(itup!=NULL)
					fprintf(stderr, "Inner: [%ld,%ld) %s \n", itup->ts, itup->te, itup->data);
				else
					fprintf(stderr, "Inner: NULL  \n" );

				fprintf(stderr, "X: [%ld,%ld),  Y: [%ld,%ld) \n", X->ts, X->te, Y->ts, Y->te);
				fflush(stderr);


				if (otup!=NULL && intervalLen(otup)>0 && intervalLen(X)>0 && overlapFRA(otup,X) && (itup==NULL || strcmp(otup->data,itup->data)==0))
				{
					fprintf(stderr, "00");
					fflush(stderr);

					//OUTPUT
					tup->ts = MAX(otup->ts,X->ts);
					tup->te=MIN(otup->te, X->te);
					strcpy(tup->data,otup->data);
					fprintf(stderr, "WRITE O: [%ld,%ld) %s \n", tup->ts, tup->te, tup->data);
					fflush(stderr);
					ExecHeapAmInsert(outputOuterStorage, tup);
					match++;
					otup->ts=X->te;
				}


				if (itup!=NULL && otup!=NULL && intervalLen(otup)>0  && overlapFRA(otup, itup) && strcmp(otup->data,itup->data)==0)
				{
					fprintf(stderr, "11");
					fflush(stderr);

					//OUTPUT
					tup->ts = MAX(itup->ts,otup->ts);
					tup->te=MIN(itup->te, otup->te);
					strcpy(tup->data,otup->data);
					fprintf(stderr, "INTERSECTION: [%ld,%ld) %s \n", tup->ts, tup->te, tup->data);
					fflush(stderr);
					otup->ts=tup->te;
				}


				if (itup!=NULL && intervalLen(itup)>0 && intervalLen(Y)>0 && overlapFRA(itup,Y) && (otup==NULL || strcmp(otup->data,itup->data)==0))
				{
					//OUTPUT

					tup->ts = MAX(otup->ts,X->ts);
					tup->te=MIN(otup->te, X->te);
					strcpy(tup->data,otup->data);
					fprintf(stderr, "WRITE I: [%ld,%ld) %s \n", tup->ts, tup->te, tup->data);
					fflush(stderr);
					ExecHeapAmInsert(outputInnerStorage, tup);
					match++;
					itup->ts=Y->te;
				}



				if((itup==NULL && otup!=NULL) || (otup!=NULL && otup->te<itup->te))
				{
					fprintf(stderr, "22");
					fflush(stderr);

					if(itup!=NULL && strcmp(otup->data,itup->data)==0)
						Y->ts=otup->te;

					if(intervalLen(otup)>0)
					{
						fprintf(stderr, "WRITE OO: [%ld,%ld) %s \n", otup->ts, otup->te, otup->data);
						fflush(stderr);
						ExecHeapAmInsert(outputOuterStorage, otup);
						match++;
					}

					otup=ExecHeapAmScan(oStorage);
					if (otup != NULL)
					{
						Y->te=otup->ts;
					}else
					{
						Y->te=INT32_MAX;
					}

				}
				else
				{
					fprintf(stderr, "33");
					fflush(stderr);

					if(otup!=NULL && strcmp(otup->data,itup->data)==0)
						X->ts=itup->te;

					if(intervalLen(itup)>0)
					{
						fprintf(stderr, "WRITE II: [%ld,%ld) %s \n", itup->ts, itup->te, itup->data);
						fflush(stderr);
						ExecHeapAmInsert(outputInnerStorage, itup);
						match++;
					}

					itup = ExecHeapAmScan(iStorage);
					if (itup != NULL)
					{
						X->te=itup->ts;
					}
					else
						X->te=INT32_MAX;
				}


			}


			//CLOSE OLD S
			ExecEndHeapAM(iStorage);
			buffer_close(iStorageBuffer);
			file_close(innerFile);
			snprintf(buf, 199,  "%s.part%dNormalized.txtIntermediate",  ifn, numInner);
			unlink(buf);
			//CLOSE NEW S
			ExecEndHeapAM(outputInnerStorage);
			buffer_close(outputInnerBuffer);
			file_close(outputInnerFile);

			//RENAME NEW S AS OLD
			snprintf(bufOld, 199, "%s.part%dNormalized.txtFinal",  ifn, numInner);
			snprintf(buf, 199, "%s.part%dNormalized.txtIntermediate",  ifn, numInner);
			rename (bufOld, buf);

			//CLOSE OLD R
			ExecEndHeapAM(oStorage);
			buffer_close(oStorageBuffer);
			file_close(outerFile);
			snprintf(buf, 199,  "%s.part%dNormalized.txtIntermediate",  ofn, numOuter);
			unlink(buf);
			//CLOSE NEW S
			ExecEndHeapAM(outputOuterStorage);
			buffer_close(outputOuterBuffer);
			file_close(outputOuterFile);

			//RENAME NEW AS OLD
			snprintf(bufOld, 199, "%s.part%dNormalized.txtFinal",  ofn, numOuter);
			snprintf(buf, 199, "%s.part%dNormalized.txtIntermediate",  ofn, numOuter);
			rename (bufOld, buf);

			//NEW R
			outerFile= file_open(buf);
			oStorageBuffer = buffer_create(outerFile, WORK_MEM, POLICY_LRU);
			oStorage = ExecInitHeapAm(oStorageBuffer, sizeof(ttup)+50);



			//NEW S
			numInner++;
			snprintf(buf, 199, "%s.part%dNormalized.txtIntermediate",  ifn, numInner);
			if( access( buf, F_OK )== -1 )
				break;
			innerFile= file_open(buf);


		}

		numInner=1;
		snprintf(buf, 199,  "%s.part%dNormalized.txtIntermediate", ifn, numInner);
		fprintf(stderr, "%s \n", buf);
		fflush(stderr);
		innerFile= file_open(buf);
		iStorageBuffer = buffer_create(innerFile, WORK_MEM, POLICY_LRU);
		iStorage = ExecInitHeapAm(iStorageBuffer, sizeof(ttup)+50);
		fprintf(stderr, "OKK \n", buf);
		fflush(stderr);



		ExecEndHeapAM(oStorage);
		buffer_close(oStorageBuffer);
		file_close(outerFile);
		numOuter++;
		snprintf(buf, 199, "%s.part%dNormalized.txt", ofn, numOuter);
		if( access( buf, F_OK ) == -1 )
			break;
		outerFile= file_open(buf);
		oStorageBuffer = buffer_create(outerFile, WORK_MEM, POLICY_LRU);
		oStorage = ExecInitHeapAm(oStorageBuffer, sizeof(ttup)+50);


	}
}



static void
run_EQUIJOIN_MM(HeapAm *orel,	char *ofn,
		char *ifn)
{
	size_t mem_usage;
	void *thandle;
	long long time;
	long match=0;
	long fhits=0;
	long comparisons=0;

	mem_usage = cur_size;

	int numOuter=1;
	int numInner=1;
	fprintf(stderr, "Entered EquiJoinMM ... ");
	fflush(stderr);
	File *innerFile[10000] ;
	Buffer *iStorageBuffer[10000] ;
	HeapAm *iStorage[10000];
	File *outerFile[10000];
	Buffer *oStorageBuffer[10000];
	HeapAm *oStorage[10000];
	List *outer_partition_tups[10000];
	List *inner_partition_tups[10000];
	ttup *otup, *itup;


	char buf[0x100];
	char bufRes[0x100];
	snprintf(bufRes, sizeof(bufRes),  "%sEQUIJOIN.txt",  ofn);
	fprintf(stderr, "%s \n", bufRes);
	fflush(stderr);

	File *outputFile= file_create(bufRes, orel->f->f->type);
	Buffer *outputBuffer = buffer_create(outputFile, WORK_MEM, POLICY_LRU);
	HeapAm *outputStorage = ExecInitHeapAm(outputBuffer, orel->htupsze);

	ttup *tup= private_malloc(sizeof(ttup));

	tup->ts=-1;
	tup->te=-1;
	tup->data=private_malloc(50);

	snprintf(buf, sizeof(buf),  "%s.part%d.txt",  ifn, numInner);
	while(access( buf, F_OK ) != -1)
	{
		innerFile[numInner]= file_open(buf);
		iStorageBuffer[numInner]= buffer_create(innerFile[numInner], WORK_MEM, POLICY_LRU);
		iStorage[numInner] = ExecInitHeapAm(iStorageBuffer[numInner], sizeof(ttup)+50);
		ExecHeapAmScanInit(iStorage[numInner]);
		itup=ExecHeapAmScan(iStorage[numInner]);
		inner_partition_tups[numInner] = NIL;
        do {
        	inner_partition_tups[numInner] = lappend(inner_partition_tups[numInner], ttupdup(itup, orel->htupsze));
          itup = ExecHeapAmScan(iStorage[numInner]);
        } while(itup != NULL);
   // 	inner_partition_tups[numInner] = lappend(inner_partition_tups[numInner], ttupdup(tup, orel->htupsze));
		numInner++;
		snprintf(buf, sizeof(buf),  "%s.part%d.txt", ifn, numInner);
	}



	snprintf(buf, sizeof(buf), "%s.part%d.txt", ofn, numOuter);
	while(access( buf, F_OK ) != -1 )
	{
		outerFile[numOuter]= file_open(buf);
		oStorageBuffer[numOuter] = buffer_create(outerFile[numOuter], WORK_MEM, POLICY_LRU);
		oStorage[numOuter] = ExecInitHeapAm(oStorageBuffer[numOuter], sizeof(ttup)+50);

		ExecHeapAmScanInit(oStorage[numOuter]);
		otup=ExecHeapAmScan(oStorage[numOuter]);
		outer_partition_tups[numOuter] = NIL;
        do {
          outer_partition_tups[numOuter] = lappend(outer_partition_tups[numOuter], ttupdup(otup, orel->htupsze));
          otup = ExecHeapAmScan(oStorage[numOuter]);
        } while(otup != NULL);
 //       outer_partition_tups[numOuter] = lappend(outer_partition_tups[numOuter], ttupdup(tup, orel->htupsze));
		numOuter++;
		snprintf(buf, sizeof(buf),  "%s.part%d.txt", ofn, numOuter);
	}

	thandle = time_start();
	numOuter--;
	numInner--;
	match=0;
	fhits=0;

	int j=1;
	int i=1;



	  ListCell *outer_tup_cell, *inner_tup_cell;


	while (true){ //outer partition

		while (true){ //Inner partition

			//fprintf(stderr, "O: %d I: %d  \n", i,j);
			//fflush(stderr);
            outer_tup_cell = list_head(outer_partition_tups[i]) ;
        	otup= ((ttup *)lfirst(outer_tup_cell)) ;


            inner_tup_cell = list_head(inner_partition_tups[j]) ;
        	itup= ((ttup *)lfirst(inner_tup_cell)) ;


        	while(true )
        	{
        	  if(otup->te<=itup->te)
        	  {
        	    if(itup->ts < otup->te)
        	      match++;
        	    else
        	      fhits++;

        	    // advance outer
        	    outer_tup_cell = lnext(outer_tup_cell);
        	    if(outer_tup_cell)
        	    	otup       = ((ttup *)lfirst(outer_tup_cell)) ;

        	    else
        	    	break;
        	  }
        	  else
        	  {
        	    if(otup->ts < itup->te)
        	      match++;
        	    else
        	      fhits++;

        	    // advance inner
        	    inner_tup_cell = lnext(inner_tup_cell);
        	    if(inner_tup_cell)
        	      itup       = ((ttup *)lfirst(inner_tup_cell)) ;
        	    else
        	    	break;
        	  }
        	}

			j++;
			if( j>numInner)
				break;
		}
		j=1;

		i++;
		if( i> numOuter )
			break;
	}

	time = time_end(thandle);
	fprintf(stderr,"TIME %lld \t", time);
	fprintf(stderr,"MATCHES: %ld \t", match);
	fprintf(stderr, "FALSE HITS %ld \t", fhits);
	fprintf(stderr, "COMPARISONS %ld \t", match+fhits);
	fflush(stdout);

	ExecEndHeapAM(outputStorage);
	buffer_close(outputBuffer);
	file_close(outputFile);

	File *sortedOutputFile= file_open(bufRes);
	Buffer *sortedOutputBuffer = buffer_create(sortedOutputFile, WORK_MEM, POLICY_LRU);
	HeapAm *sortedOutputStorage = ExecInitHeapAm(sortedOutputBuffer, sizeof(ttup)+50);
	Sort *sort1 = ExecInitSort(sortedOutputStorage, cmp_timestmp_start, orel->htupsze, WORK_MEM, orel->f->f->type);

	snprintf(bufRes, sizeof(bufRes),  "%sEQUIJOINsorted.txt",  ofn);
	outputFile= file_create(bufRes, orel->f->f->type);
	outputBuffer = buffer_create(outputFile, WORK_MEM, POLICY_LRU);
	outputStorage = ExecInitHeapAm(outputBuffer, orel->htupsze);


	match=0;
	while((tup = ExecSort(sort1)) != NULL){
		ExecHeapAmInsert(outputStorage, tup);
		fprintf(stderr, "%d: [%ld,%ld)%s \n", ++match, tup->ts, tup->te, tup->data);
	}
	ExecEndSort(sort1);

	ExecEndHeapAM(outputStorage);
	buffer_close(outputBuffer);
	file_close(outputFile);

	ExecEndHeapAM(sortedOutputStorage);
	buffer_close(sortedOutputBuffer);
	file_close(sortedOutputFile);
}




static void
run_EQUIJOIN_DISK(HeapAm *orel,	char *ofn,
		char *ifn)
{
	size_t mem_usage;
	void *thandle;
	long long time;
	long match, fhits, ios=0;

	mem_usage = cur_size;

	int numOuter=1;
	int numInner=1;
	fprintf(stderr, "Entered EquiJoin ... ");
	fflush(stderr);
	File *innerFile[10000] ;
	Buffer *iStorageBuffer[10000] ;
	HeapAm *iStorage[10000];
	File *outerFile[10000];
	Buffer *oStorageBuffer[10000];
	HeapAm *oStorage[10000];

	fprintf(stderr, "1 ... ");
	fflush(stderr);

	char buf[0x100];
	char bufRes[0x100];
	snprintf(bufRes, sizeof(bufRes),  "%sEQUIJOIN.txt",  ofn);
	File *outputFile= file_create(bufRes, orel->f->f->type);
	Buffer *outputBuffer = buffer_create(outputFile, WORK_MEM, POLICY_LRU);
	HeapAm *outputStorage = ExecInitHeapAm(outputBuffer, orel->htupsze);
	fprintf(stderr, "2 ... ");
	fflush(stderr);



	snprintf(buf, sizeof(buf),  "%s.part%d.txt",  ifn, numInner);
	while(access( buf, F_OK ) != -1)
	{
	innerFile[numInner]= file_open(buf);
	iStorageBuffer[numInner]= buffer_create(innerFile[numInner], WORK_MEM, POLICY_LRU);
	iStorage[numInner] = ExecInitHeapAm(iStorageBuffer[numInner], sizeof(ttup)+50);
	numInner++;
	snprintf(buf, sizeof(buf),  "%s.part%d.txt", ifn, numInner);
	}

	fprintf(stderr, "3 ... ");
	fflush(stderr);

	snprintf(buf, sizeof(buf), "%s.part%d.txt", ofn, numOuter);
	while(access( buf, F_OK ) != -1 )
	{
		outerFile[numOuter]= file_open(buf);
		oStorageBuffer[numOuter] = buffer_create(outerFile[numOuter], WORK_MEM, POLICY_LRU);
		oStorage[numOuter] = ExecInitHeapAm(oStorageBuffer[numOuter], sizeof(ttup)+50);
		numOuter++;
		snprintf(buf, sizeof(buf),  "%s.part%d.txt", ofn, numOuter);
	}

	fprintf(stderr, "4 ... ");
	fflush(stderr);


	thandle = time_start();
	numOuter--;
	numInner--;
	match=0;
	fhits=0;
	ttup *otup, *itup;

	int j=1;
	int i=1;

	ExecHeapAmScanInit(oStorage[i]);

	ttup *tup= private_malloc(sizeof(ttup));
	otup=ExecHeapAmScan(oStorage[i]);
	tup->data = strdup(otup->data);


	while (true){ //outer partition
		while (true){ //Inner partition



			ExecHeapAmScanInit(oStorage[i]);
			ExecHeapAmScanInit(iStorage[j]);
			otup=ExecHeapAmScan(oStorage[i]);
			itup = ExecHeapAmScan(iStorage[j]);

			while(otup!=NULL && itup!=NULL )
			{
				//fprintf(stderr, "O: [%ld,%ld) %s,  I: [%ld,%ld)%s \n", otup->ts, otup->te, otup->data, itup->ts, itup->te, itup->data);
				if (overlapFRA(otup,itup))
				{
					//OUTPUT
					//tup->ts = MAX(itup->ts,otup->ts);
					//tup->te=MIN(itup->te, otup->te);
					//strcpy(tup->data,otup->data);
					//strcat(tup->data,  " + ");
					//tup->data=strcat(tup->data, itup->data);
					//	fprintf(stderr, " O: [%ld,%ld) %s,  I: [%ld,%ld)%s \n", tup->ts, tup->te, tup->data, otup->ts, otup->te, otup->data, itup->ts, itup->te, itup->data);
					//	fprintf(stderr, "OUTPUT: TUP:[%ld,%ld) %s      O: [%ld,%ld) %s,  I: [%ld,%ld)%s \n", tup->ts, tup->te, tup->data, otup->ts, otup->te, otup->data, itup->ts, itup->te, itup->data);

					//	ExecHeapAmInsert(outputStorage, tup);
					match++;
				}
				else
				{
					fhits++;
				}
				if(otup->te<=itup->te)
				{
					otup=ExecHeapAmScan(oStorage[i]);
				}
				else{
					itup = ExecHeapAmScan(iStorage[j]);
				}

			}

			j++;
			if( j>numInner)
				break;
		}
		j=1;

		i++;
		if( i> numOuter )
			break;
	}

	time = time_end(thandle);
	fprintf(stderr,"TIME %lld \t", time);
	fprintf(stderr," %ld \t", ios);
	fprintf(stderr,"MATCHES: %ld \t", match);
	fprintf(stderr, "FALSE HITS %ld \t", fhits);
	fprintf(stderr, "%ld \t", mem_usage);
	fflush(stdout);

	ExecEndHeapAM(outputStorage);
	buffer_close(outputBuffer);
	file_close(outputFile);

	File *sortedOutputFile= file_open(bufRes);
	Buffer *sortedOutputBuffer = buffer_create(sortedOutputFile, WORK_MEM, POLICY_LRU);
	HeapAm *sortedOutputStorage = ExecInitHeapAm(sortedOutputBuffer, sizeof(ttup)+50);
	Sort *sort1 = ExecInitSort(sortedOutputStorage, cmp_timestmp_start, orel->htupsze, WORK_MEM, orel->f->f->type);

	snprintf(bufRes, sizeof(bufRes),  "%sEQUIJOINsorted.txt",  ofn);
	outputFile= file_create(bufRes, orel->f->f->type);
	outputBuffer = buffer_create(outputFile, WORK_MEM, POLICY_LRU);
	outputStorage = ExecInitHeapAm(outputBuffer, orel->htupsze);


	match=0;
	while((tup = ExecSort(sort1)) != NULL){
		ExecHeapAmInsert(outputStorage, tup);
		fprintf(stderr, "%d: [%ld,%ld)%s \n", ++match, tup->ts, tup->te, tup->data);
	}
	ExecEndSort(sort1);

	ExecEndHeapAM(outputStorage);
	buffer_close(outputBuffer);
	file_close(outputFile);

	ExecEndHeapAM(sortedOutputStorage);
	buffer_close(sortedOutputBuffer);
	file_close(sortedOutputFile);
}




/*
static void
run_EQUIJOIN_DISK(HeapAm *orel,	char *ofn,
		char *ifn)
{
	size_t mem_usage;
	void *thandle;
	long long time;
	long match, fhits, ios;

	mem_usage = cur_size;
	void *outer_rel, *inner_rel;
	int numOuter=1;
	int numInner=1;
	fprintf(stderr, "Entered EquiJoin ... ");
	fflush(stderr);


	char buf[0x100];
	char bufRes[0x100];
	snprintf(bufRes, sizeof(bufRes),  "%sEQUIJOIN.txt",  ofn);
	File *outputFile= file_create(bufRes, orel->f->f->type);
	Buffer *outputBuffer = buffer_create(outputFile, WORK_MEM, POLICY_LRU);
	HeapAm *outputStorage = ExecInitHeapAm(outputBuffer, orel->htupsze);

	thandle = time_start();
	snprintf(buf, sizeof(buf), "%s.part%d.txt", ofn, numOuter);
	//fprintf(stderr, "%s \n", buf);
	//fflush(stderr);

	File *outerFile= file_open(buf);
	Buffer *oStorageBuffer = buffer_create(outerFile, WORK_MEM, POLICY_LRU);
	HeapAm *oStorage = ExecInitHeapAm(oStorageBuffer, sizeof(ttup)+50);

	ExecHeapAmScanInit(oStorage);
	//fprintf(stderr, "cccc");
	//fflush(stderr);



	snprintf(buf, sizeof(buf),  "%s.part%d.txt",  ifn, numInner);
	//fprintf(stderr, "%s \n", buf);
	//fflush(stderr);

	File *innerFile= file_open(buf);
	Buffer *iStorageBuffer;
	HeapAm *iStorage;


	match=0;
	fhits=0;
	ttup *otup, *itup;

	ttup *tup= private_malloc(sizeof(ttup));
	otup=ExecHeapAmScan(oStorage);
	tup->data = strdup(otup->data);
	ExecHeapAmScanInit(oStorage);


	while (true){ //outer partition
		while (true){ //Inner partition
			iStorageBuffer = buffer_create(innerFile, WORK_MEM, POLICY_LRU);
			iStorage = ExecInitHeapAm(iStorageBuffer, sizeof(ttup)+50);
			//	fprintf(stderr, "O: %d  I: %d \n", numOuter, numInner);
			fflush(stderr);


			ExecHeapAmScanInit(oStorage);
			ExecHeapAmScanInit(iStorage);
			otup=ExecHeapAmScan(oStorage);
			itup = ExecHeapAmScan(iStorage);

			while(otup!=NULL && itup!=NULL )
			{
				//	fprintf(stderr, "O: [%ld,%ld) %s,  I: [%ld,%ld)%s \n", otup->ts, otup->te, otup->data, itup->ts, itup->te, itup->data);

				//  	fprintf(stderr, "1 \n");
				//	fflush(stderr);

				if (overlapFRA(otup,itup))
				{
					//OUTPUT
					tup->ts = MAX(itup->ts,otup->ts);
					tup->te=MIN(itup->te, otup->te);
					strcpy(tup->data,otup->data);
					strcat(tup->data,  " + ");
					tup->data=strcat(tup->data, itup->data);
					//	fprintf(stderr, " O: [%ld,%ld) %s,  I: [%ld,%ld)%s \n", tup->ts, tup->te, tup->data, otup->ts, otup->te, otup->data, itup->ts, itup->te, itup->data);
					//	fprintf(stderr, "OUTPUT: TUP:[%ld,%ld) %s      O: [%ld,%ld) %s,  I: [%ld,%ld)%s \n", tup->ts, tup->te, tup->data, otup->ts, otup->te, otup->data, itup->ts, itup->te, itup->data);

					//	ExecHeapAmInsert(outputStorage, tup);
					match++;
				}
				else
				{
					fhits++;
				}
				if(otup->te<=itup->te)
				{
					otup=ExecHeapAmScan(oStorage);
				}
				else{
					itup = ExecHeapAmScan(iStorage);
				}

			}

			ExecEndHeapAM(iStorage);
			buffer_close(iStorageBuffer);
			file_close(innerFile);
			numInner++;
			snprintf(buf, sizeof(buf),  "%s.part%d.txt", ifn, numInner);
			if( access( buf, F_OK )== -1 )
				break;
			innerFile= file_open(buf);
		}
		numInner=1;
		snprintf(buf, sizeof(buf),  "%s.part%d.txt", ifn, numInner);
		innerFile= file_open(buf);
		iStorageBuffer = buffer_create(innerFile, WORK_MEM, POLICY_LRU);
		iStorage = ExecInitHeapAm(iStorageBuffer, sizeof(ttup)+50);



		ExecEndHeapAM(oStorage);
		buffer_close(oStorageBuffer);
		file_close(outerFile);
		numOuter++;
		snprintf(buf, sizeof(buf), "%s.part%d.txt", ofn, numOuter);
		if( access( buf, F_OK ) == -1 )
			break;
		outerFile= file_open(buf);
		oStorageBuffer = buffer_create(outerFile, WORK_MEM, POLICY_LRU);
		oStorage = ExecInitHeapAm(oStorageBuffer, sizeof(ttup)+50);
	}

	time = time_end(thandle);
	fprintf(stderr,"TIME %lld \t", time);
	fprintf(stderr," %ld \t", ios);
	fprintf(stderr,"MATCHES: %ld \t", match);
	fprintf(stderr, "FALSE HITS %ld \t", fhits);
	fprintf(stderr, "%ld \t", mem_usage);
	fflush(stdout);

	ExecEndHeapAM(outputStorage);
	buffer_close(outputBuffer);
	file_close(outputFile);

	File *sortedOutputFile= file_open(bufRes);
	Buffer *sortedOutputBuffer = buffer_create(sortedOutputFile, WORK_MEM, POLICY_LRU);
	HeapAm *sortedOutputStorage = ExecInitHeapAm(sortedOutputBuffer, sizeof(ttup)+50);
	Sort *sort1 = ExecInitSort(sortedOutputStorage, cmp_timestmp_start, orel->htupsze, WORK_MEM, orel->f->f->type);

	snprintf(bufRes, sizeof(bufRes),  "%sEQUIJOINsorted.txt",  ofn);
	outputFile= file_create(bufRes, orel->f->f->type);
	outputBuffer = buffer_create(outputFile, WORK_MEM, POLICY_LRU);
	outputStorage = ExecInitHeapAm(outputBuffer, orel->htupsze);


	match=0;
	while((tup = ExecSort(sort1)) != NULL){
		ExecHeapAmInsert(outputStorage, tup);
		fprintf(stderr, "%d: [%ld,%ld)%s \n", ++match, tup->ts, tup->te, tup->data);
	}
	ExecEndSort(sort1);

	ExecEndHeapAM(outputStorage);
	buffer_close(outputBuffer);
	file_close(outputFile);

	ExecEndHeapAM(sortedOutputStorage);
	buffer_close(sortedOutputBuffer);
	file_close(sortedOutputFile);

}*/


static void
run_Partition_Fra_DISK(HeapAm *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		HeapAm *irel, long in, timestmp ius, timestmp iue, timestmp il, char *ofn)
{
	size_t mem_usage;
	void *thandle;
	long long time;
	long match, fhits, ios;



	fprintf(stderr, "Deleting old partitions ... ");

		char buf[0x200];
		int numOuter=1;
		snprintf(buf, 199,  "%s.part%d.txt", ofn, numOuter);
		while( access( buf, F_OK )==0 )
		{
			fprintf(stderr, "Remove %s ... ", buf);
			remove(buf);
			snprintf(buf, 199,  "%s.part%d.txt", ofn, ++numOuter);

		}


	fprintf(stderr, "Creating Files FRA ... ");
	fflush(stderr);
	mem_usage = cur_size;
	thandle = time_start();

	Sort *sort1 = ExecInitSort(orel, cmp_timestmp_start, orel->htupsze, WORK_MEM, orel->f->f->type);
	ttup *tup;

	mem_usage = cur_size - mem_usage;
	fprintf(stderr, " done.\n");
	fflush(stderr);



	printf(" %lld \t", time);


	fprintf(stderr, "Partitioning ... ");
	fflush(stderr);
	match = 0;
	fhits = 0;
	ios=0;
	fprintf(stderr, "start ... ");


	/* JOIN */
	size_t nmemtups = orel->tups_per_page;
	List *tups, *partitions;
	ListCell *actualPartition;
	partition *part, *tempPart;
	partitions=NIL;
	int num_partitions=0;
	int flag;
	tup = ExecSort(sort1);

	part=makePartition(1,orel,ofn);
	part->prev_last= ttupdup(tup,orel->htupsze); //QUICK FIX FOR THE FREETUP DONE LATER
	part->last= ttupdup(tup,orel->htupsze);

	tup = ExecSort(sort1);

	partitions = lappend(partitions, part);
	actualPartition= (list_head(partitions));
	while(tup != NULL)
	{
		//fprintf(stderr, "CURRENT TUP: [%ld,%ld] \n",tup->ts, tup->te);
		flag=0;
		/* Find */
		if(tup->ts <= part->last->te && tup->te >=  part->last->ts)// || intervalLen(part->last)>intervalLen(tup)) //OR SMALLER GRAN.
		{
			//fprintf(stderr, "[%ld,%ld] overlapps with [%ld,%ld]. REORDER PARITION %d \n",tup->ts, tup->te, part->last->ts, part->last->te, ((partition *)(actualPartition->ptr_value))->id);

			/* REORDER PARTITION BY SIZE	 */
			reorderParitions(actualPartition);


			actualPartition=list_head(partitions);

			/* VISIT EACH PARTITION AND TRY TO PUT TUP IN IT */
			while(actualPartition!=NULL)
			{
				part=(partition *) (actualPartition->ptr_value);
				//	fprintf(stderr, "Visit partition %d(%d elements)  ", part->id, part->num_tuples);
				if(overlapFRA(part->last, tup))
				{
					//			fprintf(stderr, "[%ld,%ld] overlaps with [%ld,%ld] \n",tup->ts, tup->te, part->last->ts, part->last->te);
					actualPartition=actualPartition->next;
				}
				else
				{
					//fprintf(stderr, "[%ld,%ld] into file %d (not smallest) \n",part->last->ts, part->last->te, part->id);
					//fprintf(stderr, "[%ld,%ld] into buffer (not smallest)\n",tup->ts, tup->te);
					/* write buffer */
					ExecHeapAmInsert(part->partitionStorage, part->last);
					freeTTup(part->prev_last);
					part->prev_last=part->last;
					part->last=ttupdup(tup,orel->htupsze);
					part->num_tuples++;
					break;
				}
			}
			if(actualPartition==NULL)
			{
				//fprintf(stderr, "CREATE NEW PARTITION: %d \n", partitions->length+1);
				part=makePartition(partitions->length+1,orel,ofn);
				part->prev_last= ttupdup(tup,orel->htupsze); //QUICK FIX FOR THE FREETUP DONE LATER
				part->last=ttupdup(tup,orel->htupsze);
				new_head_cell(partitions);
				partitions->head->ptr_value= part;
				actualPartition=list_head(partitions);
			}
		}
		else
		{
		//	fprintf(stderr, "[%ld,%ld] into file %d \n",part->last->ts, part->last->te, part->id);
		//	fprintf(stderr, "[%ld,%ld] into buffer \n",tup->ts, tup->te);

			//			/* write buffer */
			ExecHeapAmInsert(part->partitionStorage, part->last);
			freeTTup(part->prev_last);
			part->prev_last=part->last;
			part->last=ttupdup(tup,orel->htupsze);
			part->num_tuples++;
		}
		//freeTTup(tup);
		tup = ExecSort(sort1);
	}

	time = time_end(thandle);
	fprintf(stderr, "done.\n");
	fprintf(stderr,"num. of partitions %d \n \n \n ", (partitions->length));



	actualPartition=list_head(partitions);
	while(actualPartition!=NULL)
	{
		fprintf(stderr,"PARTITION ID=%d, LEN=%d \n", ((partition *)(actualPartition->ptr_value))->id, ++((partition *)(actualPartition->ptr_value))->num_tuples);
		ExecHeapAmInsert(((partition *)(actualPartition->ptr_value))->partitionStorage, ((partition *)(actualPartition->ptr_value))->last);

		ExecEndHeapAM(((partition *)(actualPartition->ptr_value))->partitionStorage);
		buffer_close(((partition *)(actualPartition->ptr_value))->partitionBuffer);
		file_close(((partition *)(actualPartition->ptr_value))->partitionFile);

		actualPartition=actualPartition->next;
	}
	fflush(stderr);

	//ios = buffer_numReads(oStorageBuffer) + buffer_numReads(iStorageBuffer);

	printf("TIME DIP %lld \t", time);
	printf(" %ld \t", ios);
	printf(" %ld \t", match);
	printf(" %ld \t", fhits);
	printf(" %ld \t", mem_usage);
	fflush(stdout);


	ExecEndSort(sort1);

}



static List **
run_Partition_Fra_MM(List *rel, long on, timestmp ous, timestmp oue, timestmp ol)
{
	size_t mem_usage;
	void *thandle;
	long long time;
	long match, fhits, ios;
	List* *outer_partition_tups;

	outer_partition_tups=malloc(10000*sizeof(List *));

	ListCell *tup_cell, *outer_tup_cell[10000], *inner_tup_cell[10000];


	mem_usage = cur_size;

	thandle = time_start();

	 list_sort(rel, cmp_timestmp_timestmp);
	ttup *tup;

	mem_usage = cur_size - mem_usage;
	fprintf(stderr, " done.\n");
	fflush(stderr);



	printf(" %lld \t", time);


	fprintf(stderr, "Partitioning ... ");
	fflush(stderr);
	match = 0;
	fhits = 0;
	ios=0;
	// fprintf(stderr, "start.. ... ");


	List *tups, *partitions;
	ListCell *actualPartition;
	partition *part, *tempPart;
	partitions=NIL;
	int num_partitions=0;
	int flag;

	int i=1;
	int num_tuples=0;
	tup_cell=rel->head;
    tup = ((ttup *)lfirst(tup_cell));
	outer_partition_tups[i]=     lappend(outer_partition_tups[i], tup);
	outer_tup_cell[i]=outer_partition_tups[i]->head;



	foreach(tup_cell, rel)
	{
		num_tuples++;
	    tup = ((ttup *)lfirst(tup_cell));
		//fprintf(stderr, "CURRENT TUP: [%ld,%ld] \n",tup->ts, tup->te);
		flag=0;
		/* Find */

		//fprintf(stderr, "current [%ld,%ld] VS last [%ld,%ld] \n",tup->ts, tup->te, ((ttup *)lfirst(outer_tup_cell[i]))->ts, ((ttup *)lfirst(outer_tup_cell[i]))->te, i);

		if(tup->ts <=  ((ttup *)lfirst(outer_tup_cell[i]))->te && tup->te >=   ((ttup *)lfirst(outer_tup_cell[i]))->ts)// || intervalLen(part->last)>intervalLen(tup)) //OR SMALLER GRAN.
		{
			// fprintf(stderr, "[%ld,%ld] overlapps with [%ld,%ld]. \n",tup->ts, tup->te, ((ttup *)lfirst(outer_tup_cell[i]))->ts, ((ttup *)lfirst(outer_tup_cell[i]))->te, i);

			/* REORDER PARTITION BY SIZE	 */
			//reorderParitions(actualPartition);


			/*if(intervalLen((ttup *)lfirst(outer_tup_cell[i]))>100)
			{
			while(i<num_partitions)
			{

				if(((ttup *)lfirst(outer_tup_cell[i]))->te > ((ttup *)lfirst(outer_tup_cell[i+1]))->te )
				{
					//SWAP PART
					ListCell *temp2 = (outer_tup_cell[i]);
					outer_tup_cell[i]=outer_tup_cell[i+1];
					outer_tup_cell[i+1]=temp2;
					List* *temp;
					temp= outer_partition_tups[i];
					outer_partition_tups[i]=outer_partition_tups[i+1];
					outer_partition_tups[i+1]=temp;
				}
				else
					break;
				i++;

			}
			}*/

			//actualPartition=list_head(partitions);
			i=1;
			/* VISIT EACH PARTITION AND TRY TO PUT TUP IN IT */
			while(i<=num_partitions)
			{
				//	fprintf(stderr, "Visit partition %d(%d elements)  ", part->id, part->num_tuples);
				if(overlapFRA( ((ttup *)lfirst(outer_tup_cell[i])), tup))
				{
					//		fprintf(stderr, "[%ld,%ld] overlaps with [%ld,%ld] \n",tup->ts, tup->te, ((ttup *)lfirst(outer_tup_cell[i]))->ts, ((ttup *)lfirst(outer_tup_cell[i]))->te);
					i++;
				}
				else
				{
					//fprintf(stderr, "[%ld,%ld] into partition %d \n",((ttup *)lfirst(outer_tup_cell[i]))->ts, ((ttup *)lfirst(outer_tup_cell[i]))->te, i);
					/* write buffer */
					outer_partition_tups[i]= lappend(outer_partition_tups[i], tup);
					outer_tup_cell[i]=outer_tup_cell[i]->next;
					break;
				}
			}
			if(i>num_partitions)
			{
				num_partitions++;
				outer_partition_tups[i]= lappend(NIL, tup);
				outer_tup_cell[i]=outer_partition_tups[i]->head;

				//fprintf(stderr, "CREATE NEW PARTITION: %d \n", i);
			}
		}
		else
		{
		//	fprintf(stderr, "[%ld,%ld] into file %d \n",part->last->ts, part->last->te, part->id);
		//	fprintf(stderr, "[%ld,%ld] into buffer \n",tup->ts, tup->te);

			//			/* write buffer */
			//fprintf(stderr, "add to partition %d \n", i);

			outer_partition_tups[i]= lappend(outer_partition_tups[i], tup);
			outer_tup_cell[i]=outer_tup_cell[i]->next;
		}
		//freeTTup(tup);
		if(num_tuples>on)
		{
			break;
		}
	}

	time = time_end(thandle);
	fprintf(stderr, "done.\n");



	i=0;
	/* while((++i)<=num_partitions)
	{
		fprintf(stderr,"PARTITION ID=%d, LEN=%d \n", i, (outer_partition_tups[i])->length);
	}
	fflush(stderr); */


	outer_partition_tups[i]=NIL;
	//ios = buffer_numReads(oStorageBuffer) + buffer_numReads(iStorageBuffer);


	return outer_partition_tups;

}


static void
run_DIP_MM(List *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		List *irel, long in, timestmp ius, timestmp iue, timestmp il, char *ofn)
{

	size_t mem_usage;
	void *thandle;
	long long time;
	long match, fhits;

	long n_outer = on;
	long n_inner = in;
	List **outer_partition_tups;
	List **inner_partition_tups;

	int num_inner_partitions=0;
	int num_outer_partitions=0;
	fprintf(stderr, "Creating DIP MEM... ");
	fflush(stderr);
	mem_usage = cur_size;
	thandle = time_start();

	// create outer DIP

	outer_partition_tups=run_Partition_Fra_MM(orel,  on,  ous,  oue,  ol);
	inner_partition_tups=run_Partition_Fra_MM(irel,  in,  ius,  iue,  il);



	time = time_end(thandle);
	mem_usage = cur_size - mem_usage;
	fprintf(stderr, " done.\n");
	fflush(stderr);

	int i,j;

	for(i=1; i<=10000; i++)
	{
		if(outer_partition_tups[i]!=NIL)
		{
			num_outer_partitions++;
		}
		else
			break;
	}

	// create inner DIP

	for(j=1; j<=10000; j++)
	{
		if(inner_partition_tups[j]!=NIL)
		{
			num_inner_partitions++;
		}
		else
			break;
	}

	i=1;j=1;


	fprintf(stderr, "Joining OIP ... ");

	fprintf(stderr,"dip_num_outer_partitions %d \t", num_outer_partitions);
	fprintf(stderr,"dip_num_inner_partitions %d \t", num_inner_partitions);
	fprintf(stderr,"TIME DIP %lld \t", time);

	fflush(stderr);
	match = 0;
	fhits = 0;
	thandle = time_start();
	  ListCell *outer_tup_cell, *inner_tup_cell;
		ttup *otup, *itup;

	while (true){ //outer partition

		while (true){ //Inner partition

			//fprintf(stderr, "O: %d I: %d  \n", i,j);
			//fflush(stderr);
            outer_tup_cell = list_head(outer_partition_tups[i]) ;
        	otup= ((ttup *)lfirst(outer_tup_cell)) ;


            inner_tup_cell = list_head(inner_partition_tups[j]) ;
        	itup= ((ttup *)lfirst(inner_tup_cell)) ;


        	while(true )
        	{
        	  if(otup->te<=itup->te)
        	  {
        	    if(itup->ts < otup->te)
        	      match++;
        	    else
        	      fhits++;

        	    // advance outer
        	    outer_tup_cell = lnext(outer_tup_cell);
        	    if(outer_tup_cell)
        	    	otup       = ((ttup *)lfirst(outer_tup_cell)) ;

        	    else
        	    	break;
        	  }
        	  else
        	  {
        	    if(otup->ts < itup->te)
        	      match++;
        	    else
        	      fhits++;

        	    // advance inner
        	    inner_tup_cell = lnext(inner_tup_cell);
        	    if(inner_tup_cell)
        	      itup       = ((ttup *)lfirst(inner_tup_cell)) ;
        	    else
        	    	break;
        	  }
        	}

			j++;
			if( j>num_inner_partitions)
				break;
		}
		j=1;

		i++;
		if( i> num_outer_partitions )
			break;
	}

	time = time_end(thandle);
	fprintf(stderr,"TIME JOIN %lld \t", time);
	fprintf(stderr,"MATCHES: %ld \t", match);
	fprintf(stderr, "FALSE HITS %ld \t", fhits);
	fprintf(stderr, "num_out_partitions %d \t", num_outer_partitions);
	fprintf(stderr, "num_inner_partitions %d \t", num_inner_partitions);
	fflush(stdout);


}



/*

static void
run_Partition_Fra_DISK_BALANCED(HeapAm *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		HeapAm *irel, long in, timestmp ius, timestmp iue, timestmp il, char *ofn)
{
	size_t mem_usage;
	void *thandle;
	long long time;
	long match, fhits, ios;



	fprintf(stderr, "Deleting old partitions ... ");

		char buf[0x200];
		int numOuter=1;
		snprintf(buf, 199,  "%s.part%d.txt", ofn, numOuter);
		while( access( buf, F_OK )==0 )
		{
			fprintf(stderr, "Remove %s ... ", buf);
			remove(buf);
			snprintf(buf, 199,  "%s.part%d.txt", ofn, ++numOuter);

		}


	fprintf(stderr, "Creating Files FRA ... ");
	fflush(stderr);
	mem_usage = cur_size;
	thandle = time_start();

	File *oStorageFile = file_create("mergejoin_outer_storage.dat", orel->f->f->type);
	Buffer *oStorageBuffer = buffer_create(oStorageFile, WORK_MEM, POLICY_LRU);
	HeapAm *oStorage = ExecInitHeapAm(oStorageBuffer, orel->htupsze);
	Sort *sort1 = ExecInitSort(orel, cmp_timestmp_start, orel->htupsze, WORK_MEM, orel->f->f->type);
	ttup *tup;
	while((tup = ExecSort(sort1)) != NULL)
		ExecHeapAmInsert(oStorage, tup);
	ExecEndSort(sort1);

	mem_usage = cur_size - mem_usage;
	fprintf(stderr, " done.\n");
	fflush(stderr);



	printf(" %lld \t", time);

	buffer_clear(oStorageBuffer);


	fprintf(stderr, "Partitioning ... ");
	fflush(stderr);
	match = 0;
	fhits = 0;
	ios=0;
	buffer_clearIOStat(oStorageBuffer);
	fprintf(stderr, "start ... ");


	 JOIN
	size_t nmemtups = orel->tups_per_page;
	List *tups, *partitions;
	ListCell *actualPartition, *actualPartition2, *temp;
	partition *part, *tempPart;
	partitions=NIL;
	int num_partitions=0;
	int flag;
	ExecHeapAmScanInit(oStorage);
	tup = ExecHeapAmScan(oStorage);

	part=makePartition(1,oStorage,ofn);
	part->prev_last= ttupdup(tup,oStorage->htupsze); //QUICK FIX FOR THE FREETUP DONE LATER
	part->last= ttupdup(tup,oStorage->htupsze);

	tup = ExecHeapAmScan(oStorage);

	partitions = lappend(partitions, part);
	actualPartition= (list_head(partitions));
	while(tup != NULL)
	{
		//fprintf(stderr, "CURRENT TUP: [%ld,%ld] \n",tup->ts, tup->te);
		flag=0;
		 Find
		if(overlapFRA(part->last, tup))// || intervalLen(part->last)>intervalLen(tup)) //OR SMALLER GRAN.
		{
			//fprintf(stderr, "[%ld,%ld] overlapps with [%ld,%ld]. REORDER PARITION %d \n",tup->ts, tup->te, part->last->ts, part->last->te, ((partition *)(actualPartition->ptr_value))->id);

			 REORDER PARTITION BY SIZE
			reorderParitions(actualPartition);

			 BEFORE SWITCHING TO A LARGER PARTITION, LET'S TRY TO PUT ITS LAST ELEMENT IN A BIGGER PARTITION

					part=(partition *) (list_head(partitions)->ptr_value);
			actualPartition=list_head(partitions)->next;

			if(overlapFRA(part->last, tup) && !overlapFRA(part->prev_last, tup))//
			{

				while(actualPartition!=NULL)
				{
					tempPart=(partition *) (actualPartition->ptr_value);
					if(part->last->ts > tempPart->last->te) //No !overlapFRA(part->last,tempPart->last) because after the reorder part may be an old partition
					{
						fprintf(stderr, "Partition %d(size %d, ending with [%ld,%ld]) is the smallest: move [%ld,%ld] to partition %d(size %d) and put new ele [%ld,%ld] in it \n",
								part->id, part->num_tuples, part->last->ts, part->last->te, part->last->ts, part->last->te,  tempPart->id, tempPart->num_tuples, tup->ts, tup->te);
						ExecHeapAmInsert(tempPart->partitionStorage, tempPart->last);
						freeTTup(tempPart->prev_last);
						tempPart->prev_last=tempPart->last;
						tempPart->last=part->last;
						part->last=ttupdup(tup,oStorage->htupsze);
						tempPart->num_tuples++;
						reorderParitions(actualPartition);
						tup = ExecHeapAmScan(oStorage);
						flag=1;
						break;
					}
					actualPartition=actualPartition->next;
				}
			}

			//


			if (flag==1)
			{
				actualPartition2=(list_head(partitions))->next;
				while(tempPart->id != ((partition *) (actualPartition2->ptr_value))->id )
				{
					//fprintf(stderr, "\n \n compare %d with %d \n \n", tempPart->id, ((partition *) (actualPartition->ptr_value))->id );

					if ( tempPart->last->te < ((partition *) (actualPartition2->ptr_value))->last->te &&
							tempPart->prev_last->te < ((partition *) (actualPartition2->ptr_value))->last->ts &&
							((partition *) (actualPartition2->ptr_value))->prev_last->te < tempPart->last->ts )
					{
						 EXCHANGE TUPLES
						fprintf(stderr, "Partition %d(size %d) ends with [%ld,%ld]) while partition %d(size %d) ends with [%ld,%ld]: EXCHANGE THE ELEMENTS! \n",
								tempPart->id, tempPart->num_tuples, tempPart->last->ts, tempPart->last->te,
								((partition *) (actualPartition2->ptr_value))->id, ((partition *) (actualPartition2->ptr_value))->num_tuples,
								((partition *) (actualPartition2->ptr_value))->last->ts, ((partition *) (actualPartition2->ptr_value))->last->te);
						ttup *tmp = ttupdup(tempPart->last, oStorage->htupsze);
						freeTTup(tempPart->last);
						tempPart->last=((partition *) (actualPartition2->ptr_value))->last;
						((partition *) (actualPartition2->ptr_value))->last = tmp;
						break;
					}
					actualPartition2=actualPartition2->next;
				}
				actualPartition=list_head(partitions);
				continue;
			}

			actualPartition=list_head(partitions);

			 VISIT EACH PARTITION AND TRY TO PUT TUP IN IT
			while(actualPartition!=NULL)
			{
				part=(partition *) (actualPartition->ptr_value);
				//	fprintf(stderr, "Visit partition %d(%d elements)  ", part->id, part->num_tuples);
				if(overlapFRA(part->last, tup))
				{
					//			fprintf(stderr, "[%ld,%ld] overlaps with [%ld,%ld] \n",tup->ts, tup->te, part->last->ts, part->last->te);
					actualPartition=actualPartition->next;
				}
				else
				{
					//fprintf(stderr, "[%ld,%ld] into file %d (not smallest) \n",part->last->ts, part->last->te, part->id);
					//fprintf(stderr, "[%ld,%ld] into buffer (not smallest)\n",tup->ts, tup->te);
					 write buffer
					ExecHeapAmInsert(part->partitionStorage, part->last);
					freeTTup(part->prev_last);
					part->prev_last=part->last;
					part->last=ttupdup(tup,oStorage->htupsze);
					part->num_tuples++;
					break;
				}
			}
			if(actualPartition==NULL)
			{
				fprintf(stderr, "CREATE NEW PARTITION: %d \n", partitions->length+1);
				part=makePartition(partitions->length+1,oStorage,ofn);
				part->prev_last= ttupdup(tup,oStorage->htupsze); //QUICK FIX FOR THE FREETUP DONE LATER
				part->last=ttupdup(tup,oStorage->htupsze);
				new_head_cell(partitions);
				partitions->head->ptr_value= part;
				actualPartition=list_head(partitions);
			}
		}
		else
		{
		//	fprintf(stderr, "[%ld,%ld] into file %d \n",part->last->ts, part->last->te, part->id);
		//	fprintf(stderr, "[%ld,%ld] into buffer \n",tup->ts, tup->te);

			//			 write buffer
			ExecHeapAmInsert(part->partitionStorage, part->last);
			freeTTup(part->prev_last);
			part->prev_last=part->last;
			part->last=ttupdup(tup,oStorage->htupsze);
			part->num_tuples++;
		}
		//freeTTup(tup);
		tup = ExecHeapAmScan(oStorage);
	}

	time = time_end(thandle);
	fprintf(stderr, "done.\n");
	fprintf(stderr,"num. of partitions %d \n \n \n ", (partitions->length));

	printf("TIME  %lld \t", time);
	printf(" %ld \t", ios);
	printf(" %ld \t", match);
	printf(" %ld \t", fhits);
	printf(" %ld \t", mem_usage);
	fflush(stdout);


	actualPartition=list_head(partitions);
	while(actualPartition!=NULL)
	{
		fprintf(stderr,"PARTITION ID=%d, LEN=%d \n", ((partition *)(actualPartition->ptr_value))->id, ++((partition *)(actualPartition->ptr_value))->num_tuples);
		ExecHeapAmInsert(((partition *)(actualPartition->ptr_value))->partitionStorage, ((partition *)(actualPartition->ptr_value))->last);

		fprintf(stderr,"a");
		fflush(stdout);
		ExecEndHeapAM(((partition *)(actualPartition->ptr_value))->partitionStorage);
		fprintf(stderr,"b");
		fflush(stdout);
		buffer_close(((partition *)(actualPartition->ptr_value))->partitionBuffer);
		fprintf(stderr," c \t");
		fflush(stdout);
		file_close(((partition *)(actualPartition->ptr_value))->partitionFile);

		actualPartition=actualPartition->next;
	}
	fflush(stderr);

	//ios = buffer_numReads(oStorageBuffer) + buffer_numReads(iStorageBuffer);

	printf("TIME  %lld \t", time);
	printf(" %ld \t", ios);
	printf(" %ld \t", match);
	printf(" %ld \t", fhits);
	printf(" %ld \t", mem_usage);
	fflush(stdout);


	ExecEndHeapAM(oStorage);
	buffer_close(oStorageBuffer);
	file_close(oStorageFile);

}*/

static void
reorderParitions(ListCell *lc)
{
	//fprintf(stderr, "Reorder Partition %d(size %d) \n", ((partition *)(lc->ptr_value))->id, ((partition *)(lc->ptr_value))->num_tuples);
	while(lc->next!=NULL)
	{
		/*fprintf(stderr, "Reorder: Compare Partition %d(size %d, prev_last[%ld,%ld], last[%ld,%ld]) with partition %d(size %d, prev_last[%ld,%ld], last[%ld,%ld]) \n",
				((partition *)(actualPartition->ptr_value))->id, ((partition *)(actualPartition->ptr_value))->num_tuples,
				((partition *)(actualPartition->ptr_value))->prev_last->ts, ((partition *)(actualPartition->ptr_value))->prev_last->te,
				((partition *)(actualPartition->ptr_value))->last->ts, ((partition *)(actualPartition->ptr_value))->last->te,
				((partition *) (actualPartition->next->ptr_value))->id, ((partition *) (actualPartition->next->ptr_value))->num_tuples,
				((partition *) (actualPartition->next->ptr_value))->prev_last->ts, ((partition *) (actualPartition->next->ptr_value))->prev_last->te,
				((partition *) (actualPartition->next->ptr_value))->last->ts, ((partition *) (actualPartition->next->ptr_value))->last->te);*/

		if(((partition *) (lc->ptr_value))->num_tuples > ((partition *) (lc->next->ptr_value))->num_tuples)
		{
			/*fprintf(stderr, "Reorder: Exchange Partition %d(size %d) with partition %d(size %d) \n",
					((partition *)(lc->ptr_value))->id, ((partition *)(lc->ptr_value))->num_tuples,
					((partition *) (lc->next->ptr_value))->id, ((partition *) (lc->next->ptr_value))->num_tuples);*/
			void *tmp=lc->ptr_value;
			lc->ptr_value=lc->next->ptr_value;
			lc->next->ptr_value=tmp;

		}else break;

		lc=lc->next;
	}
}

static void
run_readFile_Fra_DISK(char *fname)
{
	size_t mem_usage;
	void *thandle;
	ttup *tup;
	long long time;

	thandle = time_start();

	File *inputF= file_open(fname);
	fprintf(stderr, "%s  \n", fname);

	Buffer *oStorageBuffer = buffer_create(inputF, WORK_MEM, POLICY_LRU);
	HeapAm *oStorage = ExecInitHeapAm(oStorageBuffer, sizeof(ttup)+50);


	ExecHeapAmScanInit(oStorage);
	tup = ExecHeapAmScan(oStorage);
	while(tup != NULL)
	{
		/* Find */
		fprintf(stderr, "%ld  %ld  %s  \n", tup->ts, tup->te, tup->data);
		tup = ExecHeapAmScan(oStorage);
	}

	time = time_end(thandle);
	printf("TIME %lld \t", time);
	fflush(stderr);

	//ios = buffer_numReads(oStorageBuffer) + buffer_numReads(iStorageBuffer);

	fflush(stdout);

	ExecEndHeapAM(oStorage);

}


static int intervalLen(ttup *tup1)
{
	return (tup1->te-tup1->ts);
}

static int overlapFRA(ttup *tup1, ttup *tup)
{
	if( (tup1->ts <= tup->te && tup1->te >= tup->ts) )
		return 1;
	return 0;
}

/*
static void
run_MERGEJOIN_EQUALITY_DISK(HeapAm *orel, long on, timestmp ous, timestmp oue, timestmp ol,
    HeapAm *irel, long in, timestmp ius, timestmp iue, timestmp il)
{
  size_t mem_usage;
  void *thandle;
  long long time;
  long match, fhits, ios;

  fprintf(stderr, "Creating MERGEJOIN ... ");
  fflush(stderr);
  mem_usage = cur_size;
  thandle = time_start();

  File *oStorageFile = file_create("mergejoin_outer_storage.dat", orel->f->f->type);
  Buffer *oStorageBuffer = buffer_create(oStorageFile, WORK_MEM, POLICY_LRU);
  HeapAm *oStorage = ExecInitHeapAm(oStorageBuffer, orel->htupsze);
  Sort *sort1 = ExecInitSort(orel, cmp_timestmp_timestmp, orel->htupsze, WORK_MEM, orel->f->f->type);
  ttup *tup;
  while((tup = ExecSort(sort1)) != NULL)
    ExecHeapAmInsert(oStorage, tup);
  ExecEndSort(sort1);

  File *iStorageFile = file_create("mergejoin_inner_storage.dat", irel->f->f->type);
  Buffer *iStorageBuffer = buffer_create(iStorageFile, WORK_MEM, POLICY_LRU);
  HeapAm *iStorage = ExecInitHeapAm(iStorageBuffer, irel->htupsze);
  Sort *sort2 = ExecInitSort(irel, cmp_timestmp_timestmp, irel->htupsze, WORK_MEM, irel->f->f->type);
  while((tup = ExecSort(sort2)) != NULL)
    ExecHeapAmInsert(iStorage, tup);
  ExecEndSort(sort2);

  time = time_end(thandle);
  mem_usage = cur_size - mem_usage;
  fprintf(stderr, " done.\n");
  fflush(stderr);

  printf(" %lld \t", time);

  buffer_clear(iStorageBuffer);
  buffer_clear(oStorageBuffer);

  fprintf(stderr, "Joining MERGEJOIN ... ");
  fflush(stderr);
  match = 0;
  fhits = 0;
  ios=0;
  buffer_clearIOStat(oStorageBuffer);
  buffer_clearIOStat(iStorageBuffer);
  thandle = time_start();

  // JOIN
  ExecHeapAmScanInit(oStorage);
  ExecHeapAmScanInit(iStorage);
  ttup *otup, *oprev, *itup, *itup_marked;
  oprev = NULL;
  otup = ExecHeapAmScan(oStorage);
  itup = ExecHeapAmScan(iStorage);
  itup_marked = NULL;
  int qual;

  while(otup && itup)
  {
    qual = cmp_timestmp_timestmp(otup, itup);
    if(qual == 0)
    {
      if(!itup_marked)
      {
        itup_marked = ttupdup(itup, iStorage->htupsze);
        ExecMarkPos(iStorage);
      }
      match++;
      itup = ExecHeapAmScan(iStorage);
    }
    else if(qual < 0)
    {
      fhits++;
      if(oprev)
        freeTTup(oprev);
      oprev = ttupdup(otup, oStorage->htupsze);
      otup = ExecHeapAmScan(oStorage);
      if(itup_marked && cmp_timestmp_timestmp(oprev, otup) == 0)
      {
        itup = itup_marked;
        ExecRestorPos(iStorage);
      }
      else if(itup_marked)
      {
        freeTTup(itup_marked);
        itup_marked = NULL;
      }
    }
    else
    {
      fhits++;
      itup = ExecHeapAmScan(iStorage);
    }

  }

  time = time_end(thandle);
  fprintf(stderr, "done.\n");
  fflush(stderr);

  ios = buffer_numReads(oStorageBuffer) + buffer_numReads(iStorageBuffer);

  printf(" %lld \t", time);
  printf(" %ld \t", ios);
  printf(" %ld \t", match);
  printf(" %ld \t", fhits);
  printf(" %ld \t", mem_usage);
  fflush(stdout);

  ExecEndHeapAM(oStorage);
  ExecEndHeapAM(iStorage);
  buffer_close(oStorageBuffer);
  buffer_close(iStorageBuffer);
  file_close(oStorageFile);
  file_close(iStorageFile);

}*/

static void
run_NESTLOOP_DISK(HeapAm *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		HeapAm *irel, long in, timestmp ius, timestmp iue, timestmp il)
{
	void *thandle;
	long long time;
	long match, fhits, ios;
	ttup *otup, *itup;
	List *otups;
	size_t nmemtups;
	ListCell *otupcell;

	buffer_clearIOStat(orel->f);
	buffer_clearIOStat(irel->f);

	fprintf(stderr, "Joining BLOCK NESTED LOOP ... ");
	fflush(stderr);
	match = 0;
	fhits = 0;
	ios=0;
	thandle = time_start();

	nmemtups = orel->tups_per_page;

	/* JOIN */
	ExecHeapAmScanInit(orel);
	do
	{
		otups = NIL;
		while(list_length(otups) <= nmemtups && (otup = ExecHeapAmScan(orel)) != NULL)
			otups = lappend(otups, ttupdup(otup, orel->htupsze));

		if(list_length(otups) < 1)
			break;

		ExecHeapAmScanInit(irel);
		while((itup = ExecHeapAmScan(irel)) != NULL)
		{
			foreach(otupcell, otups)
    						  {
				otup = (ttup *)lfirst(otupcell);
				if (itup->ts <= otup->te
						&& itup->te >= otup->ts)
					match++;
				else
					fhits++;
    						  }
		}
		foreach(otupcell, otups)
		freeTTup(lfirst(otupcell));
		list_free(otups);

	} while (1);

	time = time_end(thandle);
	fprintf(stderr, "done.\n");
	fflush(stderr);

	ios = buffer_numReads(orel->f) + buffer_numReads(irel->f);

	printf(" %ld \t", 0L); /* creation time */
	printf(" %lld \t", time); /* join time */
	printf(" %ld \t", ios);
	printf(" %ld \t", match);
	printf(" %ld \t", fhits);
	printf(" %ld \t", 0L); /* mem_usage */
	fflush(stdout);
}

static void run_SEGTREE_MM(List *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		List *irel, long in, timestmp ius, timestmp iue, timestmp il)
{
	SEGTREE *segtree;

	size_t mem_usage;
	void *thandle;
	long long time;
	long match, fhits;

	fprintf(stderr, "Creating SEGTREE ... ");
	fflush(stderr);
	mem_usage = cur_size;
	thandle = time_start();

	segtree = segtree_create(irel);

	time = time_end(thandle);
	mem_usage = cur_size - mem_usage;
	fprintf(stderr, " done.\n");
	fflush(stderr);

	printf(" %lld \t", time);

	fprintf(stderr, "Joining SEGTREE ... ");
	fflush(stderr);
	match = 0;
	fhits = 0;
	thandle = time_start();

	segtree_Join(orel, segtree, &match, &fhits);

	time = time_end(thandle);
	fprintf(stderr, "done.\n");
	fflush(stderr);

	printf(" %lld \t", time);
	printf(" %ld \t", 0L);
	printf(" %ld \t", match);
	printf(" %ld \t", fhits);
	printf(" %ld \t", mem_usage);
	fflush(stdout);

	segtree_close(segtree);
}

static void run_SEGTREE_DISK(HeapAm *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		HeapAm *irel, long in, timestmp ius, timestmp iue, timestmp il)
{
	SEGTREE *segtree;

	size_t mem_usage;
	void *thandle;
	long long time;
	long match, fhits, ios;

	fprintf(stderr, "Creating SEGTREE ... ");
	fflush(stderr);
	mem_usage = cur_size;
	thandle = time_start();

	segtree = segtree_create_disk(irel);

	time = time_end(thandle);
	mem_usage = cur_size - mem_usage;
	fprintf(stderr, " done.\n");
	fflush(stderr);

	printf(" %lld \t", time);

	buffer_clearIOStat(orel->f);
	buffer_clearIOStat(irel->f);

	fprintf(stderr, "Joining SEGTREE ... ");
	fflush(stderr);
	match = 0;
	fhits = 0;
	thandle = time_start();

	segtree_Join_disk(orel, segtree, &match, &fhits);

	time = time_end(thandle);
	fprintf(stderr, "done.\n");
	fflush(stderr);

	ios = buffer_numReads(orel->f) + buffer_numReads(irel->f);

	printf(" %lld \t", time);
	printf(" %ld \t", ios);
	printf(" %ld \t", match);
	printf(" %ld \t", fhits);
	printf(" %ld \t", mem_usage);
	fflush(stdout);

	segtree_close(segtree); /* TODO needs disk version */
}
