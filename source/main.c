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
#include <stdlib.h>

typedef struct Filenode {
	int isopen;
	int counter;
	File* f;
	struct Filenode* next;
	char* name;
	Buffer* b;
	HeapAm* h;
	struct ttup* lasttup;
} Filenode;
typedef struct ArrayList{
	struct ArrayList* next;
	ttup* partition;
	int position;
	int numberoftup;
}ArrayList;
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
	int DIPMem; /* Partitioning Francesco */
	int DIPDisk; /* Partitioning Francesco */
	int readFile; /* Read Binary File */
	int nestloop; /* nested loop */
	int segtree; /* index nested loop using segment tree */

	int MergeMode; /*After DIP, we chose can Join, Antijoin etc*/
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
	int parallel;
	bool rit_clustered;

	/*
	 * storage
	 */
	int on_disk;
	int with_order;
	int on_mem_disk;
	int on_array;
};

static void
run_readFile_Fra_DISK(char *fname);
static void
reorderParitions(ListCell *lc);

static void
run_Partition_Fra_DISK(HeapAm *orel, long on, timestmp ous, timestmp oue,
		timestmp ol, HeapAm *irel, long in, timestmp ius, timestmp iue,
		timestmp il, char *ofn);

static List *
run_Partitioning_Ordered_MM(List *rel, long on, timestmp ous, timestmp oue,
		timestmp ol);
static Filenode* run_Partitioning_Ordered_DISK(HeapAm *orel, long on,
		timestmp ous, timestmp oue, timestmp ol, int memdisk);
void DIPMerge(HeapAm* orel, HeapAm* irel);
static long long MergeArray(ArrayList* orel, ArrayList* irel, int mergemode, int para,
		ttup* x, ttup** yarray);
static void
run_DIP_MM(List *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		List *irel, long in, timestmp ius, timestmp iue, timestmp il, char *ofn);
static long long MergeDiskParallel(HeapAm** outer, HeapAm* irel, int mergemode,
		int para, ttup* x, ttup** yarray, ttup** rarray);
static void
run_DIP_Ordered_MM(List *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		List *irel, long in, timestmp ius, timestmp iue, timestmp il, char *ofn,
		int mode, int parallel, int arr);
static void
run_DIP_Ordered_Disk(HeapAm *orel, long on, timestmp ous, timestmp oue,
		timestmp ol, HeapAm *irel, long in, timestmp ius, timestmp iue,
		timestmp il, char *ofn, int mode, int parallel,int on_mem_disk);

static int overlapFRA(ttup *tup1, ttup *tup);
static  int overlapFRAtup(ttup tup1, ttup *tup);
static int overlapFRAtuptup(ttup tup1, ttup tup);
static int intervalLen(ttup *tup1);
static long long DIPmerge(HeapAm* outer, HeapAm* inner, int mergemode, ttup* x,
		ttup* y);
static long long DipmergeMem(List* orel, List* irel, int mergemode, ttup* x,
		ttup* y);
static long long MergeParallel(List* orel, List* irel, int mergemode, int para,
		ttup* x, ttup** yarray);
static void
run_NESTLOOP_DISK(HeapAm *orel, long on, timestmp ous, timestmp oue,
		timestmp ol, HeapAm *irel, long in, timestmp ius, timestmp iue,
		timestmp il);

static void
run_SEGTREE_MM(List *orel, long on, timestmp ous, timestmp oue, timestmp ol,
		List *irel, long in, timestmp ius, timestmp iue, timestmp il);
static void run_SEGTREE_DISK(HeapAm *orel, long on, timestmp ous, timestmp oue,
		timestmp ol, HeapAm *irel, long in, timestmp ius, timestmp iue,
		timestmp il);

static int cmp_timestmp_start(const void *a, const void *b);

static void diskJoin(HeapAm *orel, long on, timestmp ous, timestmp oue,
		timestmp ol, HeapAm *irel, long in, timestmp ius, timestmp iue,
		timestmp il, char *ofn);

static void
getParams(int argc, char *argv[], struct params *params);
int partition_folder = 0; /* the two folders in which the partitions are going to be stored */
long long counter = 0;
long long false_hits = 0;
long long num_inner_partitions = 0;
long long num_outer_partitions = 0;
long long const MAXLONG = 9223372036854775807;
long nump = 0;
int helper = 0;
List *ylist;
List *z = NIL;

int main(int argc, char *argv[]) {

	struct params params;
	getParams(argc, argv, &params);

	long n_outer, l_outer, n_inner, l_inner;
	timestmp us_outer, ue_outer, us_inner, ue_inner;

	void *outer_rel, *inner_rel;

	if (params.opt == 1) {
		/* random */
		n_outer = params.on;
		l_outer = params.ol;
		us_outer = params.ous;
		ue_outer = params.oue;
		n_inner = params.in;
		l_inner = params.il;
		us_inner = params.ius;
		ue_inner = params.iue;

		if (n_outer < 1 || l_outer < 1 || us_outer < 1 || ue_outer < 2
				|| n_inner < 1 || l_inner < 1 || us_inner < 1 || ue_inner < 2) {
			fprintf(stderr, "ERROR: parameters incorrect!\n");
			exit(-1);
		}

		if (params.on_disk || params.on_mem_disk) {

			FILE_TYPE type = TYPE_DISK;
			if (params.on_mem_disk)
				type = TYPE_MEM;
			outer_rel = makeRandDiskRel("orel.dat", sizeof(ttup) + 50, n_outer,
					l_outer, us_outer, ue_outer, type);
			inner_rel = makeRandDiskRel("irel.dat", sizeof(ttup) + 50, n_inner,
					l_inner, us_inner, ue_inner, type);
		} else {
			outer_rel = makeRandRel(n_outer, l_outer, us_outer, ue_outer);
			inner_rel = makeRandRel(n_inner, l_inner, us_inner, ue_inner);
		}

	} else if (params.opt == 2) {
		/* from file */

		if (!params.ofn || !params.ifn) {
			fprintf(stderr, "ERROR: parameters incorrect!\n");
			exit(-1);
		}

		if (params.on_disk || params.on_mem_disk) {

			fflush(stdout);
			FILE_TYPE type = TYPE_DISK;
			if (params.on_mem_disk){
				type = TYPE_MEM;

			}
			outer_rel = makeDiskRelFromFile("orel.dat", sizeof(ttup) + 50,
					params.ofn, type);
			inner_rel = makeDiskRelFromFile("irel.dat", sizeof(ttup) + 50,
					params.ifn, type);

		} else {

			outer_rel = makeRelFromFile(params.ofn);

			inner_rel = makeRelFromFile(params.ifn);
		}
	} else if (params.opt == 3) {
		/* random with Gaussian random interval duration */
		n_outer = params.on;
		us_outer = params.ous;
		ue_outer = params.oue;
		n_inner = params.in;
		us_inner = params.ius;
		ue_inner = params.iue;

		if (n_outer < 1 || params.omean < 1 || params.ostdev < 1 || us_outer < 1
				|| ue_outer < 2 || n_inner < 1 || params.imean < 1
				|| params.istdev < 1 || us_inner < 1 || ue_inner < 2) {
			fprintf(stderr, "ERROR: parameters incorrect!\n");
			exit(-1);
		}

		if (params.on_disk || params.on_mem_disk) {
			FILE_TYPE type = TYPE_DISK;
			if (params.on_mem_disk)
				type = TYPE_MEM;
			outer_rel = makeGaussRandDiskRel("orel.dat", sizeof(ttup) + 50,
					n_outer, params.omean, params.ostdev, us_outer, ue_outer,
					type);
			inner_rel = makeGaussRandDiskRel("irel.dat", sizeof(ttup) + 50,
					n_inner, params.imean, params.istdev, us_inner, ue_inner,
					type);
		} else {
			outer_rel = makeGaussRandRel(n_outer, params.omean, params.ostdev,
					us_outer, ue_outer);
			inner_rel = makeGaussRandRel(n_inner, params.imean, params.istdev,
					us_inner, ue_inner);
		}
	} else {
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

	if (params.on_disk || params.on_mem_disk) {

		ttup *tup;
		ExecHeapAmScanInit((HeapAm *) outer_rel);
		while ((tup = ExecHeapAmScan((HeapAm *) outer_rel)) != NULL) {
			n_outer++;
			l_outer = MAX(l_outer, tup->te - tup->ts + 1);
			us_outer = MIN(us_outer, tup->ts);
			ue_outer = MAX(ue_outer, tup->te);

		}
		ExecHeapAmScanInit((HeapAm *) inner_rel);
		while ((tup = ExecHeapAmScan((HeapAm *) inner_rel)) != NULL) {
			n_inner++;
			l_inner = MAX(l_inner, tup->te - tup->ts + 1);
			us_inner = MIN(us_inner, tup->ts);
			ue_inner = MAX(ue_inner, tup->te);
		}
	} else {
		ListCell *c;
		ttup *tup;
		foreach(c, (List *)outer_rel)
		{
			tup = (ttup *) lfirst(c);
			n_outer++;
			l_outer = MAX(l_outer, tup->te - tup->ts + 1);
			us_outer = MIN(us_outer, tup->ts);
			ue_outer = MAX(ue_outer, tup->te);
		}
		foreach(c, (List *)inner_rel)
		{
			tup = (ttup *) lfirst(c);
			n_inner++;
			l_inner = MAX(l_inner, tup->te - tup->ts + 1);
			us_inner = MIN(us_inner, tup->ts);
			ue_inner = MAX(ue_inner, tup->te);
		}
	}
	/* end evaluate dataset characteristics */

	if (us_outer < 1 || us_inner < 1) {
		fprintf(stderr, "ERROR: starting timestamps must be larger than 0!\n");
		fflush(stdout);
	}

	printf(
			"# 1:outer_n \t 2:outer_l \t 3:outer_u \t 4:inner_n \t 5:inner_l \t 6:inner_u \t 7:k_oip_outer \t 8:k_oip_inner \t 9:p_oip_outer \t 10:p_oip_inner \t 11:t_oip_create \t 12:t_oip_join \t 13:io_oip_join \t 14:oip_match \t 15:oip_fhits \t 16:oip_memusage \t 17:t_rit_create \t 18:t_rit_join \t 19:io_rit_join \t 20:rit_match \t 21:rit_fhits \t 22:rit_memusage \t 23:t_qtree_create \t 24:t_qtree_join \t 25:io_qtree_join \t 26:qtree_match \t 27:qtree_fhits \t 28:qtree_memusage \t 29:t_lqtree_create \t 30:t_lqtree_join \t 31:io_lqtree_join \t 32:lqtree_match \t 33:lqtree_fhits \t 34:p_lqt_outer \t 35:p_lqt_inner \t 36:lqtree_memusage \t 37:t_merge_create \t 38:t_merge_join \t 39:io_merge_join \t 40:merge_match \t 41:merge_fhits \t 42:merge_memusage  \t 43:t_segtree_create \t 44:t_segtree_join \t 45:io_segtree_join \t 46:segtree_match \t 47:segtree_fhits \t 48:segtree_memusage \n");

	printf(" %ld \t %ld \t %ld \t %ld \t %ld \t %ld \t", n_outer, l_outer,
			ue_outer - us_outer + 1, n_inner, l_inner, ue_inner - us_inner + 1);

	/* read File FRA */
	if (params.readFile && params.on_disk)
		run_readFile_Fra_DISK(params.ofn);
	else if (params.readFile) {
		fprintf(stderr, "ERROR: Read file only implemented on disk!");
		exit(-1);
	} else {
		printf(" x \t x \t x \t x \t x \t x \t");
	}

	if (params.eJoin && params.on_disk) {

		//run_EQUIJOIN_DISK((HeapAm *)outer_rel, params.ofn, params.ifn);

	} else if (params.eJoin) {
		fprintf(stderr, "ERROR: Equijoin only implemented on disk!");
		exit(-1);
	} else {
		printf(" x \t x \t x \t x \t x \t x \t");
	}

	/* Partitions not overlapping FRA */
	if (params.DIPMem) {

		run_DIP_Ordered_MM((List *) outer_rel, n_outer, us_outer, ue_outer,
				l_outer, (List *) inner_rel, n_inner, us_inner, ue_inner,
				l_inner, params.ofn, params.MergeMode, params.parallel, params.on_array);
					printf("1  \n");
	}

	if (params.DIPDisk) {
		printf("123");
		run_DIP_Ordered_Disk((HeapAm *) outer_rel, n_outer, us_outer, ue_outer,
				l_outer, (HeapAm *) inner_rel, n_inner, us_inner, ue_inner,
				l_inner, params.ofn, params.MergeMode, params.parallel, params.on_mem_disk);
				printf("2  \n");
	}

	printf("\n");

	if (params.on_disk || params.on_mem_disk) {
		freeDiskRel((HeapAm *) outer_rel);
		freeDiskRel((HeapAm *) inner_rel);
	} else {
		freeRel((List *) outer_rel);
		freeRel((List *) inner_rel);
	}
	printf(
			"# mallocs: %ld, frees: %ld (mem_sze: %ld, curr_size: %ld, peak_size: %ld)\n",
			mallocs, frees, mem_size, cur_size, peak_size);
	return 0;
}

static void initParams(struct params *params) {
	int i;
	int null = 0;
	for (i = 0; i < sizeof(struct params); i++)
		memcpy(((void* )params) + i, &null, 1);
}

static void getParams(int argc, char *argv[], struct params *params) {

	int c;
	static char short_options[] =
			"o:h:a:1:2:3:4:5:6:7:8:z:y:x:w:v:u:t:s:r:q:p:n:m:l:k:j:i";
	static struct option long_options[] = { { "opt", required_argument, NULL,
			'o' }, { "help", no_argument, NULL, 'h' }, { "algo",
	required_argument, NULL, 'a' }, { "on", required_argument, NULL, '1' }, {
			"ol", required_argument,
			NULL, '2' }, { "ous", required_argument, NULL, '3' }, { "oue",
	required_argument, NULL, '4' }, { "in",
	required_argument, NULL, '5' }, { "il", required_argument,
	NULL, '6' }, { "ius", required_argument, NULL, '7' }, { "iue",
	required_argument, NULL, '8' }, { "of",
	required_argument, NULL, 'z' }, { "if", required_argument,
	NULL, 'y' }, { "on-disk", no_argument, NULL, 'x' }, { "Join",
	no_argument, NULL, 'b' }, { "AntiJoin",
	no_argument, NULL, 'b2' }, { "Aggregation",
	no_argument, NULL, 'b3' }, { "tight", no_argument, NULL, 'w' }, { "omean",
	required_argument, NULL, 'v' }, { "ostddev", required_argument,
	NULL, 'u' }, { "imean",
	required_argument, NULL, 't' }, { "istddev",
	required_argument, NULL, 's' }, { "ok", required_argument,
	NULL, 'r' }, { "ik", required_argument, NULL, 'q' },

	{ "parallel", required_argument, NULL, 'p' }, { "density-based",
	required_argument, NULL, 'n' }, { "cpu_cost",
	required_argument, NULL, 'm' }, { "io_cost",
	required_argument, NULL, 'l' }, { "rit-clustered",
	no_argument, NULL, 'k' }, { "threads", required_argument,
	NULL, 'j' }, { "on-mem-disk", no_argument, NULL, 'i' }, { "on-array", no_argument, NULL, 'i2' },{ "ordered",
	no_argument, NULL, 'g' }, { NULL, 0, 0, 0 } };

	initParams(params);

	while ((c = getopt_long(argc, argv, short_options, long_options, NULL))
			!= -1) {

		switch (c) {
		//case parallel atoi
		case 'o':
			params->opt = atoi(optarg);
			break;
		case 'p':
			params->parallel = atoi(optarg);
			break;
		case 'h':
			printf("usage: %s -o num\n", argv[0]);
			printf("num:\n1 ... random\n2 ... from file\n");
			exit(0);
			break;
		case 'a':
			if (strcasecmp(optarg, "oip") == 0)
				params->oip = 1;
			else if (strcasecmp(optarg, "rit") == 0)
				params->rit = 1;
			else if (strcasecmp(optarg, "qt") == 0)
				params->qtree = 1;
			else if (strcasecmp(optarg, "lqt") == 0)
				params->lqtree = 1;
			else if (strcasecmp(optarg, "mjoin") == 0)
				params->mjoin = 1;

			else if (strcasecmp(optarg, "eJoin") == 0)
				params->eJoin = 1;
			else if (strcasecmp(optarg, "eJoinMM") == 0)
				params->eJoinMM = 1;
			else if (strcasecmp(optarg, "antiJoin") == 0)
				params->antiJoin = 1;
			/*else if (strcasecmp(optarg, "aggregation") == 0)
			 params->aggregation = 1;*/
			else if (strcasecmp(optarg, "normalization") == 0)
				params->normalization = 1;
			else if (strcasecmp(optarg, "intersection") == 0)
				params->intersection = 1;
			else if (strcasecmp(optarg, "DIPMEM") == 0)
				params->DIPMem = 1;
			else if (strcasecmp(optarg, "DIPDISK") == 0)
				params->DIPDisk = 1;
			else if (strcasecmp(optarg, "readFile") == 0)
				params->readFile = 1;
			else if (strcasecmp(optarg, "nestloop") == 0)
				params->nestloop = 1;
			else if (strcasecmp(optarg, "segtree") == 0)
				params->segtree = 1;
			else {
				fprintf(stderr, "ERROR: Unrecognized option for -a \n");
				exit(-1);
			}
			break;
		case '1':
			params->on = atol(optarg);
			break;
		case '2':
			params->ol = atol(optarg);
			break;
		case '3':
			params->ous = atol(optarg);
			break;
		case '4':
			params->oue = atol(optarg);
			break;

		case '5':
			params->in = atol(optarg);
			break;
		case '6':
			params->il = atol(optarg);
			break;
		case '7':
			params->ius = atol(optarg);
			break;
		case '8':
			params->iue = atol(optarg);
			break;
		case 'z':
			params->ofn = strdup(optarg);
			break;
		case 'y':
			params->ifn = strdup(optarg);
			break;

		case 'x':
			params->on_disk = 1;
			break;
		case 'w':
			params->tight = 1;
			break;
		case 'b':
			params->MergeMode = 1; //JOIN
			break;
		case 'b2':
			params->MergeMode = 2; //Antijoin
			break;
		case 'b3':
			params->MergeMode = 3; //Aggregation
			break;
		case 'v':
			params->omean = atol(optarg);
			break;
		case 'u':
			params->ostdev = atol(optarg);
			break;
		case 't':
			params->imean = atol(optarg);
			break;
		case 's':
			params->istdev = atol(optarg);
			break;
		case 'r':
			params->ok = atoi(optarg);
			break;
		case 'q':
			params->ik = atoi(optarg);
			break;
			/*case 'p':
			 params->k_idependent = 1;
			 break;*/
		case 'n':
			params->T = atoi(optarg);
			break;
		case 'm':
			params->cpu_io_cost = 1;
			cpu_cost = atof(optarg);
			break;
		case 'l':
			params->cpu_io_cost = 1;
			io_cost = atof(optarg);
			break;
		case 'k':
			params->rit_clustered = true;
			break;
		case 'j':
			params->threads = atoi(optarg);
			break;
		case 'i':
			params->on_mem_disk = 1;
			break;
		case 'i2':
			params->on_array = 1;
			break;
		case 'g':
			params->with_order = 1;
			break;
		case '?':
			printf("usage: %s -o num\n", argv[0]);
			printf("num:\n1 ... random\n2 ... from file\n");
			exit(0);
			break;
		}
	}
}

static int cmp_timestmp_timestmp(const void *a, const void *b) {
	int ret = (*(timestmp*) a > *(timestmp*) b)
			- (*(timestmp*) a < *(timestmp*) b);
	if (ret == 0)
		ret = (*(timestmp*) (a + sizeof(timestmp))
				< *(timestmp*) (b + sizeof(timestmp)))
				- (*(timestmp*) (a + sizeof(timestmp))
						> *(timestmp*) (b + sizeof(timestmp)));
	return ret;
}

static int cmp_timestmp_start(const void *a, const void *b) {
	return (*(timestmp*) a > *(timestmp*) b) - (*(timestmp*) a < *(timestmp*) b);
}

static int cmp_timestmp_end(const void *a, const void *b) {
	return (*(timestmp*) (a + sizeof(timestmp))
			> *(timestmp*) (b + sizeof(timestmp)))
			- (*(timestmp*) (a + sizeof(timestmp))
					< *(timestmp*) (b + sizeof(timestmp)));
}

static void run_Partition_Fra_DISK(HeapAm *orel, long on, timestmp ous,
		timestmp oue, timestmp ol, HeapAm *irel, long in, timestmp ius,
		timestmp iue, timestmp il, char *ofn) {
	size_t mem_usage;
	void *thandle;
	long long time;
	long match, fhits, ios;

	fprintf(stderr, "Deleting old partitions ... ");

	char buf[0x200];
	int numOuter = 1;
	snprintf(buf, 199, "%s.part%d.txt", ofn, numOuter);
	while (access(buf, F_OK) == 0) {
		fprintf(stderr, "Remove %s ... ", buf);
		remove(buf);
		snprintf(buf, 199, "%s.part%d.txt", ofn, ++numOuter);

	}

	fprintf(stderr, "Creating Files FRA ... ");
	fflush(stderr);
	mem_usage = cur_size;
	thandle = time_start();

	Sort *sort1 = ExecInitSort(orel, cmp_timestmp_start, orel->htupsze,
	WORK_MEM, orel->f->f->type);
	ttup *tup;

	mem_usage = cur_size - mem_usage;
	fprintf(stderr, " done.\n");
	fflush(stderr);

	printf(" %lld \t", time);

	fprintf(stderr, "Partitioning ... ");
	fflush(stderr);
	match = 0;
	fhits = 0;
	ios = 0;
	fprintf(stderr, "start ... ");

	/* JOIN */
	size_t nmemtups = orel->tups_per_page;
	List *tups, *partitions;
	ListCell *actualPartition;
	partition *part, *tempPart;
	partitions = NIL;
	int num_partitions = 0;
	int flag;
	tup = ExecSort(sort1);

	part = makePartition(1, orel, ofn);
	part->prev_last = ttupdup(tup, orel->htupsze); //QUICK FIX FOR THE FREETUP DONE LATER
	part->last = ttupdup(tup, orel->htupsze);

	tup = ExecSort(sort1);

	partitions = lappend(partitions, part);
	actualPartition = (list_head(partitions));
	while (tup != NULL) {
		//fprintf(stderr, "CURRENT TUP: [%ld,%ld] \n",tup->ts, tup->te);
		flag = 0;
		/* Find */
		if (tup->ts <= part->last->te && tup->te >= part->last->ts) // || intervalLen(part->last)>intervalLen(tup)) //OR SMALLER GRAN.
				{
			//fprintf(stderr, "[%ld,%ld] overlapps with [%ld,%ld]. REORDER PARITION %d \n",tup->ts, tup->te, part->last->ts, part->last->te, ((partition *)(actualPartition->ptr_value))->id);

			/* REORDER PARTITION BY SIZE	 */
			reorderParitions(actualPartition);

			actualPartition = list_head(partitions);

			/* VISIT EACH PARTITION AND TRY TO PUT TUP IN IT */
			while (actualPartition != NULL) {
				part = (partition *) (actualPartition->ptr_value);
				//	fprintf(stderr, "Visit partition %d(%d elements)  ", part->id, part->num_tuples);
				if (overlapFRA(part->last, tup)) {
					//			fprintf(stderr, "[%ld,%ld] overlaps with [%ld,%ld] \n",tup->ts, tup->te, part->last->ts, part->last->te);
					actualPartition = actualPartition->next;
				} else {
					//fprintf(stderr, "[%ld,%ld] into file %d (not smallest) \n",part->last->ts, part->last->te, part->id);
					//fprintf(stderr, "[%ld,%ld] into buffer (not smallest)\n",tup->ts, tup->te);
					/* write buffer */
					ExecHeapAmInsert(part->partitionStorage, part->last);
					freeTTup(part->prev_last);
					part->prev_last = part->last;
					part->last = ttupdup(tup, orel->htupsze);
					part->num_tuples++;
					break;
				}
			}
			if (actualPartition == NULL) {
				//fprintf(stderr, "CREATE NEW PARTITION: %d \n", partitions->length+1);
				part = makePartition(partitions->length + 1, orel, ofn);
				part->prev_last = ttupdup(tup, orel->htupsze); //QUICK FIX FOR THE FREETUP DONE LATER
				part->last = ttupdup(tup, orel->htupsze);
				new_head_cell(partitions);
				partitions->head->ptr_value = part;
				actualPartition = list_head(partitions);
			}
		} else {
			//	fprintf(stderr, "[%ld,%ld] into file %d \n",part->last->ts, part->last->te, part->id);
			//	fprintf(stderr, "[%ld,%ld] into buffer \n",tup->ts, tup->te);

			//			/* write buffer */
			ExecHeapAmInsert(part->partitionStorage, part->last);
			freeTTup(part->prev_last);
			part->prev_last = part->last;
			part->last = ttupdup(tup, orel->htupsze);
			part->num_tuples++;
		}
		//freeTTup(tup);
		tup = ExecSort(sort1);
	}

	time = time_end(thandle);
	fprintf(stderr, "done.\n");
	fprintf(stderr, "num. of partitions %d \n \n \n ", (partitions->length));

	actualPartition = list_head(partitions);
	while (actualPartition != NULL) {
		fprintf(stderr, "PARTITION ID=%d, LEN=%d \n",
				((partition *) (actualPartition->ptr_value))->id,
				++((partition *) (actualPartition->ptr_value))->num_tuples);
		ExecHeapAmInsert(
				((partition *) (actualPartition->ptr_value))->partitionStorage,
				((partition *) (actualPartition->ptr_value))->last);

		ExecEndHeapAM(
				((partition *) (actualPartition->ptr_value))->partitionStorage);
		buffer_close(
				((partition *) (actualPartition->ptr_value))->partitionBuffer);
		file_close(((partition *) (actualPartition->ptr_value))->partitionFile);

		actualPartition = actualPartition->next;
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

static void run_DIP_Ordered_MM(List *orel, long on, timestmp ous, timestmp oue,
		timestmp ol, List *irel, long in, timestmp ius, timestmp iue,
		timestmp il, char *ofn, int mergemode, int parallel, int arr) {

	void *thandle;
	long long timedip;
	long long timejoin;
	long long timearray=0;

	thandle = time_start();
	List *innerpartition;
	// create outer DIP
	partition_folder++;
	List *outerpartition = run_Partitioning_Ordered_MM(orel, on, ous, oue, ol);
	partition_folder++;
	if (mergemode != 2 && mergemode != 3)
		innerpartition = run_Partitioning_Ordered_MM(irel, in, ius, iue, il);

	timedip = time_end(thandle);
	List *innerrel = innerpartition;
	List* outerrel= outerpartition;
	List* ot=outerpartition;
	List* it=innerpartition;
	long long count_join = 0;
	int i = 0;
	int counterr = 0;
	if(parallel==0)
		parallel=1;
	parallel = MIN(parallel, num_outer_partitions);
	if(parallel==0)
		parallel=1;
	ttup** yarray = (ttup*) malloc(sizeof(ttup) * parallel);
	for (int i = 0; i < parallel; i++) {
		yarray[i] = (ttup*) malloc(sizeof(ttup));
	}
	ttup* x = (ttup*) malloc(sizeof(ttup));
	ttup* y = (ttup*) malloc(sizeof(ttup));


	if(arr==1){ //**********************on_array***************************
		thandle = time_start();
		if(mergemode ==1){

	ArrayList* firstopartition=(ArrayList*)malloc(sizeof(ArrayList));
	ArrayList* tmpo = firstopartition;
	ArrayList* outerfirst=firstopartition;
	firstopartition->partition=malloc((2*sizeof(timestmp)+50)*(ot->counter+20000));
	firstopartition->numberoftup=ot->counter;
	firstopartition->position=0;
	ListCell* ocell= ot->head;
	int e=0;
	while(ocell!=NULL){
		firstopartition->partition[e].ts=((ttup*)lfirst(ocell))->ts;
		firstopartition->partition[e].te=((ttup*)lfirst(ocell))->te;
		firstopartition->partition[e].data=((ttup*)lfirst(ocell))->data;
		e++;
		ocell=ocell->next;
	}
	firstopartition->partition[e].ts=NULL;
			firstopartition->partition[e].te=NULL;
			firstopartition->partition[e].data=NULL;
	ot=ot->next;
	while(ot!=NULL){
		ArrayList* nextpartition=(ArrayList*)malloc(sizeof(ArrayList));
		nextpartition->partition=malloc((2*sizeof(timestmp)+50)*(ot->counter+20000));
		nextpartition->numberoftup=ot->counter;
		nextpartition->position=0;
		ocell= ot->head;
		e=0;
		while(ocell!=NULL){
			nextpartition->partition[e].ts=((ttup*)lfirst(ocell))->ts;
			nextpartition->partition[e].te=((ttup*)lfirst(ocell))->te;
			nextpartition->partition[e].data=((ttup*)lfirst(ocell))->data;
			e++;
			ocell=ocell->next;
		}
		nextpartition->partition[e].ts=NULL;
				nextpartition->partition[e].te=NULL;
				nextpartition->partition[e].data=NULL;
		firstopartition->next=nextpartition;
		firstopartition=nextpartition;
		ot=ot->next;

	}
	firstopartition->next= NULL;

	ArrayList* firstipartition=malloc(sizeof(ArrayList));
	ArrayList* tmpi = firstipartition;

firstipartition->partition=malloc((2*sizeof(timestmp)+50)*(it->counter+20000));
	firstipartition->numberoftup=it->counter;
	firstipartition->position=0;
	ListCell* icell= it->head;
	e=0;
	while(icell!=NULL){
		firstipartition->partition[e].ts=((ttup*)lfirst(icell))->ts;
		firstipartition->partition[e].te=((ttup*)lfirst(icell))->te;
		firstipartition->partition[e].data=((ttup*)lfirst(icell))->data;
		e++;
		icell=icell->next;
	}
	firstipartition->partition[e].ts=NULL;
			firstipartition->partition[e].te=NULL;

	it=it->next;
	while(it!=NULL){
		ArrayList* nextpartition=malloc(sizeof(ArrayList));
		nextpartition->partition=malloc((2*sizeof(timestmp)+50)*(it->counter+20000));
		nextpartition->numberoftup=it->counter;
		nextpartition->position=0;
		icell= it->head;
		e=0;
		while(icell!=NULL){
			nextpartition->partition[e].ts=((ttup*)lfirst(icell))->ts;
			nextpartition->partition[e].te=((ttup*)lfirst(icell))->te;
			nextpartition->partition[e].data=((ttup*)lfirst(icell))->data;
			e++;
			icell=icell->next;
		}
		nextpartition->partition[e].ts=NULL;
					nextpartition->partition[e].te=NULL;
		firstipartition->next=nextpartition;
		firstipartition=nextpartition;

		it=it->next;

	}
	firstipartition->next=NULL;
	timearray = time_end(thandle);
	thandle = time_start();
	firstopartition=tmpo;
	firstipartition=tmpi;

	while(firstopartition!=NULL){
		while(firstipartition!=NULL){
			count_join=count_join+MergeArray(firstopartition,firstipartition,mergemode,parallel,x,yarray);
			firstipartition=firstipartition->next;
		}
		int temp=0;

		while(temp<parallel&& firstopartition!=NULL){
			firstopartition=firstopartition->next;
			temp++;
		}

		firstipartition=tmpi;
	}
	timejoin = time_end(thandle);
	}if(mergemode ==2){ //*************on_array antijoin***************
		thandle = time_start();
		list_sort(irel, cmp_timestmp_timestmp);
		ArrayList* fullirel= (ArrayList*)malloc(sizeof(ArrayList));
		ArrayList* ireltmp = fullirel;
		fullirel->partition=malloc((2*sizeof(timestmp)+50)*(irel->counter+20000));
		fullirel->numberoftup=irel->counter;
		ListCell *irelcell=irel->head;
		int e=0;
		while(irelcell!=NULL){
			fullirel->partition[e].ts=((ttup*)lfirst(irelcell))->ts;
			fullirel->partition[e].te=((ttup*)lfirst(irelcell))->te;
			fullirel->partition[e].data=((ttup*)lfirst(irelcell))->data;
			e++;
			irelcell=irelcell->next;
		}
		ArrayList* firstopartition=(ArrayList*)malloc(sizeof(ArrayList));
			ArrayList* tmpo = firstopartition;

			firstopartition->partition=malloc((2*sizeof(timestmp)+50)*(ot->counter+20000));
			firstopartition->numberoftup=ot->counter;
			firstopartition->position=0;
			ListCell* ocell= ot->head;
			 e=0;
			while(ocell!=NULL){
				firstopartition->partition[e].ts=((ttup*)lfirst(ocell))->ts;
				firstopartition->partition[e].te=((ttup*)lfirst(ocell))->te;
				firstopartition->partition[e].data=((ttup*)lfirst(ocell))->data;
				e++;
				ocell=ocell->next;
			}
			firstopartition->partition[e].ts=NULL;
					firstopartition->partition[e].te=NULL;
					firstopartition->partition[e].data=NULL;
			ot=ot->next;
			while(ot!=NULL){
				ArrayList* nextpartition=(ArrayList*)malloc(sizeof(ArrayList));
				nextpartition->partition=malloc((2*sizeof(timestmp)+50)*(ot->counter+20000));
				nextpartition->numberoftup=ot->counter;
				nextpartition->position=0;
				ocell= ot->head;
				e=0;
				while(ocell!=NULL){
					nextpartition->partition[e].ts=((ttup*)lfirst(ocell))->ts;
					nextpartition->partition[e].te=((ttup*)lfirst(ocell))->te;
					nextpartition->partition[e].data=((ttup*)lfirst(ocell))->data;
					e++;
					ocell=ocell->next;
				}
				nextpartition->partition[e].ts=NULL;
						nextpartition->partition[e].te=NULL;
						nextpartition->partition[e].data=NULL;
				firstopartition->next=nextpartition;
				firstopartition=nextpartition;
				ot=ot->next;

			}
			firstopartition->next= NULL;
			firstopartition=tmpo;
			while (firstopartition != NULL) {
				count_join = count_join
						+ MergeArray(firstopartition, fullirel, mergemode,
								parallel, x, yarray);

				int temp = 0;

				while (temp < parallel && firstopartition != NULL) {
					firstopartition = firstopartition->next;
					temp++;
				}

			}
			timejoin = time_end(thandle);
	}
	}
	if(arr!=1){
		thandle = time_start();
	if (mergemode == 1) {

		false_hits = 0;
		while (outerpartition != NULL) {

			while (innerpartition != NULL) {

					count_join = count_join
							+ MergeParallel(outerpartition, innerpartition,
									mergemode, parallel, x, yarray);



					innerpartition = innerpartition->next;

				}

			int temp = 0;
							while (temp < parallel && outerpartition != NULL) {
								outerpartition = outerpartition->next;
								temp++;

							}
			innerpartition = innerrel;
			}



		}

	if (mergemode == 2) {
		false_hits = 0;
		list_sort(irel, cmp_timestmp_timestmp);
			while (outerpartition != NULL) {
				count_join = count_join
						+ MergeParallel(outerpartition, irel, mergemode,
								parallel, x, yarray);

				int temp = 0;

				while (temp < parallel && outerpartition != NULL) {
					outerpartition = outerpartition->next;
					temp++;
				}

			}


	}
	if (mergemode == 3) {
		false_hits = 0;

		ylist = outerpartition;
		outerpartition = outerpartition->next;
		while (outerpartition != NULL) {
			count_join = 0;
			count_join = count_join
					+ DipmergeMem(ylist, outerpartition, mergemode, x, y);
			outerpartition = outerpartition->next;

		}

	}
	timejoin = time_end(thandle);
	}

	fprintf(stderr, "JOIN MATCHES: %lld \n", count_join);
	fprintf(stderr, "UNPRODUCTIVE MATCHES: %lld \n", false_hits);
	fprintf(stderr, "TIME JOIN: %lld \n", timejoin);
	fprintf(stderr, "TIME PARTITIONING: %lld \n", timedip);
	fprintf(stderr, "ARRAY CREATION TIME %lld \n",timearray);
	fprintf(stderr, "NUM OUTER PARTITIONS: %d \n", num_outer_partitions);
	fprintf(stderr, "NUM INNER PARTITIONS: %d \n", num_inner_partitions);

}
long long MergeArray(ArrayList* orel, ArrayList* irel, int mergemode, int para,
		ttup* x, ttup** yarray) {

	counter = 0;

	long long par = para;
	ArrayList* outerarray[par];

	long long test = 0;
	for (int i = 0; i < par; i++) {
		if (orel != NULL) {

			outerarray[i] = orel;
			outerarray[i]->position=0;
			test++;
		}
		orel = orel->next;
		if (orel == NULL) {
			break;
		}

	}



	int icounter=0;
	int internal = 0;
	long long intervalstart = 0;
	long long intervalend = 0;




	x->ts = -2147483648;

	x->te = irel->partition[icounter].ts;

	for (int i = 0; i < par; i++) {

		yarray[i]->ts = -2147483648;
		yarray[i]->te = outerarray[i]->partition[outerarray[i]->position].ts;

	}

	while (outerarray[internal]->partition[outerarray[internal]->position].ts != NULL || (irel->partition[icounter].ts != NULL)) {


		if (mergemode == 2) {
			if (((x->te) - (x->ts)) > 0 && outerarray[internal]->partition[outerarray[internal]->position].ts  != NULL
					&& overlapFRAtup(outerarray[internal]->partition[outerarray[internal]->position], x)) {
				intervalend = MIN(outerarray[internal]->partition[outerarray[internal]->position].te, x->te);
				intervalstart = MAX(outerarray[internal]->partition[outerarray[internal]->position].ts, x->ts);
				counter++;

			} else {
				false_hits++;
			}
		}


		if (mergemode == 1) {
			/*if(outerarray[internal]->partition[outerarray[internal]->position].ts!=NULL)
			fprintf(stderr,"r[%d] %d %d ",internal, outerarray[internal]->partition[outerarray[internal]->position].ts, outerarray[internal]->partition[outerarray[internal]->position].te);
			else fprintf(stderr,"r is null");
			if(irel->partition[icounter].ts !=NULL){
				fprintf(stderr,"s %d %d \n",irel->partition[icounter].ts, irel->partition[icounter].te );

			}else fprintf(stderr,"s is null\n");*/
			if (outerarray[internal]->partition[outerarray[internal]->position].ts != NULL && irel->partition[icounter].ts != NULL
					&& overlapFRAtuptup(outerarray[internal]->partition[outerarray[internal]->position], irel->partition[icounter])) {


				intervalend = MIN(outerarray[internal]->partition[outerarray[internal]->position].te, irel->partition[icounter].te);
				intervalstart = MAX(outerarray[internal]->partition[outerarray[internal]->position].ts, irel->partition[icounter].ts);
				counter++;
			} else {
				false_hits++;
			}
		}


		if (outerarray[internal]->partition[outerarray[internal]->position].ts != NULL
				&& (irel->partition[icounter].ts == NULL || (outerarray[internal]->partition[outerarray[internal]->position].te < irel->partition[icounter].te))) {

			if (outerarray[internal]->partition[outerarray[internal]->position].te >= yarray[internal]->ts) {
				yarray[internal]->ts = outerarray[internal]->partition[outerarray[internal]->position].te;
			}


			outerarray[internal]->position++;



			if (outerarray[internal]->partition[outerarray[internal]->position].ts != NULL) {
				yarray[internal]->te = outerarray[internal]->partition[outerarray[internal]->position].ts;
			} else {

				yarray[internal]->te = MAXLONG;

			}

		} else {


			if (internal < par - 1) {
				internal++;

			} else {


				if (irel->partition[icounter].te >= x->ts) {
					x->ts =irel->partition[icounter].te;

				}
				icounter++;




				internal = 0;
				if (irel->partition[icounter].ts != NULL) {
					x->te = irel->partition[icounter].ts;
				} else {

					x->te = MAXLONG;

				}


				}

		}

	}

	return counter;
}
static void run_DIP_Ordered_Disk(HeapAm *orel, long on, timestmp ous,
		timestmp oue, timestmp ol, HeapAm *irel, long in, timestmp ius,
		timestmp iue, timestmp il, char *ofn, int mergemode, int parallel, int memdisk) {
	void *thandle;
	long long timejoin;
	long long timedip;
	long match, fhits;

	long n_outer = on;
	long n_inner = in;
	Filenode *outer_partition_tups;
	Filenode *inner_partition_tups;

	char buf[0x200];
	int numOuter = 1;
	int folder = 1;
	snprintf(buf, 199, "Partitions%d/Partition%d.txt", folder, numOuter);

	while (access(buf, F_OK) == 0) {
		remove(buf);
		snprintf(buf, 199, "Partitions%d/Partition%d.txt", folder, ++numOuter);

	}

	folder = 2;
	numOuter = 1;
	snprintf(buf, 199, "Partitions%d/Partition%d.txt", folder, numOuter);
	while (access(buf, F_OK) == 0) {
		remove(buf);
		snprintf(buf, 199, "Partitions%d/Partition%d.txt", folder, ++numOuter);

	}

	while (access(buf, F_OK) == 0) {
		remove(buf);
		snprintf(buf, 199, "mergejoin_inner_storage.dat");
	}
	while (access(buf, F_OK) == 0) {
		remove(buf);
		snprintf(buf, 199, "sortedfile.dat");
	}
	thandle = time_start();
	partition_folder++;
	outer_partition_tups = run_Partitioning_Ordered_DISK(orel, on, ous, oue,
			ol,memdisk);

	partition_folder++;
	if (mergemode != 2 && mergemode != 3)
		inner_partition_tups = run_Partitioning_Ordered_DISK(irel, in, ius, iue,
				il,memdisk);
	timedip = time_end(thandle);

	fprintf(stderr, "Join... \n");

	Filenode* outerrel = outer_partition_tups;
	Filenode* innerrel = inner_partition_tups;

	long long count_join = 0;
	File *outerFile;
	File *innerFile;
	Buffer *oStorageBuffer;
	Buffer *iStorageBuffer;
	HeapAm *iStorage;
	HeapAm *oStorage;
	ttup* x = (ttup*) malloc(sizeof(ttup));
	int max = 0;
	ttup* tup;
	File *fullFile;
	Buffer *fullBuffer;
	HeapAm *fullStorage;
	Sort *sort2;
	File *sortedfile;
	Buffer *sortedBuffer;
	HeapAm *sortedStorage;
	File* readFile;
	Buffer* readBuffer;
	HeapAm* readHeap;
	parallel = MIN(parallel, num_outer_partitions);



	//************disk_mem parallel***********************
if(memdisk != 0){
	thandle = time_start();
	if(mergemode == 1){

		if(parallel ==NULL)
			parallel =1;
		ttup** rarray = (ttup*) malloc(sizeof(ttup) * parallel);
		ttup** yarray = (ttup*) malloc(sizeof(ttup) * parallel);
		for(int i=0; i< parallel;i++){
			yarray[i]=(ttup*) malloc(sizeof(ttup));
			rarray[i]=(ttup*) malloc(sizeof(ttup));

		}

		ttup* x = (ttup*) malloc(sizeof(ttup));
		HeapAm* heaps[parallel];
		false_hits = 0;
		while (outer_partition_tups != NULL) {
			max = 0;
			for (int i = 0; i < parallel; i++) {
				heaps[i] = outer_partition_tups->h;
				outer_partition_tups = outer_partition_tups->next;
				max++;
				if (outer_partition_tups == NULL) {
					break;
				}
			}

			if (max < parallel)
				parallel = max;
			while (inner_partition_tups != NULL) {


				iStorage =inner_partition_tups->h;

				count_join = count_join
						+ MergeDiskParallel(heaps, iStorage, mergemode,
								parallel, x, yarray,rarray);

				fflush(stderr);


				inner_partition_tups = inner_partition_tups->next;

			}

			inner_partition_tups = innerrel;

		}
	}

	timejoin = time_end(thandle);

}else{
	//************disk parallel***********************

	if (mergemode == 1) {
		thandle = time_start();
		ttup** rarray = (ttup*) malloc(sizeof(ttup) * parallel);
		if (parallel == NULL)
			parallel = 1;
		ttup** yarray = (ttup*) malloc(sizeof(ttup) * parallel);
		for (int i = 0; i < parallel; i++) {
			yarray[i] = (ttup*) malloc(sizeof(ttup));
		}
		ttup* x = (ttup*) malloc(sizeof(ttup));
		File* files[parallel];
		Buffer* buffers[parallel];
		HeapAm* heaps[parallel];
		false_hits = 0;

		while (outer_partition_tups != NULL) {
			max = 0;
			for (int i = 0; i < parallel; i++) {
				files[i] = file_open(outer_partition_tups->name);
				buffers[i] = buffer_create(files[i], 1000, POLICY_LRU);
				heaps[i] = ExecInitHeapAm(buffers[i], sizeof(ttup) + 50);
				outer_partition_tups = outer_partition_tups->next;
				max++;
				if (outer_partition_tups == NULL) {
					break;
				}
			}

			if (max < parallel)
				parallel = max;
			while (inner_partition_tups != NULL) {

				innerFile = file_open(inner_partition_tups->name);

				iStorageBuffer = buffer_create(innerFile, WORK_MEM, POLICY_LRU);

				iStorage = ExecInitHeapAm(iStorageBuffer, sizeof(ttup) + 50);

				count_join = count_join
						+ MergeDiskParallel(heaps, iStorage, mergemode,
								parallel, x, yarray, rarray);

				ExecEndHeapAM(iStorage);
				buffer_close(iStorageBuffer);
				file_close(innerFile);

				inner_partition_tups = inner_partition_tups->next;

			}
			for (int i = 0; i < parallel; i++) {
				ExecEndHeapAM(heaps[i]);
				buffer_close(buffers[i]);
				file_close(files[i]);
			}
			inner_partition_tups = innerrel;

		}
		timejoin = time_end(thandle);
	}
	if (mergemode == 2) {

		ttup** rarray = (ttup*) malloc(sizeof(ttup) * parallel);
		false_hits = 0;
		fullFile = file_create("mergejoin_inner_storage.dat", orel->f->f->type);
		fullBuffer = buffer_create(fullFile, WORK_MEM, POLICY_LRU);
		fullStorage = ExecInitHeapAm(fullBuffer, orel->htupsze);
		sort2 = ExecInitSort(irel, cmp_timestmp_start, irel->htupsze,
		WORK_MEM, irel->f->f->type);
		tup = ExecSort(sort2);
		sortedfile = file_create("sortedfile.dat", irel->f->f->type);
		sortedBuffer = buffer_create(sortedfile, WORK_MEM, POLICY_LRU);
		sortedStorage = ExecInitHeapAm(sortedBuffer, sizeof(ttup) + 50);

		while (tup != NULL) {
			ExecHeapAmInsert(sortedStorage, tup);
			tup = ExecSort(sort2);
		}
		ExecEndHeapAM(sortedStorage);
		buffer_close(sortedBuffer);
		file_close(sortedfile);
		sortedfile = file_open("sortedfile.dat");
		sortedBuffer = buffer_create(sortedfile, WORK_MEM, POLICY_LRU);
		sortedStorage = ExecInitHeapAm(sortedBuffer, sizeof(ttup) + 50);
		ExecHeapAmScanInit(sortedStorage);

		if (parallel == NULL)
			parallel = 1;
		ttup** yarray = (ttup*) malloc(sizeof(ttup) * parallel);
		for (int i = 0; i < parallel; i++) {
			yarray[i] = (ttup*) malloc(sizeof(ttup));
		}
		ttup* x = (ttup*) malloc(sizeof(ttup));
		File* files[parallel];
		Buffer* buffers[parallel];
		HeapAm* heaps[parallel];
		false_hits = 0;
		thandle = time_start();
		while (outer_partition_tups != NULL) {
			max = 0;
			for (int i = 0; i < parallel; i++) {
				files[i] = file_open(outer_partition_tups->name);
				buffers[i] = buffer_create(files[i], 1000, POLICY_LRU);
				heaps[i] = ExecInitHeapAm(buffers[i], sizeof(ttup) + 50);
				outer_partition_tups = outer_partition_tups->next;
				max++;
				if (outer_partition_tups == NULL) {

					break;
				}
			}

			if (max < parallel)
				parallel = max;
			count_join = count_join
					+ MergeDiskParallel(heaps, sortedStorage, mergemode,
							parallel, x, yarray,rarray);

			for (int i = 0; i < parallel; i++) {
				ExecEndHeapAM(heaps[i]);
				buffer_close(buffers[i]);
				file_close(files[i]);
			}

		}
		timejoin = time_end(thandle);
		ExecEndHeapAM(sortedStorage);
		buffer_close(sortedBuffer);
		file_close(sortedfile);
	}
	if (mergemode == 3) {
		thandle = time_start();
		ttup* y = (ttup*) malloc(sizeof(ttup));
		false_hits = 0;
		outerFile = file_open(outer_partition_tups->name);
		oStorageBuffer = buffer_create(outerFile, WORK_MEM, POLICY_LRU);
		oStorage = ExecInitHeapAm(oStorageBuffer, sizeof(ttup) + 50);
		ExecHeapAmScanInit(oStorage);
		outer_partition_tups = outer_partition_tups->next;
		innerFile = file_open(outer_partition_tups->name);
		iStorageBuffer = buffer_create(innerFile, WORK_MEM, POLICY_LRU);
		iStorage = ExecInitHeapAm(iStorageBuffer, sizeof(ttup) + 50);
		ExecHeapAmScanInit(iStorage);

		count_join = count_join + DIPmerge(oStorage, iStorage, mergemode, x, y);
		ExecEndHeapAM(oStorage);
		buffer_close(oStorageBuffer);
		file_close(outerFile);
		ExecEndHeapAM(iStorage);
		buffer_close(iStorageBuffer);
		file_close(innerFile);
		outer_partition_tups = outer_partition_tups->next;
		while (outer_partition_tups != NULL) {
			char buff[0x200];
			snprintf(buff, 199, "writefile%d.txt", helper - 1);
			outerFile = file_open(outer_partition_tups->name);

			oStorageBuffer = buffer_create(outerFile, WORK_MEM, POLICY_LRU);
			oStorage = ExecInitHeapAm(oStorageBuffer, sizeof(ttup) + 50);
			ExecHeapAmScanInit(oStorage);

			readFile = file_open(buff);

			readBuffer = buffer_create(readFile, WORK_MEM, POLICY_LRU);
			readHeap = ExecInitHeapAm(readBuffer, sizeof(ttup) + 50);
			ExecHeapAmScanInit(readHeap);

			count_join = 0;

			count_join = count_join
					+ DIPmerge(readHeap, oStorage, mergemode, x, y);
			ExecEndHeapAM(oStorage);
			buffer_close(oStorageBuffer);
			file_close(outerFile);
			ExecEndHeapAM(readHeap);
			buffer_close(readBuffer);
			file_close(readFile);
			outer_partition_tups = outer_partition_tups->next;

		}
		timejoin = time_end(thandle);
	}

}



	fprintf(stderr, "JOIN MATCHES: %lld \n", count_join);
	fprintf(stderr, "UNPRODUCTIVE MATCHES: %lld \n", false_hits);
	fprintf(stderr, "TIME JOIN: %lld \n", timejoin);
	fprintf(stderr, "TIME PARTITIONING: %lld \n", timedip);
	fprintf(stderr, "NUM OUTER PARTITIONS: %d \n", num_outer_partitions);
	fprintf(stderr, "NUM INNER PARTITIONS: %d \n", num_inner_partitions);

}
long long MergeDiskParallel(HeapAm** heaps, HeapAm* inner, int mergemode,
		int para, ttup* x, ttup** yarray, ttup** rarray) {

	counter = 0;
	//ttup* rarray[para];
	int par = para;
	int test = 0;

	for (int i = 0; i < par; i++) {

		if (heaps[i] != NULL) {

			ExecHeapAmScanInit(heaps[i]);

			rarray[i] = ExecHeapAmScan(heaps[i]);

			fflush(stderr);

			test++;

		}else{
			break;}

	}
	if(test < par){
		par = test;
	}

	ExecHeapAmScanInit(inner);
	ttup *s = ExecHeapAmScan(inner);

	int internal = 0;
	long long intervalstart = 0;
	long long intervalend = 0;

	for (int i = 0; i < par; i++) {


		yarray[i]->ts = -2147483648;

		yarray[i]->te = rarray[i]->ts;
	}


	x->ts = -2147483648;
	x->te = s->ts;
	while (rarray[internal] != NULL || (s != NULL)) {

		if (mergemode == 2) {
			if (((x->te) - (x->ts)) > 0 && rarray[internal] != NULL
					&& overlapFRA(rarray[internal], x)) {
				intervalend = MIN(rarray[internal]->te, x->te);
				intervalstart = MAX(rarray[internal]->ts, x->ts);
				counter++;

			} else {
				false_hits++;
			}
		}

		if (mergemode == 1) {
			if (rarray[internal] != NULL && s != NULL
					&& overlapFRA(rarray[internal], s)) {

				intervalend = MIN(rarray[internal]->te, s->te);
				intervalstart = MAX(rarray[internal]->ts, s->ts);
				counter++;
			} else {
				false_hits++;
			}
		}

		if (rarray[internal] != NULL
				&& (s == NULL || (rarray[internal]->te < s->te))) {

			if (rarray[internal]->te >= yarray[internal]->ts) {
				yarray[internal]->ts = rarray[internal]->te;
			}
			rarray[internal] = ExecHeapAmScan(heaps[internal]);

			if (rarray[internal] != NULL) {
				yarray[internal]->te = rarray[internal]->ts;
			} else {

				yarray[internal]->te = MAXLONG;
			}

		} else {

			if (internal < par - 1) {
				internal++;
			} else {
				if (s->te >= x->ts) {
					x->ts = s->te;

				}

				s = ExecHeapAmScan(inner);
				internal = 0;
				if (s != NULL) {
					x->te = s->ts;
				} else {

					x->te = MAXLONG;

				}

			}

		}

	}
	return counter;
}
long long MergeParallel(List* orel, List* irel, int mergemode, int para,
		ttup* x, ttup** yarray) {

	counter = 0;


	ListCell *innercell = irel->head;
	long long par = para;
	ListCell* outerarray[par];
	ttup* s;
	long long test = 0;
	for (int i = 0; i < par; i++) {

		if (orel != NULL) {
			outerarray[i] = orel->head;
			test++;
		}
		orel = orel->next;
		if (orel == NULL) {
			break;
		}

	}

	if (test < par) {

		par = test;
	}

	ttup* rarray[par];
	for (long long i = 0; i < par; i++) {
		rarray[i] = lfirst(outerarray[i]);
	}

	int internal = 0;
	long long intervalstart = 0;
	long long intervalend = 0;
	if (innercell != NULL)
		s = lfirst(innercell);

	x->ts = -2147483648;
	x->te = s->ts;

	for (int i = 0; i < par; i++) {
		yarray[i]->ts = -2147483648;
		yarray[i]->te = rarray[i]->ts;
	}
	while (rarray[internal] != NULL || (s != NULL)) {


		if (mergemode == 2) {

			if (((x->te) - (x->ts)) > 0 && rarray[internal] != NULL
					&& overlapFRA(rarray[internal], x)) {
				intervalend = MIN(rarray[internal]->te, x->te);
				intervalstart = MAX(rarray[internal]->ts, x->ts);
				counter++;

			} else {
				false_hits++;
			}
		}


		if (mergemode == 1) {


			if (rarray[internal] != NULL && s != NULL
					&& overlapFRA(rarray[internal], s)) {


				intervalend = MIN(rarray[internal]->te, s->te);
				intervalstart = MAX(rarray[internal]->ts, s->ts);
				counter++;
			} else {
				false_hits++;
			}
		}


		if (rarray[internal] != NULL
				&& (s == NULL || (rarray[internal]->te < s->te))) {

			if (rarray[internal]->te >= yarray[internal]->ts) {
				yarray[internal]->ts = rarray[internal]->te;
			}


			outerarray[internal]=outerarray[internal]->next;
			if(outerarray[internal]!=NULL){
			rarray[internal] = (ttup*)lfirst(outerarray[internal]);
			}
			else{
				rarray[internal]=NULL;


			}

			if (rarray[internal] != NULL) {
				yarray[internal]->te = rarray[internal]->ts;
			} else {

				yarray[internal]->te = MAXLONG;
			}

		} else {


			if (internal < par - 1) {
				internal++;
			} else {


				if (s->te >= x->ts) {
					x->ts = s->te;

				}
				innercell=innercell->next;
				if (innercell != NULL){
					s = (ttup*) lfirst(innercell);
				}else s=NULL;

				internal = 0;
				if (s != NULL) {
					x->te = s->ts;
				} else {

					x->te = MAXLONG;

				}


				}

		}

	}

	return counter;
}

long long DipmergeMem(List* orel, List* irel, int mergemode, ttup* x, ttup* y) {

	counter = 0;
	ListCell *innercell = irel->head;
	ListCell *outercell = orel->head;
	ttup* r;
	ttup* s;

	long long intervalstart = 0;
	long long intervalend = 0;
	if (outercell != NULL)
		r = lfirst(outercell);
	if (innercell != NULL)
		s = lfirst(innercell);

	x->ts = -2147483648;
	x->te = s->ts;

	y->ts = -2147483648;
	y->te = r->ts;

	while (r != NULL || (s != NULL && mergemode != 2)) {

		if (mergemode == 3) {

			if (((y->te) - (y->ts)) > 0 && s != NULL && overlapFRA(s, y)) {

				intervalend = MIN(s->te, y->te);
				intervalstart = MAX(s->ts, y->ts);
				if (mergemode == 3) {

					counter++;

					ttup* tup1 = ttupdup(s, sizeof(ttup*) + 50);
					tup1->ts = intervalstart;
					tup1->te = intervalend;

					tup1->data = (char*) malloc(sizeof(char) * 50);
					tup1->data = strcpy(tup1->data, "null+");
					tup1->data = strcat(tup1->data, s->data);

					if (z == NIL) {
						z = lappend(NIL, tup1);
					} else
						z = lappend(z, tup1);

				}

			} else {
				false_hits++;
			}

		}
		if (mergemode == 2 || mergemode == 3) {
			if (((x->te) - (x->ts)) > 0 && r != NULL && overlapFRA(r, x)) {

				intervalstart = MAX(r->ts, x->ts);
				intervalend = MIN(r->te, x->te);
				if (mergemode == 2) {
					counter++;
				}
				if (mergemode == 3) {

					counter++;
					ttup*tup2 = ttupdup(r, sizeof(ttup*) + 50);

					tup2->ts = intervalstart;
					tup2->te = intervalend;

					tup2->data = (char*) malloc(sizeof(char) * 50);
					tup2->data = strcpy(tup2->data, r->data);
					tup2->data = strcat(tup2->data, "+null");

					if (z == NIL) {
						z = lappend(NIL, tup2);
					} else
						z = lappend(z, tup2);

				}

			} else {
				false_hits++;
			}

		}
		if (mergemode == 1 || mergemode == 3) {
			if (r != NULL && s != NULL && overlapFRA(r, s)) {

				intervalend = MIN(r->te, s->te);
				intervalstart = MAX(r->ts, s->ts);
				if (mergemode == 1) {
					counter++;
				}
				if (mergemode == 3) {

					ttup*tup = ttupdup(s, sizeof(ttup*) + 50);

					tup->ts = intervalstart;
					tup->te = intervalend;
					if (intervalstart < intervalend) {
						counter++;
						tup->data = (char*) malloc(sizeof(char) * 50);

						tup->data = strcpy(tup->data, r->data);

						tup->data = strcat(tup->data, "+");

						tup->data = strcat(tup->data, s->data);

						if (z == NIL) {
							z = lappend(NIL, tup);
						} else
							z = lappend(z, tup);
					}
				}

			} else {
				false_hits++;
			}
		}

		if (r != NULL && (s == NULL/*(r->te <= x->te)*/|| (r->te < s->te))) {

			if (r->te >= y->ts) {
				y->ts = r->te;
			}

			outercell = outercell->next;

			if (outercell != NULL) {
				r = (ttup*) lfirst(outercell);

			} else {

				r = NULL;
			}

			if (r != NULL) {
				y->te = r->ts;
			} else {

				y->te = MAXLONG;

			}

		}

		else {

			if (s->te >= x->ts) {
				x->ts = s->te;

			}

			innercell = innercell->next;

			if (innercell != NULL) {
				s = (ttup*) lfirst(innercell);
			} else {
				s = NULL;
			}

			if (s != NULL) {
				x->te = s->ts;
			} else {
				x->te = MAXLONG;
			}

		}

	}

	if (mergemode == 3) {
		ylist = z;
		z = NIL;
	}

	return counter;
}

long long DIPmerge(HeapAm* outer, HeapAm* inner, int mergemode, ttup* x,
		ttup* y) {

	File* writefile;
	Buffer* writebuffer;
	HeapAm* writeStorage;
	if (mergemode == 3) {

		char buf[0x200];
		snprintf(buf, 199, "writefile%d.txt", helper);
		helper++;

		writefile = file_create(buf, inner->f->f->type);
		writebuffer = buffer_create(writefile, WORK_MEM, POLICY_LRU);
		writeStorage = ExecInitHeapAm(writebuffer, sizeof(ttup) + 50);
	}
	counter = 0;

	ttup *r = ExecHeapAmScan(outer);
	ttup *s = ExecHeapAmScan(inner);
	long long intervalstart = 0;
	long long intervalend = 0;

	x->ts = -2147483648;

	x->te = s->ts;

	y->ts = -2147483648;
	y->te = r->ts;

	while (r != NULL || (s != NULL && mergemode != 2)) {

		if (mergemode == 3) {

			if (((y->te) - (y->ts)) > 0 && s != NULL && overlapFRA(s, y)) {

				intervalend = MIN(s->te, y->te);
				intervalstart = MAX(s->ts, y->ts);
				if (mergemode == 3) {

					counter++;

					ttup* tup1 = ttupdup(s, sizeof(ttup*) + 50);
					tup1->ts = intervalstart;
					tup1->te = intervalend;

					tup1->data = (char*) malloc(sizeof(char) * 50);
					tup1->data = strcpy(tup1->data, "null+");
					tup1->data = strcat(tup1->data, s->data);

					ExecHeapAmInsert(writeStorage, tup1);

				}

			} else {
				false_hits++;
			}

		}
		if (mergemode == 2 || mergemode == 3) {
			if (((x->te) - (x->ts)) > 0 && r != NULL && overlapFRA(r, x)) {

				intervalstart = MAX(r->ts, x->ts);
				intervalend = MIN(r->te, x->te);
				if (mergemode == 2) {
					counter++;
				}
				if (mergemode == 3) {

					counter++;
					ttup*tup2 = ttupdup(r, sizeof(ttup*) + 50);

					tup2->ts = intervalstart;
					tup2->te = intervalend;

					tup2->data = (char*) malloc(sizeof(char) * 50);
					tup2->data = strcpy(tup2->data, r->data);
					tup2->data = strcat(tup2->data, "+null");

					ExecHeapAmInsert(writeStorage, tup2);

				}

			} else {
				false_hits++;
			}
		}
		if (mergemode == 1 || mergemode == 3) {
			if (r != NULL && s != NULL && overlapFRA(r, s)) {

				intervalend = MIN(r->te, s->te);
				intervalstart = MAX(r->ts, s->ts);
				if (mergemode == 1) {
					counter++;

				}
				if (mergemode == 3) {

					ttup*tup = ttupdup(s, sizeof(ttup*) + 50);

					tup->ts = intervalstart;
					tup->te = intervalend;
					if (intervalstart < intervalend) {
						counter++;
						tup->data = (char*) malloc(sizeof(char) * 50);

						tup->data = strcpy(tup->data, r->data);

						tup->data = strcat(tup->data, "+");

						tup->data = strcat(tup->data, s->data);

						ExecHeapAmInsert(writeStorage, tup);

					}

				}
			} else {
				false_hits++;
			}
		}
		if (r != NULL && ((r->te <= x->te || r->te < s->te))) {
			if (r->te >= y->ts) {
				y->ts = r->te;
			}

			r = ExecHeapAmScan(outer);

			if (r != NULL) {

				y->te = r->ts;
			} else {
				y->te = MAXLONG;
			}
		} else {
			if (s->te >= x->ts) {
				x->ts = s->te;

			}

			s = ExecHeapAmScan(inner);

			if (s != NULL) {
				x->te = s->ts;
			} else {
				x->te = MAXLONG;
			}
		}

	}
	if (mergemode == 3) {
		ExecEndHeapAM(writeStorage);
		buffer_close(writebuffer);
		file_close(writefile);
	}

	return counter;

}

static Filenode* run_Partitioning_Ordered_DISK(HeapAm *orel, long on,
		timestmp ous, timestmp oue, timestmp ol, int memdisk) {

	int partition_num = 1;
	char filename[500];
	sprintf(filename, "Partitions%d/Partition%d.txt", partition_folder,
			partition_num);

	ttup *tup;

	File *iStorageFile = file_create("mergejoin_inner_storage.dat",
			orel->f->f->type);

	Buffer *iStorageBuffer = buffer_create(iStorageFile, WORK_MEM, POLICY_LRU);
	HeapAm *iStorage = ExecInitHeapAm(iStorageBuffer, orel->htupsze);

	Sort *sort2 = ExecInitSort(orel, cmp_timestmp_start, orel->htupsze,
	WORK_MEM, orel->f->f->type);

	tup = ExecSort(sort2);

	ttup *curtup = ttupdup(tup, sizeof(ttup) + 50);

	Filenode* firstpartition = (Filenode*) malloc(sizeof(Filenode));

	firstpartition->name = (char*) malloc(200);
	firstpartition->next = NULL;
	firstpartition->lasttup = ttupdup(tup, sizeof(ttup) + 50);
	strncpy(firstpartition->name, filename, 50);
	firstpartition->counter = 1;
	Buffer *outputOuterBuffer;
	File* fi = file_create(firstpartition->name, orel->f->f->type);
	if(orel->f->f->type==TYPE_MEM){
		outputOuterBuffer = buffer_create_void(fi);
	}else{
		outputOuterBuffer = buffer_create(fi, WORK_MEM, POLICY_LRU);
	}


	HeapAm *outerputOuterStorage = ExecInitHeapAm(outputOuterBuffer,
			sizeof(ttup) + 50);
	ExecHeapAmInsert(outerputOuterStorage, tup);
	firstpartition->b = outputOuterBuffer;
	firstpartition->h = outerputOuterStorage;
	firstpartition->f = fi;
	firstpartition->isopen = 1;
	int k = 240;
	int countt = 1;
	int openfiles = 1;

	while (tup != NULL) {

		if (tup->ts <= curtup->te && tup->te >= curtup->ts && countt > 1) {
			/*ExecEndHeapAM(outerputOuterStorage);
			 buffer_close(outputOuterBuffer);

			 file_close(fi);*/

			Filenode* swap_partition = firstpartition;
			Filenode* swap_partition2 = swap_partition->next;

			Filenode* rightFrom_swap;
			Filenode* prev_swap = firstpartition;
			int count = 0;
			if (openfiles == k) {
				if(memdisk == 0){
				firstpartition->isopen = 0;
				ExecEndHeapAM(firstpartition->h);
				buffer_close(firstpartition->b);
				file_close(firstpartition->f);
				openfiles--;
				}
			}
			while (swap_partition2 != NULL) {

				if (swap_partition2->lasttup->te
						< swap_partition->lasttup->te) {

					rightFrom_swap = swap_partition2->next;

					Filenode* tmp = swap_partition;

					swap_partition = swap_partition2;

					prev_swap->next = swap_partition;

					if (count == 0) {
						firstpartition = swap_partition;
						firstpartition->lasttup = swap_partition->lasttup;
					}

					swap_partition2 = tmp;

					swap_partition->next = tmp;

					swap_partition2->next = rightFrom_swap;

				}
				count++;

				prev_swap = swap_partition;
				swap_partition = swap_partition->next;
				swap_partition2 = swap_partition2->next;
			}

			if (overlapFRA(firstpartition->lasttup, tup)) {

				Filenode* newpartition = (Filenode*) malloc(sizeof(Filenode));

				partition_num++;

				sprintf(filename, "Partitions%d/Partition%d.txt",
						partition_folder, partition_num);
				newpartition->name = (char*) malloc(200);
				strncpy(newpartition->name, filename, 200);

				File* newFile = file_create(newpartition->name,
						orel->f->f->type);

				Buffer *newBuffer;
				if(orel->f->f->type==TYPE_MEM){
					newBuffer = buffer_create_void(newFile);
				}else{
					newBuffer = buffer_create(newFile, WORK_MEM, POLICY_LRU);
				}
				HeapAm* newHeap = ExecInitHeapAm(newBuffer, sizeof(ttup) + 50);
				ExecHeapAmInsert(newHeap, tup);

				newpartition->counter = 1;
				newpartition->lasttup = ttupdup(tup, sizeof(ttup) + 50);
				newpartition->b = newBuffer;
				newpartition->h = newHeap;
				newpartition->f = newFile;
				newpartition->isopen = 1;
				curtup = ttupdup(tup, sizeof(ttup) + 50);

				newpartition->next = firstpartition;
				firstpartition = newpartition;

				openfiles++;

			} else {

				if (firstpartition->isopen == 0) {
					firstpartition->f = file_open(firstpartition->name);
					if(orel->f->f->type==TYPE_MEM){
						firstpartition->b = buffer_create_void(firstpartition->f);
					}else{
						firstpartition->b = buffer_create(firstpartition->f, WORK_MEM, POLICY_LRU);
					}

					firstpartition->h = ExecInitHeapAm(firstpartition->b,
							sizeof(ttup) + 50);
					firstpartition->isopen = 1;
					firstpartition->counter++;
					openfiles++;
				}
				ExecHeapAmInsert(firstpartition->h, tup);

				firstpartition->lasttup = ttupdup(tup, sizeof(ttup) + 50);
				curtup = ttupdup(tup, sizeof(ttup) + 50);

				//fprintf(stderr, "added in sorted : %d %d %s\n",tup->ts, tup->te,tup->data);
			}

		} else if (countt > 1) {
			//fprintf(stderr, "added in head : %d %d %s\n",tup->ts, tup->te,tup->data);

			ExecHeapAmInsert(firstpartition->h, tup);
			firstpartition->lasttup = ttupdup(tup, sizeof(ttup) + 50);
			curtup = ttupdup(tup, sizeof(ttup) + 50);
			firstpartition->counter++;

		}

		tup = ExecSort(sort2);
		countt++;
	}
	ttup* r;
	Filenode* test = firstpartition;

	while (test != NULL) {
		if (test->isopen == 1) {
			if(memdisk == 0){
			ExecEndHeapAM(test->h);
			buffer_close(test->b);
			file_close(test->f);
			}
		}
		test = test->next;
	}
	if (partition_folder == 1)
		num_outer_partitions = partition_num;
	else
		num_inner_partitions = partition_num;
	return firstpartition;

}

static List* run_Partitioning_Ordered_MM(List *rel, long on, timestmp ous,
		timestmp oue, timestmp ol) {

	list_sort(rel, cmp_timestmp_timestmp);
	ttup *tup;

	ListCell *inputcell = rel->head;

	ttup* input_tup = ((ttup*) lfirst(inputcell));

	List* first_partition;
	List* test = lappend(NIL, input_tup);

	first_partition = lappend(NIL, input_tup);

	List* new_partition;
	first_partition->counter=1;
	int count_t = 0;
	int count_p = 1;

	first_partition->next = NULL;

	foreach(inputcell, rel)
	{

		count_t++;
		input_tup = ((ttup*) lfirst(inputcell));
		ttup*current_tup = ((ttup*) lfirst(first_partition->tail));
		//	fprintf(stderr,"curtup %d %d %s\n",current_tup->ts, current_tup->te, current_tup->data);
		if (input_tup->ts <= current_tup->te && input_tup->te >= current_tup->ts
				&& count_t > 1) {

			List* swap_partition = first_partition;
			List* swap_partition2 = swap_partition->next;
			List* rightFrom_swap;
			List* prev_swap = first_partition;
			int count = 0;
			while (swap_partition2 != NULL) {
				if (((ttup*) lfirst(swap_partition2->tail))->te
						< ((ttup*) lfirst(swap_partition->tail))->te) {

					rightFrom_swap = swap_partition2->next;

					List* tmp = swap_partition;

					swap_partition = swap_partition2;

					prev_swap->next = swap_partition;

					if (count == 0) {
						first_partition = swap_partition2;
					}

					swap_partition2 = tmp;

					swap_partition->next = tmp;

					swap_partition2->next = rightFrom_swap;
					//	rightFrom_swap = swap_partition->next;

				}
				count++;

				prev_swap = swap_partition;
				swap_partition = swap_partition->next;
				swap_partition2 = swap_partition2->next;

			}

			if (overlapFRA(((ttup*) lfirst(first_partition->tail)),
					input_tup)) {
				new_partition = lappend(NIL, input_tup);
				new_partition->next = first_partition;

				first_partition = new_partition;
				first_partition->counter=1;
				count_p++;

				//re-order partition after adding a new partition
			} else {

				first_partition = lappend(first_partition, input_tup);
				first_partition->counter++;
			}

		} else if (count_t > 1) {

			first_partition = lappend(first_partition, input_tup);
			first_partition->counter++;
		}
		if (count_t > on) {
			break;
		}

	}

	if (partition_folder == 1)
		num_outer_partitions = count_p;
	else
		num_inner_partitions = count_p;
	return first_partition;

}

static List **
run_Partition_Fra_MM(List *rel, long on, timestmp ous, timestmp oue,
		timestmp ol) {
	size_t mem_usage;
	void *thandle;
	long long time;
	long match, fhits, ios;
	List* *outer_partition_tups;

	outer_partition_tups = malloc(10000 * sizeof(List *));

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
	ios = 0;
	// fprintf(stderr, "start.. ... ");

	List *tups, *partitions;
	ListCell *actualPartition;
	partition *part, *tempPart;
	partitions = NIL;
	int num_partitions = 0;
	int flag;

	int i = 1;
	int num_tuples = 0;
	tup_cell = rel->head;
	tup = ((ttup *) lfirst(tup_cell));
	outer_partition_tups[i] = lappend(outer_partition_tups[i], tup);
	outer_tup_cell[i] = outer_partition_tups[i]->head;

	foreach(tup_cell, rel)
	{
		num_tuples++;
		tup = ((ttup *) lfirst(tup_cell));
		//fprintf(stderr, "CURRENT TUP: [%ld,%ld] \n",tup->ts, tup->te);
		flag = 0;
		/* Find */

		//fprintf(stderr, "current [%ld,%ld] VS last [%ld,%ld] \n",tup->ts, tup->te, ((ttup *)lfirst(outer_tup_cell[i]))->ts, ((ttup *)lfirst(outer_tup_cell[i]))->te, i);
		if (tup->ts <= ((ttup *) lfirst(outer_tup_cell[i]))->te
				&& tup->te >= ((ttup *) lfirst(outer_tup_cell[i]))->ts) // || intervalLen(part->last)>intervalLen(tup)) //OR SMALLER GRAN.
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
			i = 1;
			/* VISIT EACH PARTITION AND TRY TO PUT TUP IN IT */
			while (i <= num_partitions) {
				//	fprintf(stderr, "Visit partition %d(%d elements)  ", part->id, part->num_tuples);
				if (overlapFRA(((ttup *) lfirst(outer_tup_cell[i])), tup)) {
					//		fprintf(stderr, "[%ld,%ld] overlaps with [%ld,%ld] \n",tup->ts, tup->te, ((ttup *)lfirst(outer_tup_cell[i]))->ts, ((ttup *)lfirst(outer_tup_cell[i]))->te);
					i++;
				} else {
					//fprintf(stderr, "[%ld,%ld] into partition %d \n",((ttup *)lfirst(outer_tup_cell[i]))->ts, ((ttup *)lfirst(outer_tup_cell[i]))->te, i);
					/* write buffer */
					outer_partition_tups[i] = lappend(outer_partition_tups[i],
							tup);
					outer_tup_cell[i] = outer_tup_cell[i]->next;
					break;
				}
			}
			if (i > num_partitions) {
				num_partitions++;
				outer_partition_tups[i] = lappend(NIL, tup);
				outer_tup_cell[i] = outer_partition_tups[i]->head;

				//fprintf(stderr, "CREATE NEW PARTITION: %d \n", i);
			}
		} else {
			//	fprintf(stderr, "[%ld,%ld] into file %d \n",part->last->ts, part->last->te, part->id);
			//	fprintf(stderr, "[%ld,%ld] into buffer \n",tup->ts, tup->te);

			//			/* write buffer */
			//fprintf(stderr, "add to partition %d \n", i);

			outer_partition_tups[i] = lappend(outer_partition_tups[i], tup);
			outer_tup_cell[i] = outer_tup_cell[i]->next;
		}
		//freeTTup(tup);
		if (num_tuples > on) {
			break;
		}
	}

	time = time_end(thandle);
	fprintf(stderr, "done.\n");

	i = 0;
	/* while((++i)<=num_partitions)
	 {
	 fprintf(stderr,"PARTITION ID=%d, LEN=%d \n", i, (outer_partition_tups[i])->length);
	 }
	 fflush(stderr); */

	outer_partition_tups[i] = NIL;
	//ios = buffer_numReads(oStorageBuffer) + buffer_numReads(iStorageBuffer);

	return outer_partition_tups;

}

static void reorderParitions(ListCell *lc) {
	//fprintf(stderr, "Reorder Partition %d(size %d) \n", ((partition *)(lc->ptr_value))->id, ((partition *)(lc->ptr_value))->num_tuples);
	while (lc->next != NULL) {
		/*fprintf(stderr, "Reorder: Compare Partition %d(size %d, prev_last[%ld,%ld], last[%ld,%ld]) with partition %d(size %d, prev_last[%ld,%ld], last[%ld,%ld]) \n",
		 ((partition *)(actualPartition->ptr_value))->id, ((partition *)(actualPartition->ptr_value))->num_tuples,
		 ((partition *)(actualPartition->ptr_value))->prev_last->ts, ((partition *)(actualPartition->ptr_value))->prev_last->te,
		 ((partition *)(actualPartition->ptr_value))->last->ts, ((partition *)(actualPartition->ptr_value))->last->te,
		 ((partition *) (actualPartition->next->ptr_value))->id, ((partition *) (actualPartition->next->ptr_value))->num_tuples,
		 ((partition *) (actualPartition->next->ptr_value))->prev_last->ts, ((partition *) (actualPartition->next->ptr_value))->prev_last->te,
		 ((partition *) (actualPartition->next->ptr_value))->last->ts, ((partition *) (actualPartition->next->ptr_value))->last->te);*/

		if (((partition *) (lc->ptr_value))->num_tuples
				> ((partition *) (lc->next->ptr_value))->num_tuples) {
			/*fprintf(stderr, "Reorder: Exchange Partition %d(size %d) with partition %d(size %d) \n",
			 ((partition *)(lc->ptr_value))->id, ((partition *)(lc->ptr_value))->num_tuples,
			 ((partition *) (lc->next->ptr_value))->id, ((partition *) (lc->next->ptr_value))->num_tuples);*/
			void *tmp = lc->ptr_value;
			lc->ptr_value = lc->next->ptr_value;
			lc->next->ptr_value = tmp;

		} else
			break;

		lc = lc->next;
	}
}

static void run_readFile_Fra_DISK(char *fname) {
	size_t mem_usage;
	void *thandle;
	ttup *tup;
	long long time;

	thandle = time_start();

	File *inputF = file_open(fname);
	fprintf(stderr, "%s  \n", fname);

	Buffer *oStorageBuffer = buffer_create(inputF, WORK_MEM, POLICY_LRU);
	HeapAm *oStorage = ExecInitHeapAm(oStorageBuffer, sizeof(ttup) + 50);

	ExecHeapAmScanInit(oStorage);
	tup = ExecHeapAmScan(oStorage);
	while (tup != NULL) {
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

static int intervalLen(ttup *tup1) {
	return (tup1->te - tup1->ts);
}

static int overlapFRA(ttup *tup1, ttup *tup) {
	if ((tup1->ts < tup->te && tup1->te > tup->ts))
		return 1;
	return 0;
}

static int overlapFRAtup(ttup tup1, ttup *tup) {
	if ((tup1.ts < tup->te && tup1.te > tup->ts))
		return 1;
	return 0;
}
static int overlapFRAtuptup(ttup tup1, ttup tup) {
	if ((tup1.ts < tup.te && tup1.te > tup.ts))
		return 1;
	return 0;
}
