#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdarg.h>
#include <errno.h>
#include <unistd.h>
#include <termios.h>
#include <sys/queue.h>

#include <rte_common.h>
#include <rte_memory.h>
#include <rte_memzone.h>
#include <rte_launch.h>
#include <rte_eal.h>
#include <rte_per_lcore.h>
#include <rte_lcore.h>
#include <rte_debug.h>
#include <rte_atomic.h>
#include <rte_branch_prediction.h>
#include <rte_ring.h>
#include <rte_log.h>
#include <rte_mempool.h>
#include <cmdline_rdline.h>
#include <cmdline_parse.h>
#include <cmdline_socket.h>
#include <cmdline.h>
#include <rte_mbuf.h>
#include <rte_errno.h>

#include <time.h> 

#include "allHeaders.h"
//This version runs in the format of frame,which 8 pthread run on 8 individual cores 

#define RUNMAINDPDK
#ifdef RUNMAINDPDK

#define RTE_LOGTYPE_APP RTE_LOGTYPE_USER1
// #define MEMPOOL_F_SP_PUT         0x0

#define MBUF_CACHE_SIZE 128
#define NUM_MBUFS 4095
#define	NUM_MBUFS2 4095
#define interval 4
static const char *MBUF_POOL = "MBUF_POOL";
static const char *Mbuf_for_precoding_pool = "Mbuf_for_precoding_pool";

static const char *GenerateData1 = "GenerateData1";
static const char *GenerateData2 = "GenerateData2";
static const char *GenerateData3 = "GenerateData3";
static const char *GenerateData4 = "GenerateData4";

static const char *RetriveData1 = "RetriveData1";
static const char *RetriveData2 = "RetriveData2";
static const char *RetriveData3 = "RetriveData3";
static const char *RetriveData4 = "RetriveData4";

static const char *DatatoRRU = "DatatoRRU";
const unsigned APEP_LEN_DPDK = 512;

// static int i=0; 
struct rte_ring *Ring_GenerateData1;
struct rte_ring *Ring_GenerateData2;
struct rte_ring *Ring_GenerateData3;
struct rte_ring *Ring_GenerateData4;

struct rte_ring *Ring_RetriveData1;
struct rte_ring *Ring_RetriveData2;
struct rte_ring *Ring_RetriveData3;
struct rte_ring *Ring_RetriveData4;

struct rte_ring *Ring_DatatoRRU;
struct rte_mempool *mbuf_pool;
struct rte_mempool *mbuf_precode_pool;
	
volatile int quit = 0;

long int Retrive_DPDK_count = 0;
long int GenerateData_Loop1_count = 0;
long int GenerateData_Loop2_count = 0;
long int GenerateData_Loop3_count = 0;
long int GenerateData_Loop4_count = 0;
long int Data_Retrive_Loop_count = 0;
long int Ring_full_count = 0;
long int mbuf_full_count = 0;
long int DatatoTTU_Loopcount = 0;

complex32 precode_data[4][256*2*16];


int N_CBPS, N_SYM, ScrLength, valid_bits;

struct timespec time1,time2,time_diff;	/** < Test the running time. >*/
int time_test_flag = 0;
struct timespec diff(struct timespec start, struct timespec end);

static int ReadData(__attribute__((unused)) struct rte_mbuf *Data_out, unsigned char* Data_in);
static int GenDataAndScramble_DPDK (__attribute__((unused)) struct rte_mbuf *Data_In);
static int BCC_encoder_DPDK (__attribute__((unused)) struct rte_mbuf *Data_In);
static int modulate_DPDK (__attribute__((unused)) struct rte_mbuf *Data_In);
static int CSD_encode_dpdk (__attribute__((unused)) struct rte_mbuf *Data_In);
//static int IFFTAndaddWindow_dpdk(__attribute__((unused)) struct rte_mbuf *Data_In);


static int ReadData_Loop();
static int GenerateData_Loop1();
static int GenerateData_Loop2();
static int GenerateData_Loop3();
static int GenerateData_Loop4();
static int Data_Retrive_Loop();
static int Data_sendto_RRU_loop();

struct timespec diff(struct timespec start, struct timespec end)
{
    struct  timespec temp;
    if ((end.tv_nsec-start.tv_nsec)<0) {
         temp.tv_sec = end.tv_sec-start.tv_sec-1;
         temp.tv_nsec = 1000000000+end.tv_nsec-start.tv_nsec;
    } 
    else {
         temp.tv_sec = end.tv_sec-start.tv_sec;
         temp.tv_nsec = end.tv_nsec-start.tv_nsec;
    }
     return temp;
}


static int InitData(unsigned char** p_databits)
{
	FILE *fp=fopen("send_din_dec.txt","rt");
	unsigned char *databits=(unsigned char*)malloc(APEP_LEN_DPDK*sizeof(unsigned char));
	*p_databits = databits;
	if(databits == NULL){
		printf("error");
		return 0;
	}
	unsigned int datatmp=0;
	int i;
	for(i=0;i<APEP_LEN_DPDK;i++){
	    fscanf(fp,"%ud",&datatmp);
	    databits[i]=datatmp&0x000000FF;
	}
	fclose(fp);
	return 0;
}


static int ReadData(__attribute__((unused)) struct rte_mbuf *Data_out, unsigned char* Data_in) 
{
	if(time_test_flag==0){
		time_test_flag = 1;
		clock_gettime(CLOCK_REALTIME, &time1); //CLOCK_REALTIME:系统实时时间   
		//CLOCK_MONOTONIC:从系统启动这一刻起开始计时,不受系统时间被用户改变的影响
		//CLOCK_PROCESS_CPUTIME_ID:本进程到当前代码系统CPU花费的时间
		//CLOCK_THREAD_CPUTIME_ID:本线程到当前代码系统CPU花费的时间
	}
	rte_memcpy(rte_pktmbuf_mtod(Data_out,unsigned char *), Data_in, APEP_LEN_DPDK);
	return 0;
}


static int GenDataAndScramble_DPDK (__attribute__((unused)) struct rte_mbuf *Data_In) 
{	
	unsigned char *databits = rte_pktmbuf_mtod(Data_In, unsigned char *);
	unsigned char *data_scramble = rte_pktmbuf_mtod_offset(Data_In, unsigned char *, RTE_MBUF_DEFAULT_BUF_SIZE*15);
	GenDataAndScramble(data_scramble, ScrLength, databits, valid_bits);	
	return 0;
}


static int BCC_encoder_DPDK (__attribute__((unused)) struct rte_mbuf *Data_In)
{
	int CodeLength = N_SYM*N_CBPS/N_STS;
	unsigned char *data_scramble = rte_pktmbuf_mtod_offset(Data_In, unsigned char *, RTE_MBUF_DEFAULT_BUF_SIZE*15);
	unsigned char* BCCencodeout = rte_pktmbuf_mtod_offset(Data_In, unsigned char *, 0);
	BCC_encoder_OPT(data_scramble, ScrLength, N_SYM, &BCCencodeout, CodeLength);
	return 0;
}


static int modulate_DPDK(__attribute__((unused)) struct rte_mbuf *Data_In)
{
	unsigned char* BCCencodeout = rte_pktmbuf_mtod_offset(Data_In, unsigned char *, 0);
	complex32 *subcar_map_data = rte_pktmbuf_mtod_offset(Data_In, complex32 *, RTE_MBUF_DEFAULT_BUF_SIZE*15);
	modulate_mapping(BCCencodeout,&subcar_map_data);
	return 0;
}	


static int CSD_encode_DPDK (__attribute__((unused)) struct rte_mbuf *Data_In)
{
	int i;
	complex32 *csd_data = rte_pktmbuf_mtod_offset(Data_In, complex32 *,0 );
	complex32 *subcar_map_data = rte_pktmbuf_mtod_offset(Data_In, complex32 *, RTE_MBUF_DEFAULT_BUF_SIZE*15);
	for(i=0;i<N_STS;i++){
		__Data_CSD_aux(&subcar_map_data, N_SYM, &csd_data,i);
	}
	return 0;
}

/*
static int IFFTAndaddWindow_dpdk(__attribute__((unused)) struct rte_mbuf *Data_In)
{
	int i;
	//IFFTAndaddWindow_dpdk_count++;
	complex32 *csd_data = rte_pktmbuf_mtod_offset(Data_In, complex32 *,0 );
	complex32 *IFFT_data = rte_pktmbuf_mtod_offset(Data_In,complex32 *,RTE_MBUF_DEFAULT_BUF_SIZE*15);
	csd_data_IDFT(csd_data,IFFT_data,N_SYM);
	/*FILE *k=fopen("IFFT_data.txt","w");
	printStreamToFile_float(IFFT_data,5120,k);
	fclose(k);
	return 0;
}
*/

static int Data_sendto_RRU_loop()
{
	void *Data_In_GenerateData=NULL;
	while (!quit)
	{
		if (rte_ring_dequeue(Ring_DatatoRRU, &Data_In_GenerateData) >= 0 )
		{
			DatatoTTU_Loopcount++;
			//Data_sendto_RRU();
			rte_mempool_put(((struct rte_mbuf *)Data_In_GenerateData)->pool, Data_In_GenerateData);
			
		}
		else 
		{	
			//DatatoTTU_Loopcount++;
			//usleep(10000);
			continue;
		}
		if(DatatoTTU_Loopcount >= 1000)
			{
			quit = 1;
			clock_gettime(CLOCK_REALTIME, &time2);
			time_diff = diff(time1,time2);
			printf("Retrive_DPDK_count = %ld\n", Retrive_DPDK_count);
			printf("DatatoTTU_Loopcount = %ld\n", DatatoTTU_Loopcount);
			printf("Start time # %.24s %ld Nanoseconds \n",ctime(&time1.tv_sec), time1.tv_nsec);
			printf("Stop time # %.24s %ld Nanoseconds \n",ctime(&time2.tv_sec), time2.tv_nsec);
			printf("Running time # %ld.%ld Seconds \n",time_diff.tv_sec, time_diff.tv_nsec);
			printf("GenerateData_Loop1_count = %ld\n", GenerateData_Loop1_count);
			printf("GenerateData_Loop2_count = %ld\n", GenerateData_Loop2_count);
			printf("GenerateData_Loop3_count = %ld\n", GenerateData_Loop3_count);
			printf("GenerateData_Loop4_count = %ld\n", GenerateData_Loop4_count);

			printf("Data_Retrive_Loop_count = %ld\n", Data_Retrive_Loop_count);
			printf("Ring_full_count = %ld\n",Ring_full_count);
			printf("mbuf_full_count = %ld\n",mbuf_full_count);
			//free(dest);
			}
	
	}
	return 0;
}

static int check_empty_Ring()
{
	if (rte_ring_empty(Ring_RetriveData1) == 1 ) 
		return 1;
	if (rte_ring_empty(Ring_RetriveData2) == 1 ) 
		return 1;
	if (rte_ring_empty(Ring_RetriveData3) == 1 ) 
		return 1;
	if (rte_ring_empty(Ring_RetriveData4) == 1 ) 
		return 1;
	 
	return 0 ;			
}
static int Data_Retrive_Loop() 
{
	__attribute__((unused)) struct rte_mbuf *data = NULL;
	void *Data_User_1=NULL;
	void *Data_User_2=NULL;
	void *Data_User_3=NULL;
	void *Data_User_4=NULL;
	complex32 *User_1 = NULL;
	complex32 *User_2 = NULL;
	complex32 *User_3 = NULL;
	complex32 *User_4 = NULL;

	int i,j;
	int k = 0;
	complex32 *dest = NULL;
	complex32 X[4] = {{0,0}};
	//get the precoding matrix
	complex32 h[16][16];
	srand((unsigned)time(NULL));
	for(i=0; i<16 ; i++){
		for (j = 0; j < 16; j++){
			h[i][j].real = (double)(rand() / (double)RAND_MAX) * (0x1 << 13);
			h[i][j].imag = (double)(rand() / (double)RAND_MAX) * (0x1 << 13);
		}
	}

	while (!quit)
	{		
		while(data ==	NULL) {
			usleep(100);
			data = rte_pktmbuf_alloc(mbuf_precode_pool);
		}
		if (!check_empty_Ring()){		
			Retrive_DPDK_count++;
			rte_ring_dequeue(Ring_RetriveData1, &Data_User_1) ;
			rte_ring_dequeue(Ring_RetriveData2, &Data_User_2) ;
			rte_ring_dequeue(Ring_RetriveData3, &Data_User_3) ;
			rte_ring_dequeue(Ring_RetriveData4, &Data_User_4) ;
			User_1 = rte_pktmbuf_mtod_offset((struct rte_mbuf *)Data_User_1, complex32 *,0 );
			User_2 = rte_pktmbuf_mtod_offset((struct rte_mbuf *)Data_User_2, complex32 *,0 );
			User_3 = rte_pktmbuf_mtod_offset((struct rte_mbuf *)Data_User_3, complex32 *,0 );
			User_4 = rte_pktmbuf_mtod_offset((struct rte_mbuf *)Data_User_4, complex32 *,0 );
			// //Precode_processing

			// for(i = 0 ; i < subcar*N_SYM*N_STS ; i++){
			// 	X[0] = *(User_1 + i);
			// 	X[1] = *(User_2 + i);
			// 	X[2] = *(User_3 + i);
			// 	X[3] = *(User_4 + i);	
			// 	dest = rte_pktmbuf_mtod_offset(data, complex32 *, 4*i);
			// 	//Matrix_Mult_AVX2_16(h,X,dest);					
			// }
			//memcpy(precode_data[k],rte_pktmbuf_mtod_offset(data, complex32 *,0),subcar*N_SYM*N_STS/4*16);
			rte_ring_enqueue(Ring_DatatoRRU, data);
			data = NULL ;			
			/*FILE *ts =fopen("precoding_data.txt","w");
				for(i=0;i<4;i++)
					printStreamToFile(precode_data[i],subcar*N_SYM*N_STS/4*16,ts);	*/
			rte_mempool_put(((struct rte_mbuf *)Data_User_1)->pool, Data_User_1);
			rte_mempool_put(((struct rte_mbuf *)Data_User_2)->pool, Data_User_2);
			rte_mempool_put(((struct rte_mbuf *)Data_User_3)->pool, Data_User_3);
			rte_mempool_put(((struct rte_mbuf *)Data_User_4)->pool, Data_User_4);
			Data_User_1 = NULL;
			Data_User_2 = NULL;
			Data_User_3 = NULL;
			Data_User_4 = NULL;
			User_1 = NULL;
			User_2 = NULL;
			User_3 = NULL;
			User_4 = NULL;
		}
		else {
			Data_Retrive_Loop_count++;
			usleep(1000);
			continue;
		}
		
	
	}
	return 0;
}

static int GenerateData_Loop1() 
{
	void *Data_In_GenerateData=NULL;
	while (!quit)
	{
		if (rte_ring_dequeue(Ring_GenerateData1, &Data_In_GenerateData) >= 0 )
		{
			GenDataAndScramble_DPDK(Data_In_GenerateData);
			BCC_encoder_DPDK(Data_In_GenerateData);
			modulate_DPDK(Data_In_GenerateData);
			CSD_encode_DPDK(Data_In_GenerateData);
			rte_ring_enqueue(Ring_RetriveData1, Data_In_GenerateData);
		}
		else 
		{	
			GenerateData_Loop1_count++;
			//usleep(10000);
			continue;
		}
	
	}
	return 0;
}

static int GenerateData_Loop2() 
{
	void *Data_In_GenerateData=NULL;
	while (!quit)
	{
		if (rte_ring_dequeue(Ring_GenerateData2, &Data_In_GenerateData) >= 0 )
		{
			GenDataAndScramble_DPDK(Data_In_GenerateData);
			BCC_encoder_DPDK(Data_In_GenerateData);
			modulate_DPDK(Data_In_GenerateData);
			CSD_encode_DPDK(Data_In_GenerateData);
			rte_ring_enqueue(Ring_RetriveData2, Data_In_GenerateData);
		}
		else 
		{	
			GenerateData_Loop2_count++;
			//usleep(10000);
			continue;
		}
	
	}
	return 0;
}

static int GenerateData_Loop3() 
{
	void *Data_In_GenerateData=NULL;
	while (!quit)
	{
		if (rte_ring_dequeue(Ring_GenerateData3, &Data_In_GenerateData) >= 0 )
		{
			GenDataAndScramble_DPDK(Data_In_GenerateData);
			BCC_encoder_DPDK(Data_In_GenerateData);
			modulate_DPDK(Data_In_GenerateData);
			CSD_encode_DPDK(Data_In_GenerateData);
			rte_ring_enqueue(Ring_RetriveData3, Data_In_GenerateData);
		}
		else 
		{	
			GenerateData_Loop3_count++;
			//usleep(10000);
			continue;
		}
	
	}
	return 0;
}

static int GenerateData_Loop4() 
{
	void *Data_In_GenerateData=NULL;
	while (!quit)
	{
		if (rte_ring_dequeue(Ring_GenerateData4, &Data_In_GenerateData) >= 0 )
		{
			GenDataAndScramble_DPDK(Data_In_GenerateData);
			BCC_encoder_DPDK(Data_In_GenerateData);
			modulate_DPDK(Data_In_GenerateData);
			CSD_encode_DPDK(Data_In_GenerateData);
			rte_ring_enqueue(Ring_RetriveData4, Data_In_GenerateData);
		}
		else 
		{	
			GenerateData_Loop4_count++;
			//usleep(10000);
			continue;
		}
	
	}
	return 0;
}

static int FindEmptyRing()
{
	if(rte_ring_full(Ring_GenerateData1)==0)
	{
		return 1;
	}
	else if(rte_ring_full(Ring_GenerateData2)==0)
	{
		return 2;
	}
	else if(rte_ring_full(Ring_GenerateData3)==0)
	{
		return 3;
	}
	else if(rte_ring_full(Ring_GenerateData4)==0)
	{
		return 4;
	}
	else
	{
		return 0;
	} 
}

static int PutInEmptyRing(struct rte_mbuf *Data,int n)
{
	switch(n){
		case 1:	rte_ring_enqueue(Ring_GenerateData1, Data);
			break;
		case 2:	rte_ring_enqueue(Ring_GenerateData2, Data);
			break;
		case 3:	rte_ring_enqueue(Ring_GenerateData3, Data);
			break;
		case 4:	rte_ring_enqueue(Ring_GenerateData4, Data);
			break;
		//default:printf("Not empty ring\n");
	}

	return 0;
}
static int ReadData_Loop() 
{
	struct rte_mbuf *Data =NULL;
	unsigned char* Data_in =NULL;
	int dis_count = 0;
	int n;
	InitData(&Data_in);
	while (!quit){
		n=FindEmptyRing();
		if(n!=0){
			Data = rte_pktmbuf_alloc(mbuf_pool);
			if (Data != NULL){
				ReadData(Data, Data_in);
				//rte_ring_enqueue(Ring_Beforescramble, Data);
				dis_count++;
				if(dis_count == interval*4){
					dis_count = 0;
				}

				if(dis_count >= 0 && dis_count < interval){
				    	if(rte_ring_full(Ring_GenerateData1)==0)
				         		rte_ring_enqueue(Ring_GenerateData1, Data);
				     	else
				     		PutInEmptyRing(Data,n);
				}	
				else if(dis_count >= interval && dis_count < interval*2){
				    	if(rte_ring_full(Ring_GenerateData2)==0)
				       		rte_ring_enqueue(Ring_GenerateData2, Data);
				    	else
				    		PutInEmptyRing(Data,n);
				}
				else if(dis_count >= interval*2 && dis_count < interval*3){
				    	if(rte_ring_full(Ring_GenerateData3)==0)
				       		rte_ring_enqueue(Ring_GenerateData3, Data);
				    	else
				    		PutInEmptyRing(Data,n);
				}
				else if(dis_count >= interval*3 && dis_count < interval*4){
				    	if(rte_ring_full(Ring_GenerateData4)==0)
				       		rte_ring_enqueue(Ring_GenerateData4, Data);
				    	else
				    		PutInEmptyRing(Data,n);
				}
			}
			else{			 
			    mbuf_full_count++;
			    //usleep(100000);
			    continue;
			}
		}
		else{
			Ring_full_count++;
			continue;
		}
	}   
	return 0;
}



int
main(int argc, char **argv)
{
	const unsigned flags = 0;
	const unsigned ring_size = 4096;
	const unsigned pool_size = 256;
	const unsigned pool_cache = 128;
	const unsigned priv_data_sz = 0;
	int ret;
	// 运行一次得到preamble和HeLTF.
	//generatePreambleAndHeLTF_csd();
	// 运行一次得到比特干扰码表。
	Creatnewchart();
	// 运行一次得到BCC编码表。
	init_BCCencode_table();
	
	// 运行一次得到CSD表。
	//initcsdTableForHeLTF();
	// 初始化函数，计算OFDM符号个数，字节长度
	//int N_CBPS, N_SYM, ScrLength, valid_bits;
   	GenInit(&N_CBPS, &N_SYM, &ScrLength, &valid_bits);

   	// 运行一次得到生成导频的分流交织表
	initial_streamwave_table(N_SYM);
	init_mapping_table();
	///////////////////////////////////////////////////////////////////////////////////
	//unsigned lcore_id;
	ret = rte_eal_init(argc, argv);
	if (ret < 0)
		rte_exit(EXIT_FAILURE, "Cannot init EAL\n");
	Ring_GenerateData1 = rte_ring_create(GenerateData1 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);
	Ring_GenerateData2 = rte_ring_create(GenerateData2 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);
	Ring_GenerateData3 = rte_ring_create(GenerateData3 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);
	Ring_GenerateData4 = rte_ring_create(GenerateData4 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);

	Ring_RetriveData1 = rte_ring_create(RetriveData1 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);
	Ring_RetriveData2 = rte_ring_create(RetriveData2 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);
	Ring_RetriveData3 = rte_ring_create(RetriveData3 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);
	Ring_RetriveData4 = rte_ring_create(RetriveData4 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);

	Ring_DatatoRRU = rte_ring_create(DatatoRRU, 1024, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);;

	if (Ring_GenerateData1 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_GenerateData2 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_GenerateData3 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_GenerateData4 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");

	if (Ring_RetriveData1 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_RetriveData2 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_RetriveData3 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_RetriveData4 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");

	if (Ring_DatatoRRU == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");

	/* Creates a new mempool in memory to hold the mbufs. */
	mbuf_pool = rte_pktmbuf_pool_create(MBUF_POOL, NUM_MBUFS,
		MBUF_CACHE_SIZE, 0, RTE_MBUF_DEFAULT_BUF_SIZE*10, rte_socket_id());

	mbuf_precode_pool = rte_pktmbuf_pool_create(Mbuf_for_precoding_pool,NUM_MBUFS2,MBUF_CACHE_SIZE,
		0,RTE_MBUF_DEFAULT_BUF_SIZE*18,rte_socket_id());


	if (mbuf_pool == NULL)
		rte_exit(EXIT_FAILURE, "Cannot create mbuf pool\n");
	if (mbuf_precode_pool == NULL)
		rte_exit(EXIT_FAILURE, "Cannot create mbuf pool for precoding:%s\n",rte_strerror(rte_errno));

	RTE_LOG(INFO, APP, "Finished Process Init.\n");
	rte_eal_remote_launch(ReadData_Loop, NULL,1);
	rte_eal_remote_launch(GenerateData_Loop1, NULL,2);
	rte_eal_remote_launch(GenerateData_Loop2, NULL,3);
	rte_eal_remote_launch(GenerateData_Loop3, NULL,4);
	rte_eal_remote_launch(GenerateData_Loop4, NULL,5);
	rte_eal_remote_launch(Data_Retrive_Loop, NULL,6);
	rte_eal_remote_launch(Data_sendto_RRU_loop, NULL,7); 
	rte_eal_mp_wait_lcore();
	return 0;
}
#endif // RUN
