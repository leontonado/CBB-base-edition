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

//#define RUNMAINDPDK
#ifdef RUNMAINDPDK

#define RTE_LOGTYPE_APP RTE_LOGTYPE_USER1
// #define MEMPOOL_F_SP_PUT         0x0

#define MBUF_CACHE_SIZE 128
#define NUM_MBUFS 4095
#define	NUM_MBUFS2 511
#define interval 4
static const char *MBUF_POOL = "MBUF_POOL";
static const char *Mbuf_for_precoding_pool = "Mbuf_for_precoding_pool";

static const char *GenerateData1 = "GenerateData1";
static const char *GenerateData2 = "GenerateData2";
static const char *GenerateData3 = "GenerateData3";
static const char *GenerateData4 = "GenerateData4";
static const char *GenerateData5 = "GenerateData5";
static const char *GenerateData6 = "GenerateData6";
static const char *GenerateData7 = "GenerateData7";
static const char *GenerateData8 = "GenerateData8";
static const char *GenerateData9 = "GenerateData9";
static const char *GenerateData10 = "GenerateData10";
static const char *GenerateData11 = "GenerateData11";
static const char *GenerateData12 = "GenerateData12";
static const char *GenerateData13 = "GenerateData13";
static const char *GenerateData14 = "GenerateData14";
static const char *GenerateData15 = "GenerateData15";
static const char *GenerateData16 = "GenerateData16";

static const char *RetriveData1 = "RetriveData1";
static const char *RetriveData2 = "RetriveData2";
static const char *RetriveData3 = "RetriveData3";
static const char *RetriveData4 = "RetriveData4";
static const char *RetriveData5 = "RetriveData5";
static const char *RetriveData6 = "RetriveData6";
static const char *RetriveData7 = "RetriveData7";
static const char *RetriveData8 = "RetriveData8";
static const char *RetriveData9 = "RetriveData9";
static const char *RetriveData10 = "RetriveData10";
static const char *RetriveData11 = "RetriveData11";
static const char *RetriveData12 = "RetriveData12";
static const char *RetriveData13 = "RetriveData13";
static const char *RetriveData14 = "RetriveData14";
static const char *RetriveData15 = "RetriveData15";
static const char *RetriveData16 = "RetriveData16";

static const char *DatatoRRU = "DatatoRRU";
const unsigned APEP_LEN_DPDK = 512;

// static int i=0; 
struct rte_ring *Ring_GenerateData1;
struct rte_ring *Ring_GenerateData2;
struct rte_ring *Ring_GenerateData3;
struct rte_ring *Ring_GenerateData4;
struct rte_ring *Ring_GenerateData5;
struct rte_ring *Ring_GenerateData6;
struct rte_ring *Ring_GenerateData7;
struct rte_ring *Ring_GenerateData8;
struct rte_ring *Ring_GenerateData9;
struct rte_ring *Ring_GenerateData10;
struct rte_ring *Ring_GenerateData11;
struct rte_ring *Ring_GenerateData12;
struct rte_ring *Ring_GenerateData13;
struct rte_ring *Ring_GenerateData14;
struct rte_ring *Ring_GenerateData15;
struct rte_ring *Ring_GenerateData16;




struct rte_ring *Ring_RetriveData1;
struct rte_ring *Ring_RetriveData2;
struct rte_ring *Ring_RetriveData3;
struct rte_ring *Ring_RetriveData4;
struct rte_ring *Ring_RetriveData5;
struct rte_ring *Ring_RetriveData6;
struct rte_ring *Ring_RetriveData7;
struct rte_ring *Ring_RetriveData8;
struct rte_ring *Ring_RetriveData9;
struct rte_ring *Ring_RetriveData10;
struct rte_ring *Ring_RetriveData11;
struct rte_ring *Ring_RetriveData12;
struct rte_ring *Ring_RetriveData13;
struct rte_ring *Ring_RetriveData14;
struct rte_ring *Ring_RetriveData15;
struct rte_ring *Ring_RetriveData16;

struct rte_ring *Ring_DatatoRRU;
struct rte_mempool *mbuf_pool;
struct rte_mempool *mbuf_precode_pool;

	
volatile int quit = 0;

long int Retrive_DPDK_count = 0;
long int GenerateData_Loop1_count = 0;
long int GenerateData_Loop2_count = 0;
long int GenerateData_Loop3_count = 0;
long int GenerateData_Loop4_count = 0;
long int GenerateData_Loop5_count = 0;
long int GenerateData_Loop6_count = 0;
long int GenerateData_Loop7_count = 0;
long int GenerateData_Loop8_count = 0;
long int GenerateData_Loop9_count = 0;
long int GenerateData_Loop10_count = 0;
long int GenerateData_Loop11_count = 0;
long int GenerateData_Loop12_count = 0;
long int GenerateData_Loop13_count = 0;
long int GenerateData_Loop14_count = 0;
long int GenerateData_Loop15_count = 0;
long int GenerateData_Loop16_count = 0;
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
static int GenerateData_Loop5();
static int GenerateData_Loop6();
static int GenerateData_Loop7();
static int GenerateData_Loop8();
static int GenerateData_Loop9();
static int GenerateData_Loop10();
static int GenerateData_Loop11();
static int GenerateData_Loop12();
static int GenerateData_Loop13();
static int GenerateData_Loop14();
static int GenerateData_Loop15();
static int GenerateData_Loop16();
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
	for(int i=0;i<APEP_LEN_DPDK;i++){
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
			//Data_sendto_RRU();
			rte_mempool_put(((struct rte_mbuf *)Data_In_GenerateData)->pool, Data_In_GenerateData);
		}
		else 
		{	
			DatatoTTU_Loopcount++;
			//usleep(10000);
			continue;
		}
	
	}
	return 0;
}

static int check_empty_Ring()
{
	if (rte_ring_empty(Ring_RetriveData1) == 1 ) 
		return 1;
	else if (rte_ring_empty(Ring_RetriveData2) == 1 ) 
		return 1;
	else if (rte_ring_empty(Ring_RetriveData3) == 1 ) 
		return 1;
	else if (rte_ring_empty(Ring_RetriveData4) == 1 ) 
		return 1;
	else if (rte_ring_empty(Ring_RetriveData5) == 1 ) 
		return 1;
	else if (rte_ring_empty(Ring_RetriveData6) == 1 ) 
		return 1;
	else if (rte_ring_empty(Ring_RetriveData7) == 1 ) 
		return 1;
	else if (rte_ring_empty(Ring_RetriveData8) == 1 ) 
		return 1;
	else if (rte_ring_empty(Ring_RetriveData9) == 1 ) 
		return 1;
	else if (rte_ring_empty(Ring_RetriveData10) == 1 ) 
		return 1;
	else if (rte_ring_empty(Ring_RetriveData11) == 1 ) 
		return 1;
	else if (rte_ring_empty(Ring_RetriveData12) == 1 ) 
		return 1;
	else if (rte_ring_empty(Ring_RetriveData13) == 1 ) 
		return 1;
	else if (rte_ring_empty(Ring_RetriveData14) == 1 ) 
		return 1;
	else if (rte_ring_empty(Ring_RetriveData15) == 1 ) 
		return 1;
	else if (rte_ring_empty(Ring_RetriveData16) == 1 ) 
		return 1;
	else 
		return 0 ;			
}
static int Data_Retrive_Loop() 
{
	// struct rte_mbuf *data1 = NULL;
	// struct rte_mbuf *data2 = NULL;
	// struct rte_mbuf *data3 = NULL;
	// struct rte_mbuf *data4 = NULL;
	__attribute__((unused)) struct rte_mbuf *data = NULL;
	void *Data_User_1=NULL;
	void *Data_User_2=NULL;
	void *Data_User_3=NULL;
	void *Data_User_4=NULL;
	void *Data_User_5=NULL;
	void *Data_User_6=NULL;
	void *Data_User_7=NULL;
	void *Data_User_8=NULL;
	void *Data_User_9=NULL;
	void *Data_User_10=NULL;
	void *Data_User_11=NULL;
	void *Data_User_12=NULL;
	void *Data_User_13=NULL;
	void *Data_User_14=NULL;
	void *Data_User_15=NULL;
	void *Data_User_16=NULL;
	complex32 *User_1 = NULL;
	complex32 *User_2 = NULL;
	complex32 *User_3 = NULL;
	complex32 *User_4 = NULL;
	complex32 *User_5 = NULL;
	complex32 *User_6 = NULL;
	complex32 *User_7 = NULL;
	complex32 *User_8 = NULL;
	complex32 *User_9 = NULL;
	complex32 *User_10 = NULL;
	complex32 *User_11 = NULL;
	complex32 *User_12 = NULL;
	complex32 *User_13 = NULL;
	complex32 *User_14 = NULL;
	complex32 *User_15 = NULL;
	complex32 *User_16 = NULL;

	int i,j;
	int k = 0;
	complex32 *dest = NULL;
	complex32 X[16] = {{0,0}};
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
		// while(data1 ==	NULL) {
		// 	usleep(100);
		// 	data1 = rte_pktmbuf_alloc(mbuf_precode_pool);
		// }
		// while(data2 ==	NULL) {
		// 	usleep(100);
		// 	data2 = rte_pktmbuf_alloc(mbuf_precode_pool);
		// }
		// while(data3 ==	NULL) {
		// 	usleep(100);
		// 	data3 = rte_pktmbuf_alloc(mbuf_precode_pool);
		// }
		// while(data4 ==	NULL) {
		// 	usleep(100);
		// 	data4 = rte_pktmbuf_alloc(mbuf_precode_pool);
		// }		
		if (!check_empty_Ring()){			
				Retrive_DPDK_count++;
				while(rte_ring_dequeue(Ring_RetriveData1, &Data_User_1) < 0);
				while(rte_ring_dequeue(Ring_RetriveData2, &Data_User_2) < 0);
				while(rte_ring_dequeue(Ring_RetriveData3, &Data_User_3) < 0);
				while(rte_ring_dequeue(Ring_RetriveData4, &Data_User_4) < 0);
				while(rte_ring_dequeue(Ring_RetriveData5, &Data_User_5) < 0);
				while(rte_ring_dequeue(Ring_RetriveData6, &Data_User_6) < 0);
				while(rte_ring_dequeue(Ring_RetriveData7, &Data_User_7) < 0);
				while(rte_ring_dequeue(Ring_RetriveData8, &Data_User_8) < 0);
				while(rte_ring_dequeue(Ring_RetriveData9, &Data_User_9) < 0);
				while(rte_ring_dequeue(Ring_RetriveData10, &Data_User_10) < 0);
				while(rte_ring_dequeue(Ring_RetriveData11, &Data_User_11) < 0);
				while(rte_ring_dequeue(Ring_RetriveData12, &Data_User_12) < 0);
				while(rte_ring_dequeue(Ring_RetriveData13, &Data_User_13) < 0);
				while(rte_ring_dequeue(Ring_RetriveData14, &Data_User_14) < 0);
				while(rte_ring_dequeue(Ring_RetriveData15, &Data_User_15) < 0);
				while(rte_ring_dequeue(Ring_RetriveData16, &Data_User_16) < 0);

				User_1 = rte_pktmbuf_mtod_offset((struct rte_mbuf *)Data_User_1, complex32 *,0 );
				User_2 = rte_pktmbuf_mtod_offset((struct rte_mbuf *)Data_User_2, complex32 *,0 );
				User_3 = rte_pktmbuf_mtod_offset((struct rte_mbuf *)Data_User_3, complex32 *,0 );
				User_4 = rte_pktmbuf_mtod_offset((struct rte_mbuf *)Data_User_4, complex32 *,0 );
				User_5 = rte_pktmbuf_mtod_offset((struct rte_mbuf *)Data_User_5, complex32 *,0 );
				User_6 = rte_pktmbuf_mtod_offset((struct rte_mbuf *)Data_User_6, complex32 *,0 );
				User_7 = rte_pktmbuf_mtod_offset((struct rte_mbuf *)Data_User_7, complex32 *,0 );
				User_8 = rte_pktmbuf_mtod_offset((struct rte_mbuf *)Data_User_8, complex32 *,0 );
				User_9 = rte_pktmbuf_mtod_offset((struct rte_mbuf *)Data_User_9, complex32 *,0 );
				User_10 = rte_pktmbuf_mtod_offset((struct rte_mbuf *)Data_User_10, complex32 *,0 );
				User_11 = rte_pktmbuf_mtod_offset((struct rte_mbuf *)Data_User_11, complex32 *,0 );
				User_12 = rte_pktmbuf_mtod_offset((struct rte_mbuf *)Data_User_12, complex32 *,0 );
				User_13 = rte_pktmbuf_mtod_offset((struct rte_mbuf *)Data_User_13, complex32 *,0 );
				User_14 = rte_pktmbuf_mtod_offset((struct rte_mbuf *)Data_User_14, complex32 *,0 );
				User_15 = rte_pktmbuf_mtod_offset((struct rte_mbuf *)Data_User_15, complex32 *,0 );
				User_16 = rte_pktmbuf_mtod_offset((struct rte_mbuf *)Data_User_16, complex32 *,0 );
				//Precode_processing
				k = 0;
				while(k<4){
					// if(k == 0) data = data1;
					// 	else if (k == 1) data = data2;
					// 	else if (k == 2) data = data3;
					// 	else  data = data4;
					while(data == NULL) {
						usleep(100);
						data = rte_pktmbuf_alloc(mbuf_precode_pool);
					}
					for(i = subcar*N_SYM*N_STS/4*k ; i < subcar*N_SYM*N_STS/4*(k+1); i++){
						X[0] = *(User_1 + i);
						X[1] = *(User_2 + i);
						X[2] = *(User_3 + i);
						X[3] = *(User_4 + i);
						X[4] = *(User_5 + i);
						X[5] = *(User_6 + i);
						X[6] = *(User_7 + i);
						X[7] = *(User_8 + i);
						X[8] = *(User_9 + i);
						X[9] = *(User_10 + i);
						X[10] = *(User_11 + i);
						X[11] = *(User_12 + i);
						X[12] = *(User_13 + i);
						X[13] = *(User_14 + i);
						X[14] = *(User_15 + i);
						X[15] = *(User_16 + i);						
						//dest = rte_pktmbuf_mtod_offset(data, complex32 *, 16*(i-subcar*N_SYM*N_STS/4*k));
						//Matrix_Mult_AVX2_16(h,X,dest);					
					}
					//memcpy(precode_data[k],rte_pktmbuf_mtod_offset(data, complex32 *,0),subcar*N_SYM*N_STS/4*16);
					rte_ring_enqueue(Ring_DatatoRRU,data);
					data = NULL;
					k++;
				}
				// data1 = NULL ;
				// data2 = NULL ;
				// data3 = NULL ;
				// data4 = NULL ;
				data = NULL ;
								
				/*FILE *ts =fopen("precoding_data.txt","w");
					for(i=0;i<4;i++)
						printStreamToFile(precode_data[i],subcar*N_SYM*N_STS/4*16,ts);	*/
				rte_mempool_put(((struct rte_mbuf *)Data_User_1)->pool, Data_User_1);
				rte_mempool_put(((struct rte_mbuf *)Data_User_2)->pool, Data_User_2);
				rte_mempool_put(((struct rte_mbuf *)Data_User_3)->pool, Data_User_3);
				rte_mempool_put(((struct rte_mbuf *)Data_User_4)->pool, Data_User_4);
				rte_mempool_put(((struct rte_mbuf *)Data_User_5)->pool, Data_User_5);
				rte_mempool_put(((struct rte_mbuf *)Data_User_6)->pool, Data_User_6);
				rte_mempool_put(((struct rte_mbuf *)Data_User_7)->pool, Data_User_7);
				rte_mempool_put(((struct rte_mbuf *)Data_User_8)->pool, Data_User_8);
				rte_mempool_put(((struct rte_mbuf *)Data_User_9)->pool, Data_User_9);
				rte_mempool_put(((struct rte_mbuf *)Data_User_10)->pool, Data_User_10);
				rte_mempool_put(((struct rte_mbuf *)Data_User_11)->pool, Data_User_11);
				rte_mempool_put(((struct rte_mbuf *)Data_User_12)->pool, Data_User_12);
				rte_mempool_put(((struct rte_mbuf *)Data_User_13)->pool, Data_User_13);
				rte_mempool_put(((struct rte_mbuf *)Data_User_14)->pool, Data_User_14);
				rte_mempool_put(((struct rte_mbuf *)Data_User_15)->pool, Data_User_15);
				rte_mempool_put(((struct rte_mbuf *)Data_User_16)->pool, Data_User_16);
		}
		
		else {
			Data_Retrive_Loop_count++;
			usleep(1000);
			continue;
		}

		if(Retrive_DPDK_count >= 100)
		{
			quit = 1;
			clock_gettime(CLOCK_REALTIME, &time2);
			time_diff = diff(time1,time2);
			printf("Retrive_DPDK_count = %ld\n", Retrive_DPDK_count);
			printf("Start time # %.24s %ld Nanoseconds \n",ctime(&time1.tv_sec), time1.tv_nsec);
			printf("Stop time # %.24s %ld Nanoseconds \n",ctime(&time2.tv_sec), time2.tv_nsec);
			printf("Running time # %ld.%ld Seconds \n",time_diff.tv_sec, time_diff.tv_nsec);
			printf("GenerateData_Loop1_count = %ld\n", GenerateData_Loop1_count);
			printf("GenerateData_Loop2_count = %ld\n", GenerateData_Loop2_count);
			printf("GenerateData_Loop3_count = %ld\n", GenerateData_Loop3_count);
			printf("GenerateData_Loop4_count = %ld\n", GenerateData_Loop4_count);
			printf("GenerateData_Loop5_count = %ld\n", GenerateData_Loop5_count);
            printf("GenerateData_Loop6_count = %ld\n", GenerateData_Loop6_count);
            printf("GenerateData_Loop7_count = %ld\n", GenerateData_Loop7_count);
            printf("GenerateData_Loop8_count = %ld\n", GenerateData_Loop8_count);
            printf("GenerateData_Loop9_count = %ld\n", GenerateData_Loop9_count);
            printf("GenerateData_Loop10_count = %ld\n", GenerateData_Loop10_count);
            printf("GenerateData_Loop11_count = %ld\n", GenerateData_Loop11_count);
            printf("GenerateData_Loop12_count = %ld\n", GenerateData_Loop12_count);
            printf("GenerateData_Loop13_count = %ld\n", GenerateData_Loop13_count);
            printf("GenerateData_Loop14_count = %ld\n", GenerateData_Loop14_count);
            printf("GenerateData_Loop15_count = %ld\n", GenerateData_Loop15_count);
            printf("GenerateData_Loop16_count = %ld\n", GenerateData_Loop16_count);
			printf("Data_Retrive_Loop_count = %ld\n", Data_Retrive_Loop_count);
			printf("Ring_full_count = %ld\n",Ring_full_count);
			printf("mbuf_full_count = %ld\n",mbuf_full_count);
			free(dest);
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

static int GenerateData_Loop5() 
{
	void *Data_In_GenerateData=NULL;
	while (!quit)
	{
		if (rte_ring_dequeue(Ring_GenerateData5, &Data_In_GenerateData) >= 0 )
		{
			GenDataAndScramble_DPDK(Data_In_GenerateData);
			BCC_encoder_DPDK(Data_In_GenerateData);
			modulate_DPDK(Data_In_GenerateData);
			CSD_encode_DPDK(Data_In_GenerateData);
			rte_ring_enqueue(Ring_RetriveData5, Data_In_GenerateData);
		}
		else 
		{	
			GenerateData_Loop5_count++;
			//usleep(10000);
			continue;
		}
	
	}
	return 0;
}

static int GenerateData_Loop6() 
{
	void *Data_In_GenerateData=NULL;
	while (!quit)
	{
		if (rte_ring_dequeue(Ring_GenerateData6, &Data_In_GenerateData) >= 0 )
		{
			GenDataAndScramble_DPDK(Data_In_GenerateData);
			BCC_encoder_DPDK(Data_In_GenerateData);
			modulate_DPDK(Data_In_GenerateData);
			CSD_encode_DPDK(Data_In_GenerateData);
			rte_ring_enqueue(Ring_RetriveData6, Data_In_GenerateData);
		}
		else 
		{	
			GenerateData_Loop6_count++;
			//usleep(10000);
			continue;
		}
	
	}
	return 0;
}
static int GenerateData_Loop7() 
{
	void *Data_In_GenerateData=NULL;
	while (!quit)
	{
		if (rte_ring_dequeue(Ring_GenerateData7, &Data_In_GenerateData) >= 0 )
		{
			GenDataAndScramble_DPDK(Data_In_GenerateData);
			BCC_encoder_DPDK(Data_In_GenerateData);
			modulate_DPDK(Data_In_GenerateData);
			CSD_encode_DPDK(Data_In_GenerateData);
			rte_ring_enqueue(Ring_RetriveData7, Data_In_GenerateData);
		}
		else 
		{	
			GenerateData_Loop7_count++;
			//usleep(10000);
			continue;
		}
	
	}
	return 0;
}
static int GenerateData_Loop8() 
{
	void *Data_In_GenerateData=NULL;
	while (!quit)
	{
		if (rte_ring_dequeue(Ring_GenerateData8, &Data_In_GenerateData) >= 0 )
		{
			GenDataAndScramble_DPDK(Data_In_GenerateData);
			BCC_encoder_DPDK(Data_In_GenerateData);
			modulate_DPDK(Data_In_GenerateData);
			CSD_encode_DPDK(Data_In_GenerateData);
			rte_ring_enqueue(Ring_RetriveData8, Data_In_GenerateData);
		}
		else 
		{	
			GenerateData_Loop8_count++;
			//usleep(10000);
			continue;
		}
	
	}
	return 0;
}
static int GenerateData_Loop9() 
{
	void *Data_In_GenerateData=NULL;
	while (!quit)
	{
		if (rte_ring_dequeue(Ring_GenerateData9, &Data_In_GenerateData) >= 0 )
		{
			GenDataAndScramble_DPDK(Data_In_GenerateData);
			BCC_encoder_DPDK(Data_In_GenerateData);
			modulate_DPDK(Data_In_GenerateData);
			CSD_encode_DPDK(Data_In_GenerateData);
			rte_ring_enqueue(Ring_RetriveData9, Data_In_GenerateData);
		}
		else 
		{	
			GenerateData_Loop9_count++;
			//usleep(10000);
			continue;
		}
	
	}
	return 0;
}
static int GenerateData_Loop10() 
{
	void *Data_In_GenerateData=NULL;
	while (!quit)
	{
		if (rte_ring_dequeue(Ring_GenerateData10, &Data_In_GenerateData) >= 0 )
		{
			GenDataAndScramble_DPDK(Data_In_GenerateData);
			BCC_encoder_DPDK(Data_In_GenerateData);
			modulate_DPDK(Data_In_GenerateData);
			CSD_encode_DPDK(Data_In_GenerateData);
			rte_ring_enqueue(Ring_RetriveData10, Data_In_GenerateData);
		}
		else 
		{	
			GenerateData_Loop10_count++;
			//usleep(10000);
			continue;
		}
	
	}
	return 0;
}
static int GenerateData_Loop11() 
{
	void *Data_In_GenerateData=NULL;
	while (!quit)
	{
		if (rte_ring_dequeue(Ring_GenerateData11, &Data_In_GenerateData) >= 0 )
		{
			GenDataAndScramble_DPDK(Data_In_GenerateData);
			BCC_encoder_DPDK(Data_In_GenerateData);
			modulate_DPDK(Data_In_GenerateData);
			CSD_encode_DPDK(Data_In_GenerateData);
			rte_ring_enqueue(Ring_RetriveData11, Data_In_GenerateData);
		}
		else 
		{	
			GenerateData_Loop11_count++;
			//usleep(10000);
			continue;
		}
	
	}
	return 0;
}
static int GenerateData_Loop12() 
{
	void *Data_In_GenerateData=NULL;
	while (!quit)
	{
		if (rte_ring_dequeue(Ring_GenerateData12, &Data_In_GenerateData) >= 0 )
		{
			GenDataAndScramble_DPDK(Data_In_GenerateData);
			BCC_encoder_DPDK(Data_In_GenerateData);
			modulate_DPDK(Data_In_GenerateData);
			CSD_encode_DPDK(Data_In_GenerateData);
			rte_ring_enqueue(Ring_RetriveData12, Data_In_GenerateData);
		}
		else 
		{	
			GenerateData_Loop12_count++;
			//usleep(10000);
			continue;
		}
	
	}
	return 0;
}
static int GenerateData_Loop13() 
{
	void *Data_In_GenerateData=NULL;
	while (!quit)
	{
		if (rte_ring_dequeue(Ring_GenerateData13, &Data_In_GenerateData) >= 0 )
		{
			GenDataAndScramble_DPDK(Data_In_GenerateData);
			BCC_encoder_DPDK(Data_In_GenerateData);
			modulate_DPDK(Data_In_GenerateData);
			CSD_encode_DPDK(Data_In_GenerateData);
			rte_ring_enqueue(Ring_RetriveData13, Data_In_GenerateData);
		}
		else 
		{	
			GenerateData_Loop13_count++;
			//usleep(10000);
			continue;
		}
	
	}
	return 0;
}
static int GenerateData_Loop14() 
{
	void *Data_In_GenerateData=NULL;
	while (!quit)
	{
		if (rte_ring_dequeue(Ring_GenerateData14, &Data_In_GenerateData) >= 0 )
		{
			GenDataAndScramble_DPDK(Data_In_GenerateData);
			BCC_encoder_DPDK(Data_In_GenerateData);
			modulate_DPDK(Data_In_GenerateData);
			CSD_encode_DPDK(Data_In_GenerateData);
			rte_ring_enqueue(Ring_RetriveData14, Data_In_GenerateData);
		}
		else 
		{	
			GenerateData_Loop14_count++;
			//usleep(10000);
			continue;
		}
	
	}
	return 0;
}
static int GenerateData_Loop15() 
{
	void *Data_In_GenerateData=NULL;
	while (!quit)
	{
		if (rte_ring_dequeue(Ring_GenerateData15, &Data_In_GenerateData) >= 0 )
		{
			GenDataAndScramble_DPDK(Data_In_GenerateData);
			BCC_encoder_DPDK(Data_In_GenerateData);
			modulate_DPDK(Data_In_GenerateData);
			CSD_encode_DPDK(Data_In_GenerateData);
			rte_ring_enqueue(Ring_RetriveData15, Data_In_GenerateData);
		}
		else 
		{	
			GenerateData_Loop15_count++;
			//usleep(10000);
			continue;
		}
	
	}
	return 0;
}
static int GenerateData_Loop16() 
{
	void *Data_In_GenerateData=NULL;
	while (!quit)
	{
		if (rte_ring_dequeue(Ring_GenerateData16, &Data_In_GenerateData) >= 0 )
		{
			GenDataAndScramble_DPDK(Data_In_GenerateData);
			BCC_encoder_DPDK(Data_In_GenerateData);
			modulate_DPDK(Data_In_GenerateData);
			CSD_encode_DPDK(Data_In_GenerateData);
			rte_ring_enqueue(Ring_RetriveData16, Data_In_GenerateData);
		}
		else 
		{	
			GenerateData_Loop16_count++;
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
	else if(rte_ring_full(Ring_GenerateData5)==0)
	{
		return 5;
	}
	else if(rte_ring_full(Ring_GenerateData6)==0)
	{
		return 6;
	}
	else if(rte_ring_full(Ring_GenerateData7)==0)
	{
		return 7;
	}
	else if(rte_ring_full(Ring_GenerateData8)==0)
	{
		return 8;
	}
	else if(rte_ring_full(Ring_GenerateData9)==0)
	{
		return 9;
	}
	else if(rte_ring_full(Ring_GenerateData10)==0)
	{
		return 10;
	}
	else if(rte_ring_full(Ring_GenerateData11)==0)
	{
		return 11;
	}
	else if(rte_ring_full(Ring_GenerateData12)==0)
	{
		return 12;
	}
	else if(rte_ring_full(Ring_GenerateData13)==0)
	{
		return 13;
	}
	else if(rte_ring_full(Ring_GenerateData14)==0)
	{
		return 14;
	}
	else if(rte_ring_full(Ring_GenerateData15)==0)
	{
		return 15;
	}
	else if(rte_ring_full(Ring_GenerateData16)==0)
	{
		return 16;
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
        case 5:	rte_ring_enqueue(Ring_GenerateData5, Data);
        break;
        case 6:	rte_ring_enqueue(Ring_GenerateData6, Data);
        break;
        case 7:	rte_ring_enqueue(Ring_GenerateData7, Data);
        break;
        case 8:	rte_ring_enqueue(Ring_GenerateData8, Data);
        break;
        case 9:	rte_ring_enqueue(Ring_GenerateData9, Data);
        break;
        case 10: rte_ring_enqueue(Ring_GenerateData10, Data);
        break;
        case 11: rte_ring_enqueue(Ring_GenerateData11, Data);
        break;
        case 12: rte_ring_enqueue(Ring_GenerateData12, Data);
        break;
        case 13: rte_ring_enqueue(Ring_GenerateData13, Data);
        break;
        case 14: rte_ring_enqueue(Ring_GenerateData14, Data);
        break;
        case 15: rte_ring_enqueue(Ring_GenerateData15, Data);
        break;
        case 16: rte_ring_enqueue(Ring_GenerateData16, Data);
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
			   	if(dis_count == interval*16){
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
			    else if(dis_count >= interval*4 && dis_count < interval*5){
			        	if(rte_ring_full(Ring_GenerateData5)==0)
				       		rte_ring_enqueue(Ring_GenerateData5, Data);
			        	else
			        		PutInEmptyRing(Data,n);
		        }
			    else if(dis_count >= interval*5 && dis_count < interval*6){
			        	if(rte_ring_full(Ring_GenerateData6)==0)
				       		rte_ring_enqueue(Ring_GenerateData6, Data);
			        	else
			        		PutInEmptyRing(Data,n);
		        }
			    else if(dis_count >= interval*6 && dis_count < interval*7){
			        	if(rte_ring_full(Ring_GenerateData7)==0)
				       		rte_ring_enqueue(Ring_GenerateData7, Data);
			        	else
			        		PutInEmptyRing(Data,n);
		        }
			    else if(dis_count >= interval*7 && dis_count < interval*8){
			       	 	if(rte_ring_full(Ring_GenerateData8)==0)
				      	 	rte_ring_enqueue(Ring_GenerateData8, Data);
			        	else
			        		PutInEmptyRing(Data,n);
		        }
		        else if(dis_count >= interval*8 && dis_count < interval*9){
			        	if(rte_ring_full(Ring_GenerateData9)==0)
				       		rte_ring_enqueue(Ring_GenerateData9, Data);
			        	else
			        		PutInEmptyRing(Data,n);
		        }
		        else if(dis_count >= interval*9 && dis_count < interval*10){
			        	if(rte_ring_full(Ring_GenerateData10)==0)
				       		rte_ring_enqueue(Ring_GenerateData10, Data);
			        	else
			        		PutInEmptyRing(Data,n);
		        }
		        else if(dis_count >= interval*10 && dis_count < interval*11){
			        	if(rte_ring_full(Ring_GenerateData11)==0)
				       		rte_ring_enqueue(Ring_GenerateData11, Data);
			        	else
			        		PutInEmptyRing(Data,n);
		        }
		        else if(dis_count >= interval*11 && dis_count < interval*12){
			        	if(rte_ring_full(Ring_GenerateData12)==0)
				       		rte_ring_enqueue(Ring_GenerateData12, Data);
			        	else
			        		PutInEmptyRing(Data,n);
		        }
		        else if(dis_count >= interval*12 && dis_count < interval*13){
			        	if(rte_ring_full(Ring_GenerateData13)==0)
				       		rte_ring_enqueue(Ring_GenerateData13, Data);
			        	else
			        		PutInEmptyRing(Data,n);
		        }
		        else if(dis_count >= interval*13 && dis_count < interval*14){
			        	if(rte_ring_full(Ring_GenerateData14)==0)
				       		rte_ring_enqueue(Ring_GenerateData14, Data);
			        	else
			        		PutInEmptyRing(Data,n);
		        }
		        else if(dis_count >= interval*14 && dis_count < interval*15){
			        	if(rte_ring_full(Ring_GenerateData15)==0)
				       		rte_ring_enqueue(Ring_GenerateData15, Data);
			        	else
			        		PutInEmptyRing(Data,n);
		        }
		        else if(dis_count >= interval*15 && dis_count < interval*16){
			        	if(rte_ring_full(Ring_GenerateData16)==0)
				       		rte_ring_enqueue(Ring_GenerateData16, Data);
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
	Ring_GenerateData5 = rte_ring_create(GenerateData5 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);
	Ring_GenerateData6 = rte_ring_create(GenerateData6 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);
	Ring_GenerateData7 = rte_ring_create(GenerateData7 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);
	Ring_GenerateData8 = rte_ring_create(GenerateData8 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);
	Ring_GenerateData9 = rte_ring_create(GenerateData9 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);
	Ring_GenerateData10 = rte_ring_create(GenerateData10 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);
	Ring_GenerateData11 = rte_ring_create(GenerateData11 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);
	Ring_GenerateData12 = rte_ring_create(GenerateData12 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);
	Ring_GenerateData13 = rte_ring_create(GenerateData13 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);
	Ring_GenerateData14 = rte_ring_create(GenerateData14 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);
	Ring_GenerateData15 = rte_ring_create(GenerateData15 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);
	Ring_GenerateData16 = rte_ring_create(GenerateData16 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);

	Ring_RetriveData1 = rte_ring_create(RetriveData1 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);
	Ring_RetriveData2 = rte_ring_create(RetriveData2 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);
	Ring_RetriveData3 = rte_ring_create(RetriveData3 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);
	Ring_RetriveData4 = rte_ring_create(RetriveData4 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);
	Ring_RetriveData5 = rte_ring_create(RetriveData5 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);
	Ring_RetriveData6 = rte_ring_create(RetriveData6 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);
	Ring_RetriveData7 = rte_ring_create(RetriveData7 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);
	Ring_RetriveData8 = rte_ring_create(RetriveData8 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);
	Ring_RetriveData9 = rte_ring_create(RetriveData9 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);
	Ring_RetriveData10 = rte_ring_create(RetriveData10 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);
	Ring_RetriveData11 = rte_ring_create(RetriveData11 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);
	Ring_RetriveData12 = rte_ring_create(RetriveData12 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);
	Ring_RetriveData13 = rte_ring_create(RetriveData13 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);
	Ring_RetriveData14 = rte_ring_create(RetriveData14 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);
	Ring_RetriveData15 = rte_ring_create(RetriveData15 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);
	Ring_RetriveData16 = rte_ring_create(RetriveData16 , ring_size, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);

	Ring_DatatoRRU = rte_ring_create(DatatoRRU, 256, rte_socket_id(), RING_F_SP_ENQ|RING_F_SC_DEQ);;

	if (Ring_GenerateData1 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_GenerateData2 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_GenerateData3 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_GenerateData4 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_GenerateData5 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_GenerateData6 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_GenerateData7 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_GenerateData8 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_GenerateData9 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_GenerateData10 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_GenerateData11 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_GenerateData12 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_GenerateData13 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_GenerateData14 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_GenerateData15 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_GenerateData16 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_RetriveData1 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_RetriveData2 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_RetriveData3 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_RetriveData4 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_RetriveData4 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_RetriveData5 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_RetriveData6 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_RetriveData7 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_RetriveData8 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_RetriveData9 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_RetriveData10 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_RetriveData11 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_RetriveData12 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_RetriveData13 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_RetriveData14 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_RetriveData15 == NULL)
		rte_exit(EXIT_FAILURE, "Problem getting sending ring\n");
	if (Ring_RetriveData16 == NULL)
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
	rte_eal_remote_launch(GenerateData_Loop5, NULL,6);
	rte_eal_remote_launch(GenerateData_Loop6, NULL,7);
	rte_eal_remote_launch(GenerateData_Loop7, NULL,8);
	rte_eal_remote_launch(GenerateData_Loop8, NULL,9);
	rte_eal_remote_launch(GenerateData_Loop9, NULL,10);
	rte_eal_remote_launch(GenerateData_Loop10, NULL,11);
	rte_eal_remote_launch(GenerateData_Loop11, NULL,12);
	rte_eal_remote_launch(GenerateData_Loop12, NULL,13);
	rte_eal_remote_launch(GenerateData_Loop13, NULL,14);
	rte_eal_remote_launch(GenerateData_Loop14, NULL,15);
	rte_eal_remote_launch(GenerateData_Loop15, NULL,16);
	rte_eal_remote_launch(GenerateData_Loop16, NULL,17);
	rte_eal_remote_launch(Data_Retrive_Loop, NULL,18);
	rte_eal_remote_launch(Data_sendto_RRU_loop, NULL,19); 
	rte_eal_mp_wait_lcore();
	return 0;
}
#endif // RUN
