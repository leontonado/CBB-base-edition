cmd_maindpdk_test_2.o = gcc -Wp,-MD,./.maindpdk_test_2.o.d.tmp  -m64 -pthread  -march=native -DRTE_MACHINE_CPUFLAG_SSE -DRTE_MACHINE_CPUFLAG_SSE2 -DRTE_MACHINE_CPUFLAG_SSE3 -DRTE_MACHINE_CPUFLAG_SSSE3 -DRTE_MACHINE_CPUFLAG_SSE4_1 -DRTE_MACHINE_CPUFLAG_SSE4_2 -DRTE_MACHINE_CPUFLAG_AES -DRTE_MACHINE_CPUFLAG_PCLMULQDQ -DRTE_MACHINE_CPUFLAG_AVX -DRTE_MACHINE_CPUFLAG_RDRAND -DRTE_MACHINE_CPUFLAG_FSGSBASE -DRTE_MACHINE_CPUFLAG_F16C -DRTE_MACHINE_CPUFLAG_AVX2  -I/home/felix/projects/edition/build/include -I/home/felix/projects/edition/dpdk-stable-16.11.1/x86_64-native-linuxapp-gcc/include -include /home/felix/projects/edition/dpdk-stable-16.11.1/x86_64-native-linuxapp-gcc/include/rte_config.h -O1 -I/home/felix/projects/edition/BCCencode -I/home/felix/projects/edition/IFFT -I/home/felix/projects/edition/intrinsics_interface -I/home/felix/projects/edition/Process -I/home/felix/projects/edition/process_data -I/home/felix/projects/edition/process_data/process_datafunction -I/home/felix/projects/edition/typeDef -I/home/felix/projects/edition/VarINIT -D OPTIMIZATION -D AVX2    -o maindpdk_test_2.o -c /home/felix/projects/edition/maindpdk_test_2.c 
