/* 
 * Author : Paschal Ahanmisi 
 * Date : October 2026 
 
Copyright (c) 2025 <pastechnology1@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
 */


#include <atomic>
#include <cstdint>
#include <new>
#include <sys/mman.h>
#include <cstdlib>
#include <cstdio>
#include <type_traits>

/* struct definitions */
struct  zc_ring_local_t; 
struct alignas(std::hardware_destructive_interference_size*  2) SPSC_queue{
	__attribute__((aligned(128))) std::atomic<std::uint32_t> head_acutal ; 	
	 std::atomic<std::uint32_t> head_processing ; 


	__attribute__((aligned(128))) std::atomic<std::uint32_t> tail_acutal ; 	
	std::atomic<std::uint32_t> tail_processing; 
	bool empty(zc_ring_local_t* local_data);
	int work_steal(zc_ring_local_t* local_data);
};


/* These are the local variables of each queue in a thread */ 
/* note each queue can represent up to 16Gb of data at a 4byte per element number goes up as the data size is larger  */
struct alignas(64)  zc_ring_local_t {
	void* addr ;
	std::uint16_t thread_id ;
	std::uint16_t mask ;
	std::uint32_t local_count ;
	std::uint32_t prod_sz;
};

enum class ZC_QUEUE_TYPE : unsigned char  {
	PRODUCER  = 0x00 , 
	CONSUMER = 0x01
};

/* Producer and conbsunmers must be counted on proper multiples of 2 */
template <typename T , size_t PRODUCER , size_t CONSUMER , size_t BATCH_SIZE = 64 > 
struct zc_ring{
	SPSC_queue m_queues[PRODUCER];
	alignas(std::hardware_destructive_interference_size) void* addr[PRODUCER];
	size_t capacity{0};
	size_t prod_sz{0};
	zc_ring(size_t size);

	template <ZC_QUEUE_TYPE  B>
	int register_queue(zc_ring_local_t* local_var)noexcept;

	T*  zc_get_push_buffer(T* data , zc_ring_local_t* local_dat , size_t  count  ) noexcept;
	int zc_pop( T* data ) noexcept ;
	int zc_push_written(); 
	int zc_pop_read(); 

	int zc_push_bulk(T* data , size_t count ) noexcept ;
	int zc_push_bulk_written(); 
	int zc_pop_bulk(const T* data , size_t count ) noexcept;
	int zc_pop_bulk_read();
	~zc_ring();
};

// FUNCTIONS AND HELPERS

std::uint16_t generate_thread_id_prod(){
	static std::atomic<std::uint16_t> _th_id1{0};
	auto value = _th_id1.load(std::memory_order_acquire);
	_th_id1.fetch_add(1 ,std::memory_order_release );
	return value ;
}

template <size_t  P>
std::uint16_t generate_thread_id_cons(){
	static std::atomic<std::uint16_t> _th_id2{0};
	auto value = _th_id2.load(std::memory_order_acquire);
	_th_id2.fetch_add(1 ,std::memory_order_release );
	return value  & P;
}

__attribute__((noinline , cold))void tear_down(){
	std::fprintf(stderr , "Failed to insitialize quuee\n");
	exit(1);
}


// 2. Your template now works perfectly
template <typename T, size_t PRODUCER, size_t CONSUMER, size_t BATCH_SIZE>                                               
zc_ring<T, PRODUCER, CONSUMER , BATCH_SIZE>::zc_ring(size_t size) : capacity(size){
		
	size_t sz ;
	if(prod_sz == 0 ){
	 sz = (capacity / PRODUCER) ;
	 sz = (4096 + sz )  & (~(4096ULL-1ULL)) ; /* give a properly aligned size */ 
		prod_sz = sz ;
	}else {
		sz = prod_sz;
	}
	// include fuctionlaity hear for huge paegs
	//
	// // implement the queuse
	 int ret ; 
	for(size_t i {0} ; i <PRODUCER ; i++){
		auto addr1 = mmap(nullptr , sz , PROT_READ| PROT_WRITE ,MAP_SHARED | MAP_ANONYMOUS | MAP_LOCKED ,-1 , 0 );
		if(addr == MAP_FAILED ) {	
			tear_down();
		}
		 ret = mlock(  addr1, sz );
		if(ret == -1 ) {tear_down();}
		addr[i] = addr1;  
	}
			
}

/* Function only callable once in a fucntion else undefined behaviour*/
template <typename T , size_t PRODUCER , size_t CONSUMER , size_t BATCH_SIZE > 
template<ZC_QUEUE_TYPE  B>
int zc_ring<T, PRODUCER,  CONSUMER, BATCH_SIZE>::register_queue(zc_ring_local_t* local_var)noexcept{
	if constexpr (B == ZC_QUEUE_TYPE::PRODUCER){
		auto id = generate_thread_id_prod();
		new (local_var) zc_ring_local_t{addr[id] , id , 0 , 0 ,prod_sz };
	}else if constexpr (B  == ZC_QUEUE_TYPE::CONSUMER){
		auto id = generate_thread_id_cons<CONSUMER- 1 >();	
		new (local_var) zc_ring_local_t{addr[id] , id , 0 , 0 , prod_sz };
		
	}else {
		static_assert(false, "This is not an enum type" );
	}

	return 0;
}



template <typename T , size_t PRODUCER , size_t CONSUMER , size_t BATCH_SIZE > 
zc_ring<T, PRODUCER , CONSUMER , BATCH_SIZE>::~zc_ring(){
	
	for(size_t i {0} ; i < PRODUCER ; i++){
		munmap(addr[i], prod_sz);
	}
}


template <typename T , size_t PRODUCER , size_t CONSUMER , size_t BATCH_SIZE >
T* zc_ring<T,PRODUCER,CONSUMER , BATCH_SIZE>::zc_get_push_buffer(T* data , zc_ring_local_t* local_dat , size_t count ) noexcept{
	if(local_dat->local_count == 0 ) {
		
	}	

}
	


template <typename T , size_t PRODUCER , size_t CONSUMER , size_t BATCH_SIZE >
zc_ring<T ,  PRODUCER,CONSUMER , BATCH_SIZE>::zc_pop( ){}
template <typename T , size_t PRODUCER , size_t CONSUMER , size_t BATCH_SIZE >
zc_ring<T ,  PRODUCER,CONSUMER , BATCH_SIZE>::zc_push_written(){}
template <typename T , size_t PRODUCER , size_t CONSUMER , size_t BATCH_SIZE >
zc_ring<T ,  PRODUCER,CONSUMER , BATCH_SIZE>::zc_pop_read(){}
template <typename T , size_t PRODUCER , size_t CONSUMER , size_t BATCH_SIZE >
zc_ring<T ,  PRODUCER,CONSUMER , BATCH_SIZE>::zc_push_bulk(){}
template <typename T , size_t PRODUCER , size_t CONSUMER , size_t BATCH_SIZE >
zc_ring<T ,  PRODUCER,CONSUMER , BATCH_SIZE>::zc_push_bulk_written(){}
template <typename T , size_t PRODUCER , size_t CONSUMER , size_t BATCH_SIZE >
zc_ring<T ,  PRODUCER,CONSUMER , BATCH_SIZE>::zc_pop_bulk(){}
template <typename T , size_t PRODUCER , size_t CONSUMER , size_t BATCH_SIZE >
zc_ring<T ,  PRODUCER,CONSUMER , BATCH_SIZE>::zc_pop_bulk_read(){}
/* 
template <typename T , size_t PRODUCER , size_t CONSUMER , size_t BATCH_SIZE > 
template <typename T , size_t PRODUCER , size_t CONSUMER , size_t BATCH_SIZE > 
template <typename T , size_t PRODUCER , size_t CONSUMER , size_t BATCH_SIZE > 

*/
// MAIN 
int main(){
	return 0 ;
}
