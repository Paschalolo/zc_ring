/* 
 * Author : Paschal Ahanmisi 
 * Date : October 2026 
 
Copyright (c) 2026 <pastechnology1@gmail.com>

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
#include <utility>
#include <thread>
/* struct definitions */
struct alignas(std::hardware_destructive_interference_size*  2) SPSC_queue{
	__attribute__((aligned(128))) std::atomic<std::uint32_t> head_acutal ; 	


	__attribute__((aligned(128))) std::atomic<std::uint32_t> tail_acutal ; 

	/* For consumers to take work from others */ 
	int work_steal();
};


/* These are the local variables of each queue in a thread */ 
/* note each queue can represent up to 16Gb of data at a 4byte per element number goes up as the data size is larger  */
struct __attribute__((packed , aligned(4)))mask_T1{
	unsigned char mask : 8 ;
	std::uint32_t pad : 24;
};

enum class zc_mask_state_prod: unsigned char {
	NO_STATE = 0x00, 
	BUF_GIVING = 0x01 , 
	BUF_WRITING
};
enum class zc_mask_state_cons: unsigned char {
	NO_STATE = 0x00, 
	BUF_READING = 0x01 , 
	BUF_FINSHED = 0x02,
};
static_assert(sizeof(mask_T1) == 4 , "mask size is not 4 ");
struct alignas(64)  zc_ring_local_t {
	void* addr ;
	std::uint16_t thread_id ;
	std::uint16_t m_position ;
	std::uint32_t local_count ;
	std::uint32_t prod_sz;
	mask_T1 mask1 ;
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

	T*  zc_get_push_buffer( zc_ring_local_t* local_dat , size_t  count  ) noexcept;
	int zc_push(zc_ring_local_t* local_d  ) noexcept; 
	int zc_push_bulk(zc_ring_local_t* local_d  ) noexcept ;

	const T* zc_pop_bulk(zc_ring_local_t* local_data , size_t  count ) noexcept;
	const T*  zc_pop(zc_ring_local_t* local_data ) noexcept ;
	int zc_pop_read(zc_ring_local_t* local_data)noexcept; 
	~zc_ring();
	private : 
		/* this will have to add to the global atomic vairables */
		int add_buffer(zc_ring_local_t*);
		int add_pop_data(zc_ring_local_t*);
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
	// // implement the queusel
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
		new (local_var) zc_ring_local_t{addr[id] , id , 0 , 0 ,prod_sz , {0,0}};
		m_queues[id].head_actual.store(0 , std::memory_order_release);
	}else if constexpr (B  == ZC_QUEUE_TYPE::CONSUMER){
		auto id = generate_thread_id_cons<CONSUMER- 1 >();	
		new (local_var) zc_ring_local_t{addr[id] , id , 0 , 0 , prod_sz , {0, 0} };
		m_queues[id].tail_actual.store(0 , std::memory_order_release);
		
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
T* zc_ring<T,PRODUCER,CONSUMER , BATCH_SIZE>::zc_get_push_buffer( zc_ring_local_t* local_dat , size_t count ) noexcept{
	/* check if the buffer is ready for push */
	auto amount = count * sizeof(T);
	if((amount  & (BATCH_SIZE -1 ))){

		 count = (BATCH_SIZE -1 ) &  (BATCH_SIZE + amount)  ;
	}

	if(local_dat->local_count == 0 ){
		add_buffer(local_dat);
	}

	if(amount > local_dat->local_count * sizeof(T) ) {
		if(add_buffer(local_dat) == -1 ) {
			return nullptr ;
		}
	}

	void* addr1 =  reinterpret_cast<unsigned char*>(local_dat->addr) + (static_cast<size_t>(local_dat->m_position) * 64ULL);
	//possible race here;
	//.* Watch out for overflow and overwriitng written to buffer*/
	local_dat->mask1.mask = std::to_underlying(zc_mask_state_prod::BUF_GIVING);
	local_dat->m_position = local_dat->m_position + (count / BATCH_SIZE); /* expensive division operations*/
	m_queues[local_dat->thread_id].head_actual.store(local_dat->m_position , std::memory_order_release);
	return addr1;
}
	

template <typename T , size_t PRODUCER , size_t CONSUMER , size_t BATCH_SIZE >
int zc_ring<T ,  PRODUCER,CONSUMER , BATCH_SIZE>::zc_push(zc_ring_local_t* local_d )noexcept{
	return 	zc_push_bulk(local_d);
}
template <typename T , size_t PRODUCER , size_t CONSUMER , size_t BATCH_SIZE >
int zc_ring<T ,  PRODUCER,CONSUMER , BATCH_SIZE>::zc_push_bulk(zc_ring_local_t* local_d  )noexcept {

		// add mask to block if buffer not pushed but donated and is holding it 
		
		local_d->mask1.mask = std::to_underlying(zc_mask_state_prod::BUF_WRITING);
		return 0 ;
}

// asynchronous is hard 
// // this currently will only work for batch data will get gibberush on normal count 
template <typename T , size_t PRODUCER , size_t CONSUMER , size_t BATCH_SIZE >
const T* zc_ring<T ,  PRODUCER,CONSUMER , BATCH_SIZE>::zc_pop_bulk(zc_ring_local_t* local_data  , size_t count  ) noexcept{
	/* Be careful to check when buffer is empty or roll over is needed qand other stuff */	
	auto amount = count * sizeof(T);
	if((amount  & (BATCH_SIZE -1 ))){

		 count = (BATCH_SIZE -1 ) &  (BATCH_SIZE + amount)  ;
	}
	if( local_data->local_count == 0 ) {
	if(add_pop_data(local_data) == -1 ){
		return nullptr;
	}	
	}
	T* temp_addr =reinterpret_cast<T*>( reinterpret_cast<unsigned char*>(addr[local_data->thread_id]) + (local_data->m_position * BATCH_SIZE));
	local_data->m_position = local_data->m_position + (amount / BATCH_SIZE); 
	m_queues[local_data->thread_id].tail_actual.store(local_data->m_position , std::memory_order_release);
	local_data->mask1.mask = std::to_underlying(zc_mask_state_cons::BUF_READING);	
	return temp_addr ;
}


template <typename T , size_t PRODUCER , size_t CONSUMER , size_t BATCH_SIZE >
const T*  zc_ring<T ,  PRODUCER,CONSUMER , BATCH_SIZE>::zc_pop(zc_ring_local_t* local_data ) noexcept{

	return zc_pop_bulk(local_data , 1 );
} 
	
template <typename T , size_t PRODUCER , size_t CONSUMER , size_t BATCH_SIZE >
int zc_ring<T ,  PRODUCER,CONSUMER , BATCH_SIZE>::zc_pop_read(zc_ring_local_t* local_data)noexcept{
	local_data->mask1.mask = std::to_underlying(zc_mask_state_cons::BUF_FINSHED);	
	return 0;
}
// MAIN 
int main(){
	zc_ring<int , 2ULL , 2ULL , 64> my_queue{4096ULL};
	
	std::jthread th1{
		[&my_queue](){
			zc_ring_local_t local_data ;
			my_queue.register_queue<ZC_QUEUE_TYPE::PRODUCER>(&local_data);
			auto ptr = my_queue.zc_get_push_buffer(&local_data,16 );
			for(auto i{0uz} ; i < 16 ; i++){
				ptr[i] = static_cast<int>(i) * 100 ; 	
			}
			my_queue.zc_push_bulk(&local_data);
		}
	};
	std::jthread th2{

		[&my_queue](){
			zc_ring_local_t local_data ;
			my_queue.register_queue<ZC_QUEUE_TYPE::PRODUCER>(&local_data);
		}
	};
	std::jthread th3{
		[&my_queue](){
			zc_ring_local_t local_data ;
			my_queue.register_queue<ZC_QUEUE_TYPE::CONSUMER>(&local_data);
		}
	};
	std::jthread th4{
		[&my_queue](){
			zc_ring_local_t local_data ;
			my_queue.register_queue<ZC_QUEUE_TYPE::CONSUMER>(&local_data);
		}
	};
	return 0 ;
}
