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
struct __attribute__((aligned(64))) local_prod_data_t{
	void* remote_addr ;
	void* addr ; /* This contains 16 bits of local_write_left and conatins the addr to the pointer to the interal queue*/ 
	std::uint64_t local_curr_write;
	std::uint16_t local_id_and_work_mode ;/* contains localid and the work mode */
}; 
struct __attribute__(( aligned(64))) local_cons_data_t{
	void* ma_remote_addr_ ; /* contians the mask of the work steal mode and the remote address*/
	void* addr ; /* This contains 16 bits of local_read_left and conatins the addr to the pointer to the interal queue*/ 
	std::uint64_t local_curr_read;
	std::uint16_t local_id_and_work_mode ;/* contains localid and the work mode */
};


static_assert(sizeof(local_cons_data_t) == 64, "Sizeof local_cons_data_t is not 64" );
static_assert(sizeof(local_prod_data_t) == 64, "Sizeof local_prod_data_t is not 64" );


struct  spsc_queue{

	/* PRODUCER*/ 
	alignas(std::hardware_destructive_interference_size * 2 )std::atomic<std::uint64_t> head_producer ; 

	/* values local to the producer */ 
	local_prod_data_t prod_data_loc_; /* This is the local producer data */

	/* CONSUMER */
	alignas(std::hardware_destructive_interference_size* 2 )std::atomic<std::uint64_t> tail_consumer ; 
	local_cons_data_t cons_data_loc ;

	
	
	/* For consumers to take work from others */
	int work_steal() noexcept;

	/* checkign functions */
	bool empty() noexcept ; 
	bool full(); 
};


enum class ZC_QUEUE_TYPE : unsigned char  {
	PRODUCER  = 0x00 , 
	CONSUMER = 0x01
};


/* zc_ring 
 * P -> PRODUCER 
 *C -> CONSUMER 
 * BS -> BATCH_SIZE
*/
template <typename T , size_t P, size_t C , size_t BS = 64 > 
struct zc_ring{
	public : 

		zc_ring() = default ;
		~zc_ring();
	/* Methods */ 
		/* personalized setup for each queue */
		int init_queue();
		/* this returns a buffer that values can be written to 
		 * This enusres no copies are made on data transfer 
		 * This saves space and reduces instructions count spent on copying */ 
		T* get_prod_write_buffer(size_t sz );
		/* This pushes the bulk to the queue */
		int push_bulk(size_t sz);

		__attribute__((always_inline)) inline int push(){
			return push_bulk(1);
		}
		/* This gives the buffer that conatins the values 
		 * Owners of consumers can decide to wait as long as possible to do stuff with the buffer 
		 * NOTE delayed time with the buffer will lead to blocking and that queue will not be able to move on*/
		int pop_get_buffer(const T** buf);
		
		/* This frees up the space in the buffer for write to begin*/
		int pop_bulk(size_t sz );

		__attribute__((always_inline)) inline  int pop(){
			return pop_bulk(1);
		}

		/* Static methods */ 
		constexpr static inline size_t get_batch_size() {
			return  BS;
		}
		constexpr static inline size_t get_producer_count(){
			return P;
		}
		constexpr static inline size_t get_consumer_count(){
			return C;
		}
	private : 

};

// FUNCTIONS AND HELPERS

std::uint16_t generate_thread_id_prod(){
	static std::atomic<std::uint16_t> _th_id1{0};
	auto value = _th_id1.load(std::memory_order_acquire);
	_th_id1.fetch_add(1 ,std::memory_order_release );
	return value ;
}

template <size_t P  >
std::uint16_t generate_thread_id_cons(){
	static std::atomic<std::uint16_t> _th_id2{0};
	auto value = _th_id2.load(std::memory_order_acquire);
	_th_id2.fetch_add(1 ,std::memory_order_release );
	return value  & (P-1);
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
