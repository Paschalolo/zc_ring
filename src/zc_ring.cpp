
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
	void* m_remote_addr_ ;
	void* m_addr_ ; /* This contains 16 bits of local_write_left and conatins the addr to the pointer to the interal queue*/ 
	std::uint64_t m_local_curr_write_;
	std::uint16_t m_local_id_and_work_mode_ ;/* contains localid and the work mode */
}; 
struct __attribute__(( aligned(64))) local_cons_data_t{
	void* ma_remote_addr_ ; /* contians the mask of the work steal mode and the remote address*/
	void* m_addr_ ; /* This contains 16 bits of local_read_left and conatins the addr to the pointer to the interal queue*/ 
	std::uint64_t m_local_curr_read_;
	std::uint16_t m_local_id_and_work_mode_ ;/* contains localid and the work mode */
};


static_assert(sizeof(local_cons_data_t) == 64, "Sizeof local_cons_data_t is not 64" );
static_assert(sizeof(local_prod_data_t) == 64, "Sizeof local_prod_data_t is not 64" );


struct  spsc_queue{

	/* PRODUCER*/ 
	alignas(std::hardware_destructive_interference_size * 2 )std::atomic<std::uint64_t> head_producer ; 

	/* values local to the producer */ 
	local_prod_data_t m_prod_data_loc_; /* This is the local producer data */

	/* CONSUMER */
	alignas(std::hardware_destructive_interference_size* 2 )std::atomic<std::uint64_t> tail_consumer ; 
	local_cons_data_t m_cons_data_loc ;

	
	
	/* For consumers to take work from others */
	int work_steal() noexcept;

	/* checkign functions */
	bool empty() noexcept ; 
	bool full(); 
};

/* is a multiple of 8*/
template <size_t S >
concept is_multiple_of_8 = S % 8 == 0 ;
/* zc_ring
*/
template <typename T , size_t P /*PRODUCER*/, size_t C /* CONSUMER*/ , size_t SZ /*SIZE OF BUFFER */ ,
	 size_t BS = 64 /*BATCH_SIZE*/>
requires is_multiple_of_8<SZ>
struct zc_ring{
	public : 

		zc_ring()  ;
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
		constexpr static inline size_t get_size(){return SZ;}
	private :
		/*spsc queue is aligned on a hardware destructive size boundary * 2 to aviod false sharing agressive prefetching and cache line contention */
		spsc_queue m_queue_[P];
		alignas(64) void* m_addrs_[P];
		size_t m_single_buf_sz_;

};

template <typename T , size_t SZ>
struct alignas(64) Payload{
	std::uint32_t m_sz_;
	alignas(alignof(T)) T m_data[SZ];
	
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
	if(static_cast<size_t>(value) >= P ){
		_th_id2.store(0 ,std::memory_order_release);
		return 0;
	}
	_th_id2.fetch_add(1 ,std::memory_order_release );
	return value ; 
}

__attribute__((noinline , cold))void tear_down(){
	std::fprintf(stderr , "Failed to insitialize quuee\n");
	exit(1);
}


template <typename T, size_t P, size_t C, size_t SZ , size_t BS>  
requires is_multiple_of_8<SZ>
zc_ring<T, P, C , SZ , BS>::zc_ring(){
}

template <typename T, size_t P, size_t C, size_t SZ , size_t BS>                                               
int zc_ring<T, PRODUCER,  CONSUMER, BATCH_SIZE>::register_queue(){
}



template <typename T, size_t P, size_t C, size_t SZ , size_t BS>                                               
zc_ring<T, PRODUCER , CONSUMER , BATCH_SIZE>::~zc_ring(){
	
	for(size_t i {0} ; i < PRODUCER ; i++){
		munmap(addr[i], prod_sz);
	}
}


template <typename T, size_t P, size_t C, size_t SZ , size_t BS>                                               
template <typename T , size_t PRODUCER , size_t CONSUMER , size_t BATCH_SIZE >
T* zc_ring<T,PRODUCER,CONSUMER , BATCH_SIZE>::zc_get_push_buffer( zc_ring_local_t* local_dat , size_t count ) {
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
	

template <typename T, size_t P, size_t C, size_t SZ , size_t BS>                                               
template <typename T , size_t PRODUCER , size_t CONSUMER , size_t BATCH_SIZE >
int zc_ring<T ,  PRODUCER,CONSUMER , BATCH_SIZE>::zc_push(zc_ring_local_t* local_d ){
	return 	zc_push_bulk(local_d);
}
template <typename T , size_t PRODUCER , size_t CONSUMER , size_t BATCH_SIZE >
template <typename T, size_t P, size_t C, size_t SZ , size_t BS>                                               
int zc_ring<T ,  PRODUCER,CONSUMER , BATCH_SIZE>::zc_push_bulk(zc_ring_local_t* local_d  ) {

		// add mask to block if buffer not pushed but donated and is holding it 
		
		local_d->mask1.mask = std::to_underlying(zc_mask_state_prod::BUF_WRITING);
		return 0 ;
}

// asynchronous is hard 
// // this currently will only work for batch data will get gibberush on normal count 
template <typename T , size_t PRODUCER , size_t CONSUMER , size_t BATCH_SIZE >
template <typename T, size_t P, size_t C, size_t SZ , size_t BS>                                               
const T* zc_ring<T ,  PRODUCER,CONSUMER , BATCH_SIZE>::zc_pop_bulk(zc_ring_local_t* local_data  , size_t count  ) {
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
template <typename T, size_t P, size_t C, size_t SZ , size_t BS>                                               
const T*  zc_ring<T ,  PRODUCER,CONSUMER , BATCH_SIZE>::zc_pop(zc_ring_local_t* local_data ) {

	return zc_pop_bulk(local_data , 1 );
} 
	
template <typename T , size_t PRODUCER , size_t CONSUMER , size_t BATCH_SIZE >
template <typename T, size_t P, size_t C, size_t SZ , size_t BS>                                               
int zc_ring<T ,  PRODUCER,CONSUMER , BATCH_SIZE>::zc_pop_read(zc_ring_local_t* local_data){
	local_data->mask1.mask = std::to_underlying(zc_mask_state_cons::BUF_FINSHED);	
	return 0;
}
// MAIN
int main(){
	return 0 ;
}
