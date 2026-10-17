
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

#include <concepts>
#include <cstddef>
#include <sched.h>
#include <stdatomic.h>
#include <unistd.h>
#include <linux/mman.h>
#include <atomic>
#include <cstdint>
#include <new>
#include <sys/mman.h>
#include <cstdlib>
#include <cstdio>
#include <type_traits>
#include <utility>
#include <thread>

/* Mask needed */ 
constexpr size_t MASK_63 {63ULL};
constexpr size_t MASK_4095 {4096ULL - 1ULL};
constexpr size_t MASK_2MB {(2ULL * 1024ULL * 1024ULL) - 1ULL};
constexpr size_t MASK_1GB {(1ULL * 1024ULL * 1024ULL * 1024ULL) - 1ULL};
constexpr size_t MASK_48bits {0xFFFFFFFFFFFFULL};
constexpr size_t MASK_FR16bits {0xFFFF000000000000ULL};
constexpr std::uint16_t MASK_LOCAL_ID {0xFF};
constexpr std::uint16_t MASK_WORKMODE {0xFF00};
/* Enum Definitions */ 
enum class ZC_TYPE {
	PROD , CONSU,
};

/* Mask states for local_data  
 * It wil tell if teh producer or consumer is in a ready , empty or full state locally not globally  
 * */
enum class local_state_mask : std::uint8_t {
	EMPTY = 0x00 , 
	FULL = 0x01, 
	READY  =0x02,
};

enum class spsc_state : std::uint8_t {
	NO_WORK_STEAL, WORK_STEAL
};


/* struct definitions */
struct __attribute__((aligned(64))) local_prod_data_t{
	void* m_remote_addr_ ;
	void* m_addr_ ; /* This contains 16 bits of local_write_left and conatins the addr to the pointer to the interal queue*/ 
	std::uint32_t m_local_curr_write_;
	std::uint32_t m_local_write_left_;
	std::uint16_t m_local_id_and_work_mode_ ;/* contains localid and the work mode */
	std::uint8_t m_mask ;/* mask contains bit 0-2 state if mask is emtpty , full , ready*/
}; 
struct __attribute__(( aligned(64))) local_cons_data_t{
	void* ma_remote_addr_ ; /* contians the mask of the work steal mode and the remote address*/
	void* m_addr_ ; /* This contains 16 bits of local_read_left and conatins the addr to the pointer to the interal queue*/ 
	std::uint32_t m_local_curr_read_;
	std::uint32_t m_local_read_left_;
	std::uint16_t m_local_id_and_work_mode_ ;/* contains localid and the work mode */
	std::uint8_t m_mask ; /* mask contains bit 0-2 state if mask is emtpty , full , ready*/ 
};


static_assert(sizeof(local_cons_data_t) == 64, "Sizeof local_cons_data_t is not 64" );
static_assert(sizeof(local_prod_data_t) == 64, "Sizeof local_prod_data_t is not 64" );


struct  spsc_queue{

	/* PRODUCER*/ 
	alignas(std::hardware_destructive_interference_size * 2 )std::atomic<std::uint64_t> head_producer ; 


	/* CONSUMER */
	/* NOTE : We seperate both data because we want to lower contention and latency at synchronization intervals and also reduce cohenrency presure on memoery*/



	/* this represents the position as to in which a new read is available
	 * This is what is displayed to work stealers to take from to read 
	 * This will be hit by cnosumers who work steal at synchronization 
	 * */
	alignas(std::hardware_destructive_interference_size* 2 )std::atomic<std::uint64_t> tail_consumer ;
	std::atomic<std::uint8_t> m_mask_w ;/* contains mask to show if the current stuff is being stole from */
	/* this represents the local state of the consumer where its actual buffer has been read and can be reclaimined for writing 
	 * This is what is deisplayed locally to my lcoal count to know how much i have actually read and is freable to use for writing . Hence this will be hit by producers on synchronization time 
	 * */
	alignas(std::hardware_destructive_interference_size * 2 ) std::atomic<std::uint64_t> tail_consumer_actual;
	

	
	

	/* checkign functions */
	bool empty() noexcept ; 
	bool full() noexcept; 
};


/* CONCEPTS */

/* is a multiple of 8*/
template <size_t S >
concept is_multiple_of_8 = S % 8 == 0 ;

template <size_t S> 
concept is_properly_aligned = ((S % 2) == 0 )  && ((S% 4) == 0 ) && (S % 8 ==  0) ;

/* ZC_RING 
 * This Ring must be passed objects that are trivillay destructible passing in complect objects that handles say allocation etc or others will kill throughput by causing a non vectorizable loop to iterate thorough functions before they are used 
 * Hence if non trivial objects are used make sure you destroy the induvidual values before it is popped 
 * The queue will not be responsible for this for latency reasons 
 * i.e could handle it in destroctor or opp using trivially Z_construcbkel and non tribvally constexpr on it 
 * */
template <typename T , size_t P /*PRODUCER*/, size_t C /* CONSUMER*/ , size_t SZ /*SIZE OF BUFFER */ ,
	  size_t BI = 8 /* batch increment i.e BATCH_SIZE * BI is how much a producer will produce or consume */ ,
	  size_t BS = 64 /*BATCH_SIZE*/,
	  size_t CS = 64 /* Cache line size*/ >
requires is_multiple_of_8<SZ> && is_properly_aligned<alignof(T)> && std::is_trivially_destructible_v<T>
struct zc_ring{
	public : 

		zc_ring()  ;
		~zc_ring();
	/* Methods */ 
		/* personalized setup for each queue */

		template <ZC_TYPE A , typename zc_tstruct>/* Registers if this is a producer or consumer\
		It also returns the id in order to be able to find the space or -1 if error  
		*/
		int init_queue(zc_tstruct* data);


		/* this returns a buffer that values can be written to 
		 * This enusres no copies are made on data transfer 
		 * This saves space and reduces instructions count spent on copying */ 
		int  get_prod_write_buffer(T** , local_prod_data_t* data , size_t sz );
		/* This pushes the bulk to the queue */
		int push_bulk(void* addr  , size_t sz);

		__attribute__((always_inline)) inline int push(){
			return push_bulk(1);
		}
		__attribute__((always_inline)) inline int push_empty(){
			return push_bulk(0);
		}
		/* This gives the buffer that conatins the values 
		 * Owners of consumers can decide to wait as long as possible to do stuff with the buffer 
		 * NOTE delayed time with the buffer will lead to blocking and that queue will not be able to move on*/
		int pop_get_buffer(local_cons_data_t* local_data ,const T** buf);
		
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
		
		alignas(std::hardware_destructive_interference_size* 2 ) void* m_addrs_[P];
		std::uint64_t m_prod_size_; /* this stores it in the count of how many batch sizes it has */
	

		/* For consumers to take work from others */
		int work_steal(local_cons_data_t* lcaol_data ) noexcept;

};

template <typename T >
struct alignas(64) Payload{
	std::uint32_t m_sz_; /* here store size and the mask of whether its junk corrupted or it has value */
	std::uint32_t m_actual_data_sz ; /* sizeof the actual data filled here  with bit of mask*/
	alignas(alignof(T)) T m_data;
	
};
// FUNCTIONS AND HELPERS
//================================================================================================ //
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

static inline __attribute__((always_inline))void* extract_addr_from_addr(void* addr ){
	return reinterpret_cast<void*>(reinterpret_cast<std::uint64_t>(addr) & MASK_48bits) ;  
}

static inline __attribute__((always_inline)) void* insert_addr_to_addr(void* current_adr  , void* new_adr ) {
	return reinterpret_cast<void*>((reinterpret_cast<std::uint64_t>(current_adr) & MASK_FR16bits) | (reinterpret_cast<std::uint64_t>(new_adr) & MASK_48bits));
}

static inline __attribute__((always_inline)) std::uint16_t extract_localno_from_addr(void* addr ){
	
	return static_cast<std::uint16_t>((reinterpret_cast<std::uint64_t>(addr) & MASK_FR16bits) >> 48ULL) ;  
}

 static inline __attribute__((always_inline)) void* insert_size_to_addr(void* addr , std::uint16_t sz ) {
	 return reinterpret_cast<void*>((reinterpret_cast<std::uint64_t>(addr) & MASK_48bits ) | (static_cast<std::uint64_t>(sz) << 48ULL ) ) ;
}

static inline __attribute__((always_inline)) std::uint16_t extract_local_id(std::uint16_t value ){
	return value & MASK_LOCAL_ID ;
}

static inline __attribute__((always_inline)) std::uint8_t extract_workmode(std::uint16_t value ){
	 return static_cast<std::uint8_t>((value & MASK_WORKMODE) >> 8 ) ;
}

static inline __attribute__((always_inline)) std::uint16_t insert_local_id(std::uint16_t value , std::uint16_t localid){
	return (value & MASK_WORKMODE) | (localid & MASK_LOCAL_ID) ;
}

 static inline __attribute__((always_inline))  std::uint16_t insert_workmode(std::uint16_t value , std::uint8_t workmode){
	return (value & MASK_LOCAL_ID)  | static_cast<std::uint16_t>((static_cast<std::uint32_t>(workmode) << 8U));
} 



//--------------------------------------------------------




template <typename T, size_t P, size_t C, size_t SZ , size_t BI  , size_t BS , size_t CS >  
requires is_multiple_of_8<SZ> && is_properly_aligned<alignof(T)> && std::is_trivially_destructible_v<T>
zc_ring<T, P, C , SZ ,  BI , BS , CS >::zc_ring(){
	/* Regualr allocation for arays in which producer are less than a page size */
	if constexpr ((SZ/P) <= (1024ULL * 4)) {
		constexpr size_t prod_count = ((SZ/P) + 64ULL)  & ~MASK_63 ;
		m_prod_size_ = prod_count / 64ULL ;
		for(auto i{0uz} ; i < P ; i++){
			auto ptr = aligned_alloc(64, prod_count);
			if(ptr == nullptr) {
				tear_down();
			}
			m_addrs_[i] = ptr;
		}
		
	}else if constexpr((SZ/P) <= (4096ULL * 100))/*for allocation in which producer is less than 400KB*/ {
		constexpr size_t prod_count = ((SZ/P) + 4096ULL)  & ~MASK_4095 ;
		m_prod_size_ = prod_count/64ULL;
		for(auto i{0uz} ; i < P ; i++){
			void* ptr = mmap(nullptr , prod_count ,PROT_READ | PROT_WRITE , MAP_PRIVATE |MAP_ANONYMOUS , -1 , 0 ); 
			if(ptr == MAP_FAILED) {tear_down();}
			m_addrs_[i] = ptr ;
		}


	}else if constexpr((SZ/P) < (500ULL * 1024ULL * 1024ULL) /* 500MB < page sizes are 2MB here */ ){
		
		constexpr size_t prod_count = ((SZ/P) + (2 * 1024 * 1024))  & ~MASK_2MB ;
		m_prod_size_ = prod_count / 64ULL ;
		for(auto i{0uz} ; i < P ; i++){
			void* ptr = mmap(nullptr , prod_count ,PROT_READ | PROT_WRITE , MAP_HUGETLB  | MAP_HUGE_2MB | MAP_PRIVATE |MAP_ANONYMOUS , -1 , 0 ); 
			if(ptr == MAP_FAILED) {tear_down();}
			m_addrs_[i] = ptr ;
		}

	}else if constexpr((SZ/P) >= (500ULL * 1024ULL * 1024ULL))/* 500MB or more page sizes are 1gb here */{

		constexpr size_t prod_count = ((SZ/P) + (1ULL * 1024ULL * 1024ULL * 1024ULL))  & ~MASK_1GB ;
		m_prod_size_ = prod_count / 64ULL ;
		for(auto i{0uz} ; i < P ; i++){
			void* ptr = mmap(nullptr , prod_count ,PROT_READ | PROT_WRITE , MAP_HUGETLB | MAP_HUGE_1GB| MAP_PRIVATE |MAP_ANONYMOUS , -1 , 0 ); 
			if(ptr == MAP_FAILED) {tear_down();}
			m_addrs_[i] = ptr ;
		}
	}
}

template <typename T, size_t P, size_t C, size_t SZ , size_t BI ,  size_t BS , size_t CS >                                               
requires is_multiple_of_8<SZ> && is_properly_aligned<alignof(T)> && std::is_trivially_destructible_v<T>
zc_ring<T, P , C ,SZ , BI ,  BS , CS >::~zc_ring(){
	// for now in shutdown 	to shutdown 
	for(auto i {0uz} ; i < P ; i++){
		munmap(m_addrs_[i], m_prod_size_);
	}
}


template <typename T, size_t P, size_t C, size_t SZ , size_t BI  , size_t BS , size_t CS >  
requires is_multiple_of_8<SZ> && is_properly_aligned<alignof(T)> && std::is_trivially_destructible_v<T>
int zc_ring<T, P, C , SZ ,  BI , BS , CS >::work_steal(local_cons_data_t* local_consumer) noexcept {
	if(!local_consumer) return -1;
	auto id = extract_local_id(local_consumer->m_local_id_and_work_mode_);
	for(auto i{0uz} ; i < P ; i++){
		if(static_cast<std::uint64_t>(id) != i){
			auto state = m_queue_[i].m_mask_w.load(std::memory_order_acquire);

			
			if (state == std::to_underlying(spsc_state::WORK_STEAL)) {goto fin ;}
			auto h_prod = m_queue_[i].head_producer.load(std::memory_order_acquire);	
			auto t_cons =m_queue_[i].tail_consumer.load(std::memory_order_acquire);

			// check empty
			if(h_prod == t_cons){
				sched_yield() ; 
				continue ;
			}
			auto real_cons_no = t_cons & (BI-1);
			auto inc =  (real_cons_no + BI )  < m_prod_size_;
			size_t count ;
			if(inc)/* Hence we steal to the end */ {
				bool t = __builtin_uaddl_overflow(t_cons ,(m_prod_size_ -real_cons_no) , &count );
				if(h_prod < count   || (t && count > h_prod)){
					count = h_prod;
				}	
				bool success =atomic_compare_exchange_strong_explicit(&(m_queue_[i].tail_consumer) , 
						&t_cons , count ,std::memory_order_release , std::memory_order_relaxed );
				if(!success){
					count = 0;	
				}
				//  here we put in the value if succeded	
			}else /* we steal normal */{
				bool t = __builtin_uaddl_overflow(t_cons , BI , &count );
				if((h_prod < count)  || (t && count > h_prod)) {
					count = h_prod ;
				}

				bool success = atomic_compare_exchange_strong_explicit(&(m_queue_[i].tail_consumer), 
						&t_cons , count , std::memory_order_release , std::memory_order_relaxed);
				if(!success) {count = 0 ;}

			}

			if(count !=0 ){
				local_consumer->ma_remote_addr_ = m_addrs_[i];
				local_consumer->m_local_id_and_work_mode_ = insert_workmode(local_consumer->m_local_id_and_work_mode_, 0x01);
				local_consumer->m_local_curr_read_ = count ; 
				local_consumer->m_local_read_left_ = count ;
				return count ;}
			sched_yield();
		}
		
		}
fin : 
	return 0; /* Meaning error for no free lunch to steal */
}


template <typename T, size_t P, size_t C, size_t SZ ,size_t BI ,  size_t BS , size_t CS> 
requires is_multiple_of_8<SZ> && is_properly_aligned<alignof(T)> && std::is_trivially_destructible_v<T>
template <ZC_TYPE A , typename zc_tstruct> 
int zc_ring<T, P , C ,SZ ,  BI , BS , CS>::init_queue(zc_tstruct* data){
	if(!data){
		return -1; /* EROOR */
	}
	if constexpr ((A == ZC_TYPE::PROD) && std::is_same_v<zc_tstruct ,local_prod_data_t > ) {
		auto id = generate_thread_id_prod();
		m_queue_[id].head_producer.store( 0 , std::memory_order_release);
	 	auto ptr = m_addrs_[id];
		new (data) local_prod_data_t{nullptr,insert_size_to_addr(ptr,0) ,0 ,0 , insert_workmode(id,0) , 0 };
		return id; 
	
	}else if constexpr ((A == ZC_TYPE::CONSU) && std::is_same_v<zc_tstruct , local_cons_data_t>) {
		auto id = generate_thread_id_prod();
		m_queue_[id].tail_consumer.store(0, std::memory_order_release);
		m_queue_[id].tail_consumer_actual.store(0, std::memory_order_release);
	 	auto ptr = m_addrs_[id];
		new (data) local_cons_data_t{0 ,insert_size_to_addr(ptr,0) , 0 ,0 , insert_workmode(id,0) , 0 };
		return id ;
	}else {
		static_assert(false, "This type doesnot exit " );
	}
}

// BUGGY m,ight have the local not being added 
template <typename T, size_t P, size_t C, size_t SZ ,size_t BI,  size_t BS , size_t CS> 
requires is_multiple_of_8<SZ> && is_properly_aligned<alignof(T)> && std::is_trivially_destructible_v<T>
int  zc_ring<T, P , C ,SZ , BI , BS , CS>::get_prod_write_buffer(T** data , local_prod_data_t* local_data ,size_t sz ){
	/* cnhage writer m_local number */ 
	/* Resturcture this array udue to high number of if else state,ents*/
	/* check for nullptr */
	if(!data && !local_data){
		return -1 ; /* ERROR */
	}
	constexpr size_t alignmentOfT = alignof(T) < 64 ? alignof(T) : 64 ;
	constexpr size_t payload_sz_alignment = ((sizeof(std::uint32_t) * 2 ) <= alignmentOfT) ? (sizeof(std::uint32_t)* 2 )  : alignmentOfT;
	std::size_t tmp1 = (payload_sz_alignment + (sizeof(T) * sz)) & ~(BS -1) ; 
	std::size_t pyld_aligned_BS_cnt = tmp1 / BS ; 
	auto ptr_buf = local_data->m_addr_ ;
	auto id = extract_local_id(local_data->m_local_id_and_work_mode_);
	std::uint16_t no_buf_space_left /* amount of buffer space left in local*/ = extract_localno_from_addr(ptr_buf);
	/* this should not junk instruction cache and should e a fucntion that is not inline*/ 
	if(no_buf_space_left == 0 ){
		*data = reinterpret_cast<T*>(nullptr);
		if(m_queue_[id].full()){ /*work needded here */
			return -1 ; /* no wrok stealin this is a producer */
		}
		auto new_inc = (sz & (BS-1)) ? (sz + BS) & (~(BS-1)) : sz ;
		new_inc = new_inc / BS ;
		auto inc = (new_inc < BI)  ? BI : new_inc  ;
		std::uint64_t cons_val = m_queue_[id].tail_consumer.load(std::memory_order_acquire);
		auto curr_pos =local_data->m_local_curr_write_ ; 
		if((local_data->m_local_curr_write_  + inc ) > m_prod_size_){
			//add stuff to the buffer and ship it out 
			return 0 ; // return size of stuff ; // set errno flags for watage
		}	
		m_queue_[id].head_producer.fetch_add(inc,std::memory_order_acquire );
		// not finshed 
			
	}else if (no_buf_space_left <= pyld_aligned_BS_cnt ){
		// increment local read left 
		local_data->m_local_write_left_ = 0;
		auto ptr = reinterpret_cast<unsigned char*>(ptr_buf) + (BS * local_data->m_local_curr_write_);
		reinterpret_cast<Payload<T>*>(ptr)->m_sz_ = no_buf_space_left;
		*data = reinterpret_cast<T*>(ptr+ payload_sz_alignment);
		return no_buf_space_left / sizeof(T) ; 
	}

	local_data->m_local_write_left_ =  static_cast<std::uint32_t>(no_buf_space_left- pyld_aligned_BS_cnt) ;
	auto ptr = reinterpret_cast<unsigned char*>(ptr_buf) + (BS * local_data->m_local_curr_write_);
	reinterpret_cast<Payload<T>*>(ptr)->m_sz_ = static_cast<std::uint32_t>(pyld_aligned_BS_cnt) ; 
	*data = reinterpret_cast<T*>(ptr + payload_sz_alignment);	
	return pyld_aligned_BS_cnt / sizeof(T);	


}

// Use proper alignement on your values 
template <typename T, size_t P, size_t C, size_t SZ ,size_t BI,  size_t BS , size_t CS> 
requires is_multiple_of_8<SZ> && is_properly_aligned<alignof(T)> && std::is_trivially_destructible_v<T>
int   zc_ring<T, P , C ,SZ , BI , BS , CS>::push_bulk(void* addr , size_t sz ){
	if(!addr){return -1; /* ERROR */}
	constexpr size_t alignmentOfT = alignof(T) < 64 ? alignof(T) : 64 ;
	constexpr size_t payload_sz_alignment = ((sizeof(std::uint32_t) * 2 ) <= alignmentOfT) ? (sizeof(std::uint32_t)* 2 )  : alignmentOfT;
	*reinterpret_cast<Payload<T>*>(reinterpret_cast<unsigned char*>(addr) - payload_sz_alignment )->m_actual_data_sz = sz;
}


template <typename T, size_t P, size_t C, size_t SZ ,size_t BI,  size_t BS , size_t CS> 
requires is_multiple_of_8<SZ> && is_properly_aligned<alignof(T)> && std::is_trivially_destructible_v<T>
int zc_ring<T, P , C ,SZ , BI , BS , CS>::pop_get_buffer( local_cons_data_t* local_data, const T** buf){
	if(!buf && !local_data){
		return -1; /* ERROR */
	}
	
	constexpr size_t alignmentOfT = alignof(T) < 64 ? alignof(T) : 64 ;
	constexpr size_t payload_sz_alignment = ((sizeof(std::uint32_t) * 2 ) <= alignmentOfT) ? (sizeof(std::uint32_t)* 2 )  : alignmentOfT;

	auto no_of_local_read_left = local_data->m_local_read_left_ ; 
	
	if(no_of_local_read_left == 0 ){
		/* function that flushes and takes more sapce */
	}
	auto ptr = reinterpret_cast<unsigned char*>(local_data->m_addr_) + (local_data->m_local_curr_read_ * BS);
	auto payload_BS_cnt= *reinterpret_cast<Payload<T>*>(ptr)->m_sz_;
	local_data->m_local_curr_read_ =( local_data->m_local_curr_read_ +  payload_BS_cnt) & ~(m_prod_size_-1);
	auto sz = *reinterpret_cast<Payload<T>*>(ptr)->m_actual_data_sz ;
	*buf = reinterpret_cast<const T*>(ptr+ payload_sz_alignment);
	return sz ;
}
template <typename T, size_t P, size_t C, size_t SZ ,size_t BI,  size_t BS , size_t CS> 
requires is_multiple_of_8<SZ> && is_properly_aligned<alignof(T)> && std::is_trivially_destructible_v<T>
int zc_ring<T, P , C ,SZ , BI , BS , CS>::pop_bulk( size_t sz ){

	/* idea for pop hold onto the real atomic take a cache set and only change when my local is zero we might get into starvation but is less compilicated than hammering the atomic everysingle time or use an atomic to hammer on write   */
	return sz ;
}
int main(){
	// [[maybe_unused]]volatile zc_ring<int,2 , 2 , 4096> p_ring {};
	return 0 ;
}
