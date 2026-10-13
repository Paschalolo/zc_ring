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

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */


#include <atomic>
#include <cstdint>
#include <new>
#include <sys/mman.h>
#include <stdlib.h>

struct SPSC_queue{
	std::atomic<std::uint16_t> global_counter{0} ;
	__attribute__((aligned(64))) std::atomic<std::uint32_t> head_acutal ; 	
	__attribute__((aligned(64))) std::atomic<std::uint32_t> head_processing ; 


	__attribute__((aligned(64))) std::atomic<std::uint32_t> tail_acutal ; 	
	__attribute__((aligned(64))) std::atomic<std::uint32_t> tail_processing; 	
};


/* These are the local variables of each queue in a thread */ 
/* note each queue can represent up to 16Gb of data at a 4byte per element number goes up as the data size is larger  */
struct __attribute__((aligned(64)))  zc_ring_local_t {
	std::uint16_t thread_id ;
	std::uint16_t mask ;
	std::uint32_t local_count ;
};

template <typename T , size_t CACHE_LINE = (std::hardware_destructive_interference_size * 2) > 
struct zc_ring{
};
int main(){
	return 0 ;
}
