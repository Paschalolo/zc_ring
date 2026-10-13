

MFILE = $(FILE).cpp
GCC = g++-14
GCCFLAGS = -Wall -Wextra -Werror -Wpedantic -Wnull-dereference  -Wshadow -Wconversion -Wduplicated-branches -Wlogical-op -O3  -Wformat=2   -pthread  -fno-exceptions -flto
C23 = -std=c++23
INCLUDES = -I includes -I src 
COMPILE.c = $(GCC) $(GCCFLAGS) $(INCLUDES) $(C23)
OBJ_DIR = src
OBJS = $(wildcard $(OBJ_DIR)/*.o)
vpath %.c src 
vpath %.cc src 
vpath %.cpp src 
vpath %.h includes 

run : $(MFILE) $(HARGS)
	$(COMPILE.c) $<  -o ./bin/bb && ./bin/bb 
exec : $(OBJS) 
	$(COMPILE.c) $(OBJS)  -o bin/bb && ./bin/bb  	

create: 
	touch src/$(MFILE)
push : 
	git add . && git commit -m"message " && git push origin main
