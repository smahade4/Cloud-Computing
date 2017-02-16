Assignment 1 - Benchmark  different parts  of a	computer system, from the CPU, memory, disk, and network.	 
------------------------------------------------------------------------------------------------------------------------------------------
Implemented following benchmarks:

	1) CPU
	2) DISK
	3) MEMORY
*****************************************************************************************************************************************
1) CPU
*****************************************************************************************************************************************

Implementation
------------------------------------
No of Iterations Taken : 100000000
No of Operations Taken : 12

Function: threadFunc_float
Parameters: Size (This is value that takes the no of times the loop wil run)
Returns: 1) Return the time taken to perform the float operations
purpose: 1) This function takes float values and do floating point operations(addition,multiplication and subtraction)
	 2) Store the time taken for floating point operation


Function: threadFunc_int
Parameters: Size (This is value that takes the no of times the loop wil run)
Returns: 1) Return the time taken to perform the float operations
purpose: 1) This function takes integer values and calculates
	    Integer point instruction(addition,multiplication and subtraction) 
	 2) Store the time taken for integer operation
-----------------------------------------
Method For Executing the Code
-----------------------------------------
To run CPU Benchmark perform following operation:
Place the Cpu source code in linux folder
Inside the folder directory
For Compiling the Code
gcc -o pa1_cpu_first.o -c pa1_cpu_first.c
gcc -o pa1_cpu_first.exe pa1_cpu_first.o -pthread

gcc -o pa1_cpu_second.o -c pa1_cpu_second.c
gcc -o pa1_cpu_second.exe pa1_cpu_second.o -pthread

For Running the exe
./pa1_cpu_first.exe
./pa1_cpu_second.exe

*****************************************************************************************************************************************
2) DISK
*****************************************************************************************************************************************

No of Iterations Taken : 20*1000000

-----------------------------------------
Method For Executing the Code
-----------------------------------------
To run Disk Benchmark perform following operation:
Place the Disk source code in linux folder 
Inside the Disk folder execute the following command in terminal
For Compiling the Code
gcc -o pa1_disk.o -c pa1_disk.c
gcc -o pa1_disk.exe pa1_disk.o -pthread

For Running the exe
./pa1_disk.exe

*****************************************************************************************************************************************
3) MEMORY
*****************************************************************************************************************************************
No of Iterations Taken : 20*1000000


To run Memory Benchmark perform following operation:
Place the Memory source code in linux 
Inside the Memory folder execute the following command in terminal
For Compiling the Code
gcc -o pa1_memory.o -c pa1_memory.c
gcc -o pa1_memory.exe pa1_memory.o -pthread

For Running the exe
./pa1_memory.exe

***************************************************************************************************************************
BENCHMARKS
***************************************************************************************************************************


***************************************************************************************************************************

***************************************************************************************************************************

