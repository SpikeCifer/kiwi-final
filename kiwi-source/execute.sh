#!/bin/bash
requests=50000
mode=1
make clean
make all
cd bench

if [ "$mode" -eq 1 ]
then
    time ./kiwi-bench mix $requests 
    
else
    gdb --args kiwi-bench mix $requests 
fi

cd ..
make clean
