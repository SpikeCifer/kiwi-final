#!/bin/bash
requests=300000
mode=1
make clean
make all
cd bench

if [ "$mode" -eq 1 ]
then
    ./kiwi-bench mix $requests 
    
else
    gdb --args kiwi-bench mix $requests 
fi

cd ..
make clean
