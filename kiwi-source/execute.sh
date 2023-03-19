#!/bin/bash
requests=10000000
mode=0
make all
cd bench

if [ "$mode" -eq 1 ]
then
    ./kiwi-bench write $requests 
    ./kiwi-bench read $requests 
else
    gdb --args kiwi-bench mix $requests 
fi

cd ..
make clean
