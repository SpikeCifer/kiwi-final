#!/bin/bash
requests=500000
mode=2
make all
cd bench

if [ "$mode" -eq 1 ]
then
    ./kiwi-bench write $requests 
    gdb --args ./kiwi-bench read $requests 
else
    gdb --args kiwi-bench mix $requests 
fi

cd ..
make clean
