requests=1000000
make all
cd bench
./kiwi-bench write $requests
./kiwi-bench read $requests
cd ..
make clean
