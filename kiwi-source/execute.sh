make all
cd bench
./kiwi-bench write 1000000
sleep 5
./kiwi-bench read 1000000
sleep 5
cd ..
make clean
