requests=10000
make all
cd bench
./kiwi-bench mix $requests 

cd ..
make clean
