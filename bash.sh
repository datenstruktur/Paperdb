rm -rf build
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DUSE_ADJUSTMENT_LOGGING=ON .. && cmake --build .
./db_bench --multi_queue_open=0 --num=100000000 --print_process=0 --value_size=1024 --reads=10000000 --benchmarks=fillrandom,readrandom
./db_bench --multi_queue_open=1 --num=100000000 --print_process=0 --value_size=1024 --reads=10000000 --benchmarks=fillrandom,readrandom
cd ../
rm -rf build