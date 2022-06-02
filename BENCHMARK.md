simple kv separate benchmark in debug mode in wsl2:
```cpp
LevelDB:    version 1.23
Date:       Thu Jun  2 14:24:55 2022
CPU:        8 * Intel(R) Core(TM) i5-8250U CPU @ 1.60GHz
CPUCache:   256 KB
Keys:       16 bytes each
Values:     100 bytes each (50 bytes after compression)
Entries:    1000000
RawSize:    110.6 MB (estimated)
FileSize:   62.9 MB (estimated)
WARNING: Optimization is disabled: benchmarks unnecessarily slow
WARNING: Assertions are enabled; benchmarks unnecessarily slow
WARNING: Snappy compression is not enabled
------------------------------------------------
fillseq      :      13.025 micros/op;    8.5 MB/s
fillsync     :    1378.532 micros/op;    0.1 MB/s (1000 ops)
fillrandom   :      14.107 micros/op;    7.8 MB/s
overwrite    :      15.012 micros/op;    7.4 MB/s
readrandom   :      77.688 micros/op; (864322 of 1000000 found)
readrandom   :      77.752 micros/op; (864083 of 1000000 found)
readseq      :      83.157 micros/op;    1.3 MB/s
readreverse  :     106.677 micros/op;    1.0 MB/s
compact      :     211.000 micros/op;
readrandom   :      93.309 micros/op; (864105 of 1000000 found)
readseq      :      76.997 micros/op;    1.4 MB/s
readreverse  :      77.224 micros/op;    1.4 MB/s
fill100K     :     307.030 micros/op;  310.7 MB/s (1000 ops)
crc32c       :       9.672 micros/op;  403.9 MB/s (4K per op)
snappycomp   :    5602.000 micros/op; (snappy failure)
snappyuncomp :    5700.000 micros/op; (snappy failure)
```
use release, close assert, use snappy
```cpp
LevelDB:    version 1.23
Date:       Thu Jun  2 15:19:04 2022
CPU:        8 * Intel(R) Core(TM) i5-8250U CPU @ 1.60GHz
CPUCache:   256 KB
Keys:       16 bytes each
Values:     100 bytes each (50 bytes after compression)
Entries:    1000000
RawSize:    110.6 MB (estimated)
FileSize:   62.9 MB (estimated)
------------------------------------------------
fillseq      :       7.699 micros/op;   14.4 MB/s
fillsync     :    1252.170 micros/op;    0.1 MB/s (1000 ops)
fillrandom   :       9.086 micros/op;   12.2 MB/s
overwrite    :       8.349 micros/op;   13.3 MB/s
readrandom   :      77.782 micros/op; (864322 of 1000000 found)
readrandom   :      78.582 micros/op; (864083 of 1000000 found)
readseq      :      58.942 micros/op;    1.9 MB/s
readreverse  :      59.384 micros/op;    1.9 MB/s
compact      :       7.000 micros/op;
readrandom   :      85.128 micros/op; (864105 of 1000000 found)
readseq      :      64.750 micros/op;    1.7 MB/s
readreverse  :      61.726 micros/op;    1.8 MB/s
fill100K     :     127.203 micros/op;  749.8 MB/s (1000 ops)
crc32c       :       1.369 micros/op; 2853.2 MB/s (4K per op)
snappycomp   :      48.324 micros/op;   80.8 MB/s (output: 55.1%)
snappyuncomp :       4.529 micros/op;  862.6 MB/s

```