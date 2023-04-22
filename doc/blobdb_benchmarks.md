<!-- TOC -->

- [Performance](#performance)
  - [Environment](#environment)
  - [benchmarks](#benchmarks)
    - [ValueSize = 128B](#valuesize-128b)
    - [ValueSize = 1KB](#valuesize-1kb)
    - [ValueSize = 16KB](#valuesize-16kb)
  - [DISK FIO](#disk-fio)

<!-- TOC -->

# Performance

## Environment

```
CPU: Intel(R) Xeon(R) CPU E5-2686 v4
MEM: 32 GB
DISK: NVMe SSD 500 GB
```

## benchmarks

In general, under the 128B, 1KB and 16KB value size settings,

the BlobDB's random write is about 1.9x, 12x and 20x faster than LevelDB.

The BlobDB's sequential write is about 0.65x, 1.4x and 1.6x slower/faster than LevelDB.

The BlobDB's random read is about 1.08x, 1.68x and 4.8x faster than LevelDB.

The BlobDB's sequential read is about 0.24x, 0.37x and 0.86x slower than LevelDB.

### ValueSize = 128B

LevelDB:
```shell
# ./db_bench --db=./benchdb --num=15000000 --value_size=128 --bloom_bits=8
LevelDB:    version 1.23
Date:       Sat Apr 22 20:07:30 2023
CPU:        72 * Intel(R) Xeon(R) CPU E5-2686 v4 @ 2.30GHz
CPUCache:   46080 KB
Keys:       16 bytes each
Values:     128 bytes each (64 bytes after compression)
Entries:    15000000
RawSize:    2059.9 MB (estimated)
FileSize:   1144.4 MB (estimated)
WARNING: Snappy compression is not enabled
------------------------------------------------
fillseq      :       2.660 micros/op;   51.6 MB/s      
fillsync     :     972.014 micros/op;    0.1 MB/s (15000 ops)
fillrandom   :      10.562 micros/op;   13.0 MB/s      
overwrite    :      16.877 micros/op;    8.1 MB/s      
readrandom   :       6.835 micros/op;   17.4 MB/s (12971454 of 15000000 found)
readrandom   :       6.426 micros/op;   18.5 MB/s (12972012 of 15000000 found)
readseq      :       0.267 micros/op;  514.7 MB/s      
readreverse  :       0.567 micros/op;  242.4 MB/s      
compact      : 20364164.000 micros/op;
readrandom   :       4.226 micros/op;   28.1 MB/s (12971819 of 15000000 found)
readseq      :       0.208 micros/op;  661.1 MB/s      
readreverse  :       0.513 micros/op;  267.8 MB/s      
fill100K     :    3461.099 micros/op;   27.6 MB/s (15000 ops)
crc32c       :       1.921 micros/op; 2033.3 MB/s (4K per op)
snappycomp   :    8357.000 micros/op; (snappy failure)
snappyuncomp :    5312.000 micros/op; (snappy failure)
zstdcomp     :    7545.000 micros/op; (zstd failure)
zstduncomp   :    6063.000 micros/op; (zstd failure)
```

BlobDB:
```shell
# ./db_bench --db=./benchdb --num=15000000 --value_size=128 --bloom_bits=8 --blob=1 --blob_prefetch=0 --blob_iter_threads=0 --blob_value_size_threshold=64 --blob_file_size=268435456
LevelDB:    version 1.23
Date:       Sat Apr 22 20:24:13 2023
CPU:        72 * Intel(R) Xeon(R) CPU E5-2686 v4 @ 2.30GHz
CPUCache:   46080 KB
Keys:       16 bytes each
Values:     128 bytes each (64 bytes after compression)
Entries:    15000000
RawSize:    2059.9 MB (estimated)
FileSize:   1144.4 MB (estimated)
WARNING: Snappy compression is not enabled
------------------------------------------------
fillseq      :       4.037 micros/op;   34.0 MB/s      
fillsync     :    1336.192 micros/op;    0.1 MB/s (15000 ops)
fillrandom   :       5.627 micros/op;   24.4 MB/s      
overwrite    :       5.243 micros/op;   26.2 MB/s      
readrandom   :       6.001 micros/op;   19.8 MB/s (12971454 of 15000000 found)
readrandom   :       5.933 micros/op;   20.0 MB/s (12972012 of 15000000 found)
readseq      :       1.083 micros/op;  126.8 MB/s      
readreverse  :       1.435 micros/op;   95.7 MB/s      
compact      : 6881454.000 micros/op;
readrandom   :       3.953 micros/op;   30.0 MB/s (12971819 of 15000000 found)
readseq      :       1.048 micros/op;  131.0 MB/s      
readreverse  :       1.353 micros/op;  101.5 MB/s      
fill100K     :     373.512 micros/op;  255.4 MB/s (15000 ops)
crc32c       :       2.053 micros/op; 1902.8 MB/s (4K per op)
snappycomp   :   10763.000 micros/op; (snappy failure)
snappyuncomp :    3836.000 micros/op; (snappy failure)
```

### ValueSize = 1KB

LevelDB:
```shell
# ./db_bench --db=./benchdb --num=2000000 --value_size=1024 --bloom_bits=8
LevelDB:    version 1.23
Date:       Sat Apr 22 18:09:16 2023
CPU:        72 * Intel(R) Xeon(R) CPU E5-2686 v4 @ 2.30GHz
CPUCache:   46080 KB
Keys:       16 bytes each
Values:     1024 bytes each (512 bytes after compression)
Entries:    2000000
RawSize:    1983.6 MB (estimated)
FileSize:   1007.1 MB (estimated)
WARNING: Snappy compression is not enabled
------------------------------------------------
fillseq      :       8.076 micros/op;  122.8 MB/s     
fillsync     :    1254.942 micros/op;    0.8 MB/s (2000 ops)
fillrandom   :      77.676 micros/op;   12.8 MB/s     
overwrite    :      75.101 micros/op;   13.2 MB/s     
readrandom   :      11.928 micros/op;   71.9 MB/s (1729759 of 2000000 found)
readrandom   :       7.366 micros/op;  116.4 MB/s (1729191 of 2000000 found)
readseq      :       0.468 micros/op; 2121.4 MB/s     
readreverse  :       0.682 micros/op; 1454.9 MB/s     
compact      : 37270238.000 micros/op;
readrandom   :       3.594 micros/op;  238.6 MB/s (1729295 of 2000000 found)
readseq      :       0.397 micros/op; 2495.3 MB/s     
readreverse  :       0.625 micros/op; 1587.8 MB/s     
fill100K     :     775.459 micros/op;  123.0 MB/s (2000 ops)
crc32c       :       1.865 micros/op; 2094.3 MB/s (4K per op)
snappycomp   :    7983.000 micros/op; (snappy failure)
snappyuncomp :    5128.000 micros/op; (snappy failure)
zstdcomp     :    4681.000 micros/op; (zstd failure)
zstduncomp   :    6757.000 micros/op; (zstd failure)
```

BlobDB:
```shell
# ./db_bench --db=./benchdb --num=2000000 --value_size=1024 --bloom_bits=8 --blob=1 --blob_prefetch=0 --blob_iter_threads=0 --blob_value_size_threshold=128 --blob_file_size=268435456
LevelDB:    version 1.23
Date:       Sat Apr 22 18:28:41 2023
CPU:        72 * Intel(R) Xeon(R) CPU E5-2686 v4 @ 2.30GHz
CPUCache:   46080 KB
Keys:       16 bytes each
Values:     1024 bytes each (512 bytes after compression)
Entries:    2000000
RawSize:    1983.6 MB (estimated)
FileSize:   1007.1 MB (estimated)
WARNING: Snappy compression is not enabled
------------------------------------------------
fillseq      :       5.841 micros/op;  169.8 MB/s     
fillsync     :    1185.704 micros/op;    0.8 MB/s (2000 ops)
fillrandom   :       6.312 micros/op;  157.1 MB/s     
overwrite    :       6.463 micros/op;  153.5 MB/s     
readrandom   :       4.586 micros/op;  187.0 MB/s (1729759 of 2000000 found)
readrandom   :       4.367 micros/op;  196.4 MB/s (1729191 of 2000000 found)
readseq      :       1.238 micros/op;  801.0 MB/s     
readreverse  :       1.536 micros/op;  645.9 MB/s     
compact      : 1225085.000 micros/op;
readrandom   :       3.500 micros/op;  245.0 MB/s (1729295 of 2000000 found)
readseq      :       1.219 micros/op;  813.9 MB/s     
readreverse  :       1.559 micros/op;  636.0 MB/s     
fill100K     :     133.715 micros/op;  713.3 MB/s (2000 ops)
crc32c       :       1.882 micros/op; 2075.1 MB/s (4K per op)
snappycomp   :    7498.000 micros/op; (snappy failure)
snappyuncomp :    4296.000 micros/op; (snappy failure)
```


### ValueSize = 16KB

LevelDB:
```shell
# ./db_bench --db=./benchdb --num=200000 --value_size=16384 --bloom_bits=8
LevelDB:    version 1.23
Date:       Sat Apr 22 18:33:37 2023
CPU:        72 * Intel(R) Xeon(R) CPU E5-2686 v4 @ 2.30GHz
CPUCache:   46080 KB
Keys:       16 bytes each
Values:     16384 bytes each (8192 bytes after compression)
Entries:    200000
RawSize:    3128.1 MB (estimated)
FileSize:   1565.6 MB (estimated)
WARNING: Snappy compression is not enabled
------------------------------------------------
fillseq      :      58.352 micros/op;  268.0 MB/s    
fillsync     :    1373.220 micros/op;   11.4 MB/s (200 ops)
fillrandom   :     816.364 micros/op;   19.2 MB/s    
overwrite    :    1154.779 micros/op;   13.5 MB/s    
readrandom   :      40.666 micros/op;  332.6 MB/s (172966 of 200000 found)
readrandom   :      30.548 micros/op;  442.7 MB/s (172941 of 200000 found)
readseq      :       3.008 micros/op; 5200.2 MB/s    
readreverse  :       5.015 micros/op; 3118.7 MB/s    
compact      : 26579359.000 micros/op;
readrandom   :      23.951 micros/op;  564.8 MB/s (172972 of 200000 found)
readseq      :       2.416 micros/op; 6473.3 MB/s    
readreverse  :       4.189 micros/op; 3733.5 MB/s    
fill100K     :     637.000 micros/op;  149.7 MB/s (200 ops)
crc32c       :       1.909 micros/op; 2046.0 MB/s (4K per op)
snappycomp   :    9530.000 micros/op; (snappy failure)
snappyuncomp :    6214.000 micros/op; (snappy failure)
zstdcomp     :    8223.000 micros/op; (zstd failure)
zstduncomp   :    5770.000 micros/op; (zstd failure)
```

BlobDB:
```shell
# ./db_bench --db=./benchdb --num=200000 --value_size=16384 --bloom_bits=8 --blob=1 --blob_prefetch=0 --blob_iter_threads=0 --blob_value_size_threshold=128 --blob_file_size=268435456
LevelDB:    version 1.23
Date:       Sat Apr 22 18:32:30 2023
CPU:        72 * Intel(R) Xeon(R) CPU E5-2686 v4 @ 2.30GHz
CPUCache:   46080 KB
Keys:       16 bytes each
Values:     16384 bytes each (8192 bytes after compression)
Entries:    200000
RawSize:    3128.1 MB (estimated)
FileSize:   1565.6 MB (estimated)
WARNING: Snappy compression is not enabled
------------------------------------------------
fillseq      :      36.584 micros/op;  427.5 MB/s    
fillsync     :    1065.870 micros/op;   14.7 MB/s (200 ops)
fillrandom   :      40.181 micros/op;  389.2 MB/s    
overwrite    :      54.235 micros/op;  288.4 MB/s    
readrandom   :       8.565 micros/op; 1579.2 MB/s (172966 of 200000 found)
readrandom   :       6.328 micros/op; 2137.1 MB/s (172941 of 200000 found)
readseq      :       3.460 micros/op; 4520.0 MB/s    
readreverse  :       3.925 micros/op; 3984.6 MB/s    
compact      :  144524.000 micros/op;
readrandom   :       4.906 micros/op; 2757.1 MB/s (172972 of 200000 found)
readseq      :       3.227 micros/op; 4846.5 MB/s    
readreverse  :       3.521 micros/op; 4441.4 MB/s    
fill100K     :     196.240 micros/op;  486.1 MB/s (200 ops)
crc32c       :       1.902 micros/op; 2054.1 MB/s (4K per op)
snappycomp   :    9177.000 micros/op; (snappy failure)
snappyuncomp :    4563.000 micros/op; (snappy failure)
```

## DISK FIO

4K random read, 115K IOPS

```shell
# fio --randrepeat=1 --ioengine=sync --direct=1 --gtod_reduce=1 --name=test --filename=test_file --bs=4k --iodepth=64 --size=4G --readwrite=randread --numjobs=32 --group_reporting
test: (g=0): rw=randread, bs=(R) 4096B-4096B, (W) 4096B-4096B, (T) 4096B-4096B, ioengine=sync, iodepth=64
...
fio-3.1
Starting 32 processes
Jobs: 27 (f=27): [r(12),_(1),r(1),_(1),r(1),_(2),r(8),_(1),r(5)][99.7%][r=469MiB/s,w=0KiB/s][r=120k,w=0 IOPS][eta 00m:01s]
test: (groupid=0, jobs=32): err= 0: pid=8012: Sat Apr 22 20:44:51 2023
   read: IOPS=115k, BW=448MiB/s (470MB/s)(128GiB/292668msec)
   bw (  KiB/s): min=  592, max=28136, per=3.13%, avg=14358.79, stdev=5331.55, samples=18680
   iops        : min=  148, max= 7034, avg=3589.57, stdev=1332.90, samples=18680
  cpu          : usr=2.14%, sys=5.89%, ctx=33556644, majf=0, minf=5163
  IO depths    : 1=100.0%, 2=0.0%, 4=0.0%, 8=0.0%, 16=0.0%, 32=0.0%, >=64=0.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     issued rwt: total=33554432,0,0, short=0,0,0, dropped=0,0,0
     latency   : target=0, window=0, percentile=100.00%, depth=64

Run status group 0 (all jobs):
   READ: bw=448MiB/s (470MB/s), 448MiB/s-448MiB/s (470MB/s-470MB/s), io=128GiB (137GB), run=292668-292668msec

Disk stats (read/write):
  nvme0n1: ios=34920702/3659, merge=0/14, ticks=9964289/85252, in_queue=488736, util=100.00%
```

256K random read, 1761 MB/s Bandwidth

```shell
# fio --randrepeat=1 --ioengine=sync --direct=1 --gtod_reduce=1 --name=test --filename=test_file --bs=256k --iodepth=64 --size=4G --readwrite=randread --numjobs=32 --group_reporting
test: (g=0): rw=randread, bs=(R) 256KiB-256KiB, (W) 256KiB-256KiB, (T) 256KiB-256KiB, ioengine=sync, iodepth=64
...
fio-3.1
Starting 32 processes
Jobs: 27 (f=27): [_(1),r(15),_(1),r(4),_(1),r(1),_(1),r(1),_(1),r(6)][82.0%][r=1757MiB/s,w=0KiB/s][r=7026,w=0 IOPS][Jobs: 23 (f=23): [_(1),r(1),_(2),r(12),_(1),r(3),_(2),r(1),_(1),r(1),_(1),r(1),_(1),r(4)][84.1%][r=1786MiB/s,w=0KiB/Jobs: 20 (f=20): [_(4),r(12),_(1),r(1),_(1),r(1),_(2),r(1),_(1),r(1),_(1),r(1),_(2),r(3)][87.2%][r=1821MiB/s,w=0KiB/Jobs: 12 (f=12): [_(6),r(1),_(1),r(2),_(1),r(4),_(4),r(1),_(2),r(1),_(1),r(1),_(1),r(1),_(3),r(1),_(1)][89.4%][r=182Jobs: 8 (f=8): [_(6),r(1),_(1),r(2),_(1),r(1),_(2),r(1),_(9),r(1),_(1),r(1),_(3),r(1),_(1)][93.9%][r=1651MiB/s,w=0KiJobs: 1 (f=1): [_(8),r(1),_(23)][98.7%][r=1006MiB/s,w=0KiB/s][r=4022,w=0 IOPS][eta 00m:01s]                                                           
test: (groupid=0, jobs=32): err= 0: pid=8172: Sat Apr 22 20:47:17 2023
   read: IOPS=6716, BW=1679MiB/s (1761MB/s)(128GiB/78065msec)
   bw (  KiB/s): min= 8704, max=169296, per=3.21%, avg=55241.37, stdev=16920.34, samples=4790
   iops        : min=   34, max=  661, avg=215.68, stdev=66.05, samples=4790
  cpu          : usr=0.24%, sys=1.49%, ctx=524687, majf=0, minf=3010
  IO depths    : 1=100.0%, 2=0.0%, 4=0.0%, 8=0.0%, 16=0.0%, 32=0.0%, >=64=0.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     issued rwt: total=524288,0,0, short=0,0,0, dropped=0,0,0
     latency   : target=0, window=0, percentile=100.00%, depth=64

Run status group 0 (all jobs):
   READ: bw=1679MiB/s (1761MB/s), 1679MiB/s-1679MiB/s (1761MB/s-1761MB/s), io=128GiB (137GB), run=78065-78065msec

Disk stats (read/write):
  nvme0n1: ios=1279861/42, merge=0/2, ticks=5719887/211, in_queue=3446712, util=100.00%
```
