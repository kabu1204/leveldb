<!-- TOC -->

* [Performance](#performance)
    * [Environment](#environment)
    * [benchmarks](#benchmarks)
        * [BlobDB disabled](#blobdb-disabled)
        * [BlobDB enabled](#blobdb-enabled)
    * [DISK FIO](#disk-fio)

<!-- TOC -->

# Performance

## Environment

```
CPU: Intel Xeon Platinum 8255C @ 2.494GHz x 16
MEM: 64 GB
DISK: NVMe SSD 3.54TB
```

## benchmarks

In general, under the 128B, 1KB and 16KB value size,
the BlobDB's random write is about 1.5x, 7x and 14x faster than LevelDB.

The BlobDB's sequential write is about 0.8x, 0.8x and 1.08x slower/faster than LevelDB.

The BlobDB's random read is about 1.05x, 7x and 6.2x faster than LevelDB.

The BlobDB's sequential read is about 0.2x, 0.34x and 0.65x slower than LevelDB.

### BlobDB disabled

ValueSize = 128B

```shell
ubuntu@VM-0-15-ubuntu:~/nvme/leveldb/build$ ./db_bench --db=./benchdb --value_size=128 --num=10000000
LevelDB:    version 1.23
Date:       Fri Apr 21 19:21:57 2023
CPU:        16 * Intel(R) Xeon(R) Platinum 8255C CPU @ 2.50GHz
CPUCache:   36608 KB
Keys:       16 bytes each
Values:     128 bytes each (64 bytes after compression)
Entries:    10000000
RawSize:    1373.3 MB (estimated)
FileSize:   762.9 MB (estimated)
WARNING: Snappy compression is not enabled
------------------------------------------------
fillseq      :       1.976 micros/op;   69.5 MB/s      
fillsync     :      88.421 micros/op;    1.6 MB/s (10000 ops)
fillrandom   :       4.175 micros/op;   32.9 MB/s      
overwrite    :       5.374 micros/op;   25.6 MB/s      
readrandom   :       3.637 micros/op;   32.6 MB/s (8645864 of 10000000 found)
readrandom   :       3.279 micros/op;   36.2 MB/s (8644486 of 10000000 found)
readseq      :       0.125 micros/op; 1094.9 MB/s     
readreverse  :       0.278 micros/op;  493.4 MB/s     
compact      : 7306056.000 micros/op;
readrandom   :       2.550 micros/op;   46.6 MB/s (8645442 of 10000000 found)
readseq      :       0.104 micros/op; 1315.8 MB/s     
readreverse  :       0.256 micros/op;  536.9 MB/s     
fill100K     :     916.528 micros/op;  104.1 MB/s (10000 ops)
crc32c       :       1.422 micros/op; 2747.0 MB/s (4K per op)
snappycomp   :    2427.000 micros/op; (snappy failure)
snappyuncomp :    2306.000 micros/op; (snappy failure)
```

ValueSize = 1KB

```shell
ubuntu@VM-0-15-ubuntu:~/nvme/leveldb/build$ ./db_bench --db=./benchdb --value_size=1024 --num=10000000
LevelDB:    version 1.23
Date:       Fri Apr 21 19:27:52 2023
CPU:        16 * Intel(R) Xeon(R) Platinum 8255C CPU @ 2.50GHz
CPUCache:   36608 KB
Keys:       16 bytes each
Values:     1024 bytes each (512 bytes after compression)
Entries:    10000000
RawSize:    9918.2 MB (estimated)
FileSize:   5035.4 MB (estimated)
WARNING: Snappy compression is not enabled
------------------------------------------------
fillseq      :       3.139 micros/op;  316.0 MB/s      
fillsync     :      92.303 micros/op;   10.7 MB/s (10000 ops)
fillrandom   :      32.630 micros/op;   30.4 MB/s      
overwrite    :      41.577 micros/op;   23.9 MB/s      
readrandom   :      26.261 micros/op;   32.7 MB/s (8645864 of 10000000 found)
readrandom   :      24.103 micros/op;   35.6 MB/s (8644486 of 10000000 found)
readseq      :       0.338 micros/op; 2936.8 MB/s     
readreverse  :       0.513 micros/op; 1932.9 MB/s     
compact      : 21328215.000 micros/op;
readrandom   :      21.676 micros/op;   39.6 MB/s (8645442 of 10000000 found)
readseq      :       0.299 micros/op; 3318.5 MB/s     
readreverse  :       0.478 micros/op; 2073.2 MB/s     
fill100K     :     916.148 micros/op;  104.1 MB/s (10000 ops)
crc32c       :       1.366 micros/op; 2860.0 MB/s (4K per op)
snappycomp   :    2429.000 micros/op; (snappy failure)
snappyuncomp :    2256.000 micros/op; (snappy failure)
```

ValueSize = 16KB

```shell
ubuntu@VM-0-15-ubuntu:~/nvme/leveldb/build$ ./db_bench --db=./benchdb --value_size=16384 --num=1000000
LevelDB:    version 1.23
Date:       Fri Apr 21 20:06:06 2023
CPU:        16 * Intel(R) Xeon(R) Platinum 8255C CPU @ 2.50GHz
CPUCache:   36608 KB
Keys:       16 bytes each
Values:     16384 bytes each (8192 bytes after compression)
Entries:    1000000
RawSize:    15640.3 MB (estimated)
FileSize:   7827.8 MB (estimated)
WARNING: Snappy compression is not enabled
------------------------------------------------
fillseq      :      29.309 micros/op;  533.6 MB/s     
fillsync     :     117.826 micros/op;  132.7 MB/s (1000 ops)
fillrandom   :     400.378 micros/op;   39.1 MB/s     
overwrite    :     538.067 micros/op;   29.1 MB/s     
readrandom   :      58.157 micros/op;  232.4 MB/s (864322 of 1000000 found)
readrandom   :      29.991 micros/op;  450.6 MB/s (864083 of 1000000 found)
readseq      :       1.821 micros/op; 8588.2 MB/s    
readreverse  :       3.723 micros/op; 4200.7 MB/s    
compact      : 56265834.000 micros/op;
readrandom   :      25.713 micros/op;  525.6 MB/s (864105 of 1000000 found)
readseq      :       1.622 micros/op; 9642.6 MB/s    
readreverse  :       3.300 micros/op; 4739.0 MB/s    
fill100K     :     286.647 micros/op;  332.8 MB/s (1000 ops)
crc32c       :       1.366 micros/op; 2859.7 MB/s (4K per op)
snappycomp   :    3241.000 micros/op; (snappy failure)
snappyuncomp :    2288.000 micros/op; (snappy failure)
```

### BlobDB enabled

ValueSize = 128B

```shell
ubuntu@VM-0-15-ubuntu:~/nvme/leveldb/build$ ./db_bench --db=./benchdb --value_size=128 --num=10000000 --blob=1 --blob_prefetch=0 --blob_iter_threads=0 --blob_value_size_threshold=128 --blob_file_size=134217728
LevelDB:    version 1.23
Date:       Fri Apr 21 20:39:27 2023
CPU:        16 * Intel(R) Xeon(R) Platinum 8255C CPU @ 2.50GHz
CPUCache:   36608 KB
Keys:       16 bytes each
Values:     128 bytes each (64 bytes after compression)
Entries:    10000000
RawSize:    1373.3 MB (estimated)
FileSize:   762.9 MB (estimated)
WARNING: Snappy compression is not enabled
------------------------------------------------
fillseq      :       2.507 micros/op;   54.8 MB/s      
fillsync     :      92.755 micros/op;    1.5 MB/s (10000 ops)
fillrandom   :       2.988 micros/op;   46.0 MB/s      
overwrite    :       3.006 micros/op;   45.7 MB/s      
readrandom   :       3.182 micros/op;   37.3 MB/s (8645864 of 10000000 found)
readrandom   :       3.096 micros/op;   38.3 MB/s (8644486 of 10000000 found)
readseq      :       0.654 micros/op;  210.0 MB/s     
readreverse  :       0.782 micros/op;  175.5 MB/s     
compact      : 1780951.000 micros/op;
readrandom   :       2.377 micros/op;   49.9 MB/s (8645442 of 10000000 found)
readseq      :       0.638 micros/op;  215.1 MB/s     
readreverse  :       0.760 micros/op;  180.6 MB/s     
fill100K     :     148.471 micros/op;  642.4 MB/s (10000 ops)
crc32c       :       1.371 micros/op; 2848.7 MB/s (4K per op)
snappycomp   :    2789.000 micros/op; (snappy failure)
snappyuncomp :    2281.000 micros/op; (snappy failure)
```

ValueSize = 1KB

```shell
ubuntu@VM-0-15-ubuntu:~/nvme/leveldb/build$ ./db_bench --db=./benchdb --value_size=1024 --num=10000000 --blob=1 --blob_prefetch=0 --blob_iter_threads=0 --blob_value_size_threshold=128 --blob_file_size=134217728
LevelDB:    version 1.23
Date:       Fri Apr 21 22:03:28 2023
CPU:        16 * Intel(R) Xeon(R) Platinum 8255C CPU @ 2.50GHz
CPUCache:   36608 KB
Keys:       16 bytes each
Values:     1024 bytes each (512 bytes after compression)
Entries:    10000000
RawSize:    9918.2 MB (estimated)
FileSize:   5035.4 MB (estimated)
WARNING: Snappy compression is not enabled
------------------------------------------------
fillseq      :       3.873 micros/op;  256.1 MB/s      
fillsync     :      91.158 micros/op;   10.9 MB/s (10000 ops)
fillrandom   :       4.431 micros/op;  223.8 MB/s      
overwrite    :       4.504 micros/op;  220.2 MB/s      
readrandom   :       3.665 micros/op;  234.0 MB/s (8645864 of 10000000 found)
readrandom   :       3.473 micros/op;  246.8 MB/s (8644486 of 10000000 found)
readseq      :       0.967 micros/op; 1026.2 MB/s     
readreverse  :       1.091 micros/op;  909.4 MB/s     
compact      : 1813753.000 micros/op;
readrandom   :       2.681 micros/op;  319.8 MB/s (8645442 of 10000000 found)
readseq      :       0.919 micros/op; 1079.3 MB/s     
readreverse  :       1.052 micros/op;  942.9 MB/s     
fill100K     :     146.664 micros/op;  650.3 MB/s (10000 ops)
crc32c       :       1.365 micros/op; 2861.7 MB/s (4K per op)
snappycomp   :    2377.000 micros/op; (snappy failure)
snappyuncomp :    2280.000 micros/op; (snappy failure)
```

ValueSize = 16KB

```shell
ubuntu@VM-0-15-ubuntu:~/nvme/leveldb/build$ ./db_bench --db=./benchdb --value_size=16384 --num=1000000 --blob=1 --blob_prefetch=0 --blob_iter_threads=0 --blob_value_size_threshold=128 --blob_file_size=134217728
LevelDB:    version 1.23
Date:       Fri Apr 21 22:29:36 2023
CPU:        16 * Intel(R) Xeon(R) Platinum 8255C CPU @ 2.50GHz
CPUCache:   36608 KB
Keys:       16 bytes each
Values:     16384 bytes each (8192 bytes after compression)
Entries:    1000000
RawSize:    15640.3 MB (estimated)
FileSize:   7827.8 MB (estimated)
WARNING: Snappy compression is not enabled
------------------------------------------------
fillseq      :      27.076 micros/op;  577.7 MB/s     
fillsync     :      96.384 micros/op;  162.3 MB/s (1000 ops)
fillrandom   :      27.850 micros/op;  561.6 MB/s     
overwrite    :      28.457 micros/op;  549.6 MB/s     
readrandom   :       6.118 micros/op; 2209.7 MB/s (864322 of 1000000 found)
readrandom   :       4.814 micros/op; 2807.1 MB/s (864083 of 1000000 found)
readseq      :       2.817 micros/op; 5552.8 MB/s    
readreverse  :       2.945 micros/op; 5310.7 MB/s    
compact      :  172851.000 micros/op;
readrandom   :       4.353 micros/op; 3105.1 MB/s (864105 of 1000000 found)
readseq      :       2.840 micros/op; 5507.7 MB/s    
readreverse  :       2.756 micros/op; 5675.6 MB/s    
fill100K     :     111.692 micros/op;  854.0 MB/s (1000 ops)
crc32c       :       1.365 micros/op; 2862.7 MB/s (4K per op)
snappycomp   :    2483.000 micros/op; (snappy failure)
snappyuncomp :    2320.000 micros/op; (snappy failure)
```

## DISK FIO

4K random read, 201K IOPS

```shell
ubuntu@VM-0-15-ubuntu:~/nvme$ fio --randrepeat=1 --ioengine=sync --direct=1 --gtod_reduce=1 --name=test --filename=test_file --bs=4k --iodepth=64 --size=4G --readwrite=randread --numjobs=32 --group_reporting
test: (g=0): rw=randread, bs=(R) 4096B-4096B, (W) 4096B-4096B, (T) 4096B-4096B, ioengine=sync, iodepth=64
...
fio-3.28
Starting 32 processes
test: Laying out IO file (1 file / 4096MiB)
Jobs: 32 (f=32): [r(32)][100.0%][r=788MiB/s][r=202k IOPS][eta 00m:00s]
test: (groupid=0, jobs=32): err= 0: pid=18015: Fri Apr 21 18:41:33 2023
  read: IOPS=201k, BW=787MiB/s (825MB/s)(128GiB/166584msec)
   bw (  KiB/s): min=779312, max=833984, per=100.00%, avg=806701.89, stdev=291.41, samples=10624
   iops        : min=194828, max=208496, avg=201675.45, stdev=72.85, samples=10624
  cpu          : usr=2.47%, sys=8.18%, ctx=33557234, majf=0, minf=285
  IO depths    : 1=100.0%, 2=0.0%, 4=0.0%, 8=0.0%, 16=0.0%, 32=0.0%, >=64=0.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     issued rwts: total=33554432,0,0,0 short=0,0,0,0 dropped=0,0,0,0
     latency   : target=0, window=0, percentile=100.00%, depth=64

Run status group 0 (all jobs):
   READ: bw=787MiB/s (825MB/s), 787MiB/s-787MiB/s (825MB/s-825MB/s), io=128GiB (137GB), run=166584-166584msec

Disk stats (read/write):
  vdb: ios=33541979/8, merge=0/1, ticks=4738896/1, in_queue=4738898, util=100.00%
```

256K random read, 2731 MB/s Bandwidth

```shell
ubuntu@VM-0-15-ubuntu:~/nvme$ fio --randrepeat=1 --ioengine=sync --direct=1 --gtod_reduce=1 --name=test --filename=test_file --bs=256k --iodepth=64 --size=4G --readwrite=randread --numjobs=32 --group_reporting
test: (g=0): rw=randread, bs=(R) 256KiB-256KiB, (W) 256KiB-256KiB, (T) 256KiB-256KiB, ioengine=sync, iodepth=64
...
fio-3.28
Starting 32 processes
Jobs: 32 (f=32): [r(32)][100.0%][r=2573MiB/s][r=10.3k IOPS][eta 00m:00s]
test: (groupid=0, jobs=32): err= 0: pid=20460: Fri Apr 21 18:46:24 2023
  read: IOPS=10.4k, BW=2604MiB/s (2731MB/s)(128GiB/50330msec)
   bw (  MiB/s): min= 2368, max= 2889, per=100.00%, avg=2606.84, stdev= 3.19, samples=3200
   iops        : min= 9474, max=11558, avg=10427.26, stdev=12.78, samples=3200
  cpu          : usr=0.10%, sys=0.56%, ctx=524401, majf=0, minf=2298
  IO depths    : 1=100.0%, 2=0.0%, 4=0.0%, 8=0.0%, 16=0.0%, 32=0.0%, >=64=0.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     issued rwts: total=524288,0,0,0 short=0,0,0,0 dropped=0,0,0,0
     latency   : target=0, window=0, percentile=100.00%, depth=64

Run status group 0 (all jobs):
   READ: bw=2604MiB/s (2731MB/s), 2604MiB/s-2604MiB/s (2731MB/s-2731MB/s), io=128GiB (137GB), run=50330-50330msec

Disk stats (read/write):
  vdb: ios=522417/2, merge=0/0, ticks=1595254/0, in_queue=1595255, util=99.84%
```

4K random write, 265K IOPS

```shell
ubuntu@VM-0-15-ubuntu:~/nvme$ fio --randrepeat=1 --ioengine=sync --direct=1 --gtod_reduce=1 --name=test --filename=test_file --bs=4k --iodepth=64 --size=4G --readwrite=randwrite --numjobs=32 --group_reporting
test: (g=0): rw=randwrite, bs=(R) 4096B-4096B, (W) 4096B-4096B, (T) 4096B-4096B, ioengine=sync, iodepth=64
...
fio-3.28
Starting 32 processes
Jobs: 32 (f=32): [w(32)][99.2%][w=1036MiB/s][w=265k IOPS][eta 00m:01s]
test: (groupid=0, jobs=32): err= 0: pid=24991: Fri Apr 21 19:00:45 2023
  write: IOPS=265k, BW=1034MiB/s (1084MB/s)(128GiB/126743msec); 0 zone resets
   bw (  MiB/s): min=  966, max= 1146, per=100.00%, avg=1035.97, stdev= 0.80, samples=8081
   iops        : min=247474, max=293590, avg=265208.76, stdev=205.75, samples=8081
  cpu          : usr=3.34%, sys=13.82%, ctx=33546136, majf=0, minf=267
  IO depths    : 1=100.0%, 2=0.0%, 4=0.0%, 8=0.0%, 16=0.0%, 32=0.0%, >=64=0.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     issued rwts: total=0,33554432,0,0 short=0,0,0,0 dropped=0,0,0,0
     latency   : target=0, window=0, percentile=100.00%, depth=64

Run status group 0 (all jobs):
  WRITE: bw=1034MiB/s (1084MB/s), 1034MiB/s-1034MiB/s (1084MB/s-1084MB/s), io=128GiB (137GB), run=126743-126743msec

Disk stats (read/write):
  vdb: ios=13/33538916, merge=0/0, ticks=2/3364512, in_queue=3364515, util=99.98%
```

256K random write, 2835 MB/s Bandwidth

```shell
ubuntu@VM-0-15-ubuntu:~/nvme$ fio --randrepeat=1 --ioengine=sync --direct=1 --gtod_reduce=1 --name=test --filename=test_file --bs=256k --iodepth=64 --size=4G --readwrite=randwrite --numjobs=32 --group_reporting
test: (g=0): rw=randwrite, bs=(R) 256KiB-256KiB, (W) 256KiB-256KiB, (T) 256KiB-256KiB, ioengine=sync, iodepth=64
...
fio-3.28
Starting 32 processes
Jobs: 32 (f=32): [w(32)][100.0%][w=2549MiB/s][w=10.2k IOPS][eta 00m:00s]
test: (groupid=0, jobs=32): err= 0: pid=24244: Fri Apr 21 18:57:12 2023
  write: IOPS=10.8k, BW=2704MiB/s (2836MB/s)(128GiB/48467msec); 0 zone resets
   bw (  MiB/s): min= 2449, max= 2905, per=100.00%, avg=2708.34, stdev= 3.35, samples=3072
   iops        : min= 9794, max=11620, avg=10832.99, stdev=13.41, samples=3072
  cpu          : usr=0.27%, sys=0.40%, ctx=524318, majf=0, minf=265
  IO depths    : 1=100.0%, 2=0.0%, 4=0.0%, 8=0.0%, 16=0.0%, 32=0.0%, >=64=0.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     issued rwts: total=0,524288,0,0 short=0,0,0,0 dropped=0,0,0,0
     latency   : target=0, window=0, percentile=100.00%, depth=64

Run status group 0 (all jobs):
  WRITE: bw=2704MiB/s (2836MB/s), 2704MiB/s-2704MiB/s (2836MB/s-2836MB/s), io=128GiB (137GB), run=48467-48467msec

Disk stats (read/write):
  vdb: ios=4/523190, merge=0/0, ticks=0/1537703, in_queue=1537703, util=99.88%
```

Sequential read: 2672 MB/s Bandwidth

Sequential write: 3197 MB/s Bandwidth