# CyDB

A key-value database, support pluggable storage engines.

## Dependence
All in one:
```shell
sudo apt install -y gcc-10 g++-10 librocksdb-dev cmake build-essential libgflags-dev libsnappy-dev zlib1g-dev libbz2-dev libzstd-dev libpthread-stubs0-dev
```

## Recovery
CyDB use ARIES recovery algorithm, which with *steal/no-force* policy.