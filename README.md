# ZapRAID

## What's Inside
* zapraid_prototype, storing the prototype itself, implemented with C++
* zapraid_spdk, storing modified SPDK, with added SPDK bdev module for zapraid
* zapraid_femu, storing modified FEMU, with added ZNS SSD latency emulation functionality
* scripts, storing neccessary scripts for running all the experiments

## Building Guide
* Be sure to read this building guide before proceeding, and be sure to read every scripts before running it when doing experiments.

* ZapRAID SPDK
    * Build zapraid_spdk without zapraid first; configure with "--without-zapraid"
    * Build zapraid_prototype; specify the "SPDK_DIR" in src/CMakeLists.txt
    * Build zapraid_spdk again with zapraid; configure with "--with-zapraid=DIR"
    * Check scripts in scripts/ for running spdk fio and real applications

* ZapRAID FEMU
    * Follow the [FEMU build guide](https://github.com/vtess/FEMU)
