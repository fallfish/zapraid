source ../common.sh

mkdir recovery_results
# please run this script under prototype build directory
for size in 100 200 300 400 500 600 700 800 900 1000; do
  sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./src/app -n 1 -s $size > recovery_results/${size}_load.output
  for repeat in 1 2 3 4 5; do
    sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./src/app -n 0 -s $size > recovery_results/${size}_${repeat}_recovery.output
  done
done
