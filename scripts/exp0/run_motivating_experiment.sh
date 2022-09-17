# Exp0: motivating experiment; please run in the prototype build folder
source ../common.sh
for repeat in 0 1 2 3 4; do
  for s in 1 2 4; do
    mkdir -p motivating_results/${s}
     for m in 0; do
       for c in 1; do
         sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./src/simple_traffic_gen -m ${m} -c ${c} -s ${s} > motivating_results/${s}/${m}_${c}_${repeat}.result
       done
     done
    for m in 1; do
      for c in 1 2 3 4; do
        sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./src/simple_traffic_gen -m ${m} -c ${c} -s ${s} > motivating_results/${s}/${m}_${c}_${repeat}.result
      done
    done
  done
done
