# Exp0: motivating experiment; please run in the prototype build folder
source ../common.sh
# number of zones
for repeat in 0 1 2 3 4; do
  for s in 1 2 4; do
    mkdir -p motivating_results/${s}
     for m in 0; do
       for c in 1; do
         sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./src/simple_traffic_gen -m ${m} -c ${c} -s ${s} -z 1 > motivating_results/${s}/${m}_${c}_${repeat}.result
       done
     done
    for m in 1; do
      for c in 1 2 3 4; do
        sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./src/simple_traffic_gen -m ${m} -c ${c} -s ${s} -z 1 > motivating_results/${s}/${m}_${c}_${repeat}.result
      done
    done
  done
done

for repeat in 0 1 2 3 4; do
  for s in 1 2 4; do
    mkdir -p motivating_results/concurrent_reqs/${s}
    for m in 0; do
      for c in 1; do
        for z in 1; do
          sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./src/simple_traffic_gen -m ${m} -c ${c} -s ${s} -z ${z} > motivating_results/concurrent_reqs/${s}/${m}_${c}_${z}_${repeat}.result
       done 
      done
    done
    for m in 1; do
      for c in 1 2 3 4 5 6 7 8; do
        for z in 1; do
          sudo LD_LIBRARY_PATH=$LD_LIBRARY_PATH ./src/simple_traffic_gen -m ${m} -c ${c} -s ${s} -z ${z} > motivating_results/concurrent_reqs/${s}/${m}_${c}_${z}_${repeat}.result
        done
      done
    done
  done
done
