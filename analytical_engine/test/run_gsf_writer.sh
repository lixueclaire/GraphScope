#!/bin/bash
set -eo pipefail
export VINEYARD_HOME=/usr/local/bin
# for open-mpi
export OMPI_MCA_btl_vader_single_copy_mechanism=none
export OMPI_MCA_orte_allowed_exit_without_sync=1

np=2
cmd_prefix="mpirun"
if ompi_info; then
  echo "Using openmpi"
  cmd_prefix="${cmd_prefix} --allow-run-as-root"
fi

socket_file=/tmp/vineyard.sock
test_dir=/Users/weibin/Dev/gstest
function start_vineyard() {
  pkill vineyardd || true
  pkill etcd || true
  echo "[INFO] vineyardd will using the socket_file on ${socket_file}"

  timestamp=$(date +%Y-%m-%d_%H-%M-%S)
  vineyardd \
    --socket ${socket_file} \
    --size 2000000000 \
    --etcd_prefix "${timestamp}" \
    --etcd_endpoint=http://127.0.0.1:3457 &
  set +m
  sleep 5
  echo "vineyardd started."
}

function run_vy() {
  num_procs=$1
  shift
  executable=$1
  shift
  socket_file=$1
  shift
  e_label_num=$1
  shift
  e_prefix=$1
  shift
  v_label_num=$1
  shift
  v_prefix=$1
  shift

  cmd="${cmd_prefix} -n ${num_procs} --host localhost:${num_procs} ${executable} ${socket_file}"

  cmd="${cmd} ${e_label_num}"
  count=0
  for ((e=0;e<e_label_num;++e))
  do
    cmd="${cmd} '"
    first=true
    for ((src=0;src<v_label_num;src++))
    do
      for ((dst=0;dst<v_label_num;++dst))
      do
    if [ "$first" = true ]
        then
          first=false
          cmd="${cmd}${e_prefix}_${src}_${dst}_${e}#src_label=v${src}&dst_label=v${src}&label=e${count}"
        else
          cmd="${cmd};${e_prefix}_${src}_${dst}_${e}#src_label=v${src}&dst_label=v${src}&label=e${count}"
    fi
      count=$(( $count + 1 ))
      done
    done
    cmd="${cmd}'"
  done

  cmd="${cmd} ${v_label_num}"
  for ((i = 0; i < v_label_num; i++)); do
    cmd="${cmd} ${v_prefix}_${i}#label=v${i}"
  done

  cmd="${cmd} $* -v 10"

  echo "${cmd}"
  eval "${cmd}"
  echo "Finished running app ${executable} with vineyard."
}

function run_lpa() {
  num_procs=$1
  shift
  executable=$1
  shift
  socket_file=$1
  shift
  e_label_num=$1
  shift
  e_prefix=$1
  shift
  v_label_num=$1
  shift
  v_prefix=$1
  shift

  cmd="${cmd_prefix} -n ${num_procs} --host localhost:${num_procs} ${executable} ${socket_file}"

  cmd="${cmd} ${e_label_num}"
  for ((i = 0; i < e_label_num; i++)); do
    cmd="${cmd} '${e_prefix}_${i}_int2.csv#src_label=person&dst_label=person&label=knows#delimiter=|'"
  done

  cmd="${cmd} ${v_label_num}"
  for ((i = 0; i < v_label_num; i++)); do
    cmd="${cmd} '${v_prefix}_${i}.csv#label=person#delimiter=|'"
  done

  cmd="${cmd} $*"

  echo "${cmd}"
  eval "${cmd}"
  echo "Finished running lpa on property graph."
}

function run_gsf() {
  num_procs=$1
  shift
  executable=$1
  shift
  socket_file=$1
  shift
  graph_yaml_path=$1
  shift
  directed=$1
  shift

  cmd="${cmd_prefix} -n ${num_procs} --host localhost:${num_procs} ${executable} ${socket_file} ${graph_yaml_path} ${directed}"

  echo "${cmd}"
  eval "${cmd}"
  echo "Finished running gsf on property graph."
}

start_vineyard
# run_vy ${np} ./run_local "${socket_file}" 2 ./e 2 ./v 0
# run_lpa ${np} ./run_gsf "${socket_file}" "/Users/weibin/Dev/gsf/test/yaml_example/ldbc_sample.graph.yml" 1
# run_vy ${np} ./run_local "${socket_file}" 2 "${test_dir}"/new_property/v2_e2/twitter_e 2 "${test_dir}"/new_property/v2_e2/twitter_v 0
run_lpa ${np} ./run_gsf_writer "${socket_file}" 1 "/Users/weibin/Dev/gstest/ldbc_sample/person_knows_person_0" 1 "/Users/weibin/Dev/gstest/ldbc_sample/person_0" 1
# run_lpa ${np} ./run_gsf_writer "${socket_file}" 1 "/Users/weibin/Dev/gstest/property/p2p-31_property_e" 1 "/Users/weibin/Dev/gstest/property/p2p-31_property_v" 1
