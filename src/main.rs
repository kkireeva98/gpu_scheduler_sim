#![allow(unused)]
#[macro_use]
extern crate public;

mod csv_reader;
mod types;
mod evaluator;
mod heuristics;

use std::fs::File;
use heuristics::*;
use types::*;

use crate::evaluator::*;
use crate::evaluator::workload::Workload;

fn main() {

    let node_csv = File::open(&"clusterdata/node_data/all_nodes.csv").expect("node file not found");
    let pod_csv = File::open(&"clusterdata/pod_data/gpuspec33.csv").expect("pod file not found");


    let w : Workload = Workload::new(String::from("gpuspec33"), pod_csv);
    let c : Cluster = Cluster::default();

    let mut eval: Evaluator = Evaluator::new(
        random_scheduler,
        backlog_size,
        w,
        c,
    );

    eval.evaluate()

}