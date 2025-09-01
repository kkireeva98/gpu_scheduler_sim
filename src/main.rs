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
    let pod_csv = File::open(&"clusterdata/pod_data/default.csv").expect("pod file not found");

    let mut eval: Evaluator = Evaluator::new(
        dot_product_scheduler,
        max_tasks_arrived,
        pod_csv,
        node_csv,
    );

    eval.evaluate()

}