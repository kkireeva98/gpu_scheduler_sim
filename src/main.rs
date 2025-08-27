#![allow(unused)]

mod csv_reader;

use std::fs::File;
use csv_reader::*;

fn main() {
    let file_path = "clusterdata/node_data/all_nodes.csv";
    let file = File::open(file_path).expect("file not found");

    process_csv(file, |i, record: PodSpec| {
        println!("{}: {:?}", i, record);
        Ok(())
    }).unwrap();
}