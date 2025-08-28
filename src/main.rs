#[macro_use]
extern crate public;
use std::fs::File;


mod csv_reader;
mod types;

use crate::csv_reader::*;
use crate::types::*;

fn main() {
    let file_path = "clusterdata/pod_data/gpuspec33.csv";
    let file = File::open(file_path).expect("file not found");

    process_csv(file, |i, record: PodSpec| {
        println!("{}: {:?}", i, record);
        Ok(())
    }).unwrap();
}