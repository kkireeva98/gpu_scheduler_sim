#![allow(unused)]


use std::{error::Error, io::stdin, ffi::OsString, fs::File, process};
use std::collections::HashMap;

#[derive(Debug, Clone)]
#[derive(serde::Deserialize)]
#[derive(Eq, Hash, PartialEq)]
enum GpuSpec{
    A10,
    G2,
    G3,
    P100,
    T4,
    V100M16,
    V100M32,
}

type NODE = usize;
type CPU = u32;
type MEM = u32;
type GPU = u32;
type MODEL = Option<GpuSpec>;

#[derive(Debug, Clone)]
#[derive(serde::Deserialize)]
struct NodeSpec {
    #[serde(skip_deserializing)]
    node: NODE,
    cpu_milli: CPU,
    memory_mib: MEM,
    gpu: GPU,
    model: MODEL,
}

type NodeSpecKey = (MODEL, GPU);
type NodeSpecValue = Vec<NODE>;

type NodeHash = HashMap<NodeSpecKey, NodeSpecValue>;


fn example(file: File) -> Result<(NodeHash), Box<dyn Error>> {
    let mut nodes_hash = NodeHash::new();
    let mut nodes: Vec<NodeSpec> = Vec::new();

    let mut rdr = csv::Reader::from_reader(file);
    for (i, result) in rdr.deserialize().enumerate() {
        // Notice that we need to provide a type hint for automatic
        // deserialization.
        let record: NodeSpec = result?;
        nodes.push(record.clone());

        let key = (record.model, record.gpu);
        let value = i;

        nodes_hash.entry(key).or_insert(Vec::new()).push(value);
    }

    Ok(nodes_hash)
}

fn main() {
    let file_path = "clusterdata/node_data/all_nodes.csv";
    let file = File::open(file_path).expect("file not found");

    match example(file) {
        Err(err) => {
            println!("error running example: {}", err);
            process::exit(1);
        }
        Ok(nodes) => {
            for (key, value) in nodes.iter() {
                println!("{:?}: {} ", key, value.len());
            }
        }
    }
}