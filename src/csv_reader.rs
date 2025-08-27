#![allow(unused)]

use std::error::Error;
use std::fmt::Formatter;
use std::str::FromStr;
use serde::{de, Deserialize, Deserializer};


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
type POD = usize;

type CPU = u32;
type MEM = u32;
type GPU = u32;
type MODEL = Option<GpuSpec>;
type MODELS = Vec<GpuSpec>;


#[derive(Debug, Clone)]
#[derive(serde::Deserialize)]
pub struct NodeSpec {
    cpu_milli: CPU,
    memory_mib: MEM,
    #[serde(rename = "gpu")]
    num_gpu: GPU,
    model: MODEL,
}


#[derive(Debug, Clone)]
#[derive(serde::Deserialize)]
pub struct PodSpec {
    cpu_milli: CPU,
    memory_mib: MEM,
    num_gpu: GPU,
    gpu_milli: GPU,
    #[serde(rename = "gpu_spec")]
    #[serde(deserialize_with = "parse_multi_spec")]
    model: MODELS
}

pub fn process_csv<T, R, F>(csv_reader: R, mut callback: F ) -> Result<(), Box<dyn Error>>
where
    F: FnMut(usize, T) -> Result<(), Box<dyn Error>>,
    R: std::io::Read,
    T: for<'de> Deserialize<'de>,
{
    let mut rdr = csv::Reader::from_reader(csv_reader);

    rdr.deserialize().enumerate().try_for_each( |(i, result)| {
        let record = result?;

        callback( i, record )
    })
}


// Handle special case for parsing GPU Specs for Pod Specs, ei V100M16|V100M32
fn parse_multi_spec<'de, D>( deserializer : D ) -> Result< MODELS, D::Error> where
    D: Deserializer<'de> {

    struct MultiSpecVisitor;

    impl<'de> de::Visitor<'de> for MultiSpecVisitor {
        type Value = MODELS;

        fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
            formatter.write_str("valid gpu_spec string")
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> where E: de::Error {
            if v.is_empty() { return Ok(Vec::new()); }

            v.split("|")
                .map(|str| {
                    GpuSpec::try_from(str).map_err(|_| E::custom(format!("invalid gpu_spec: {}", str)))
                }).collect()
        }
    }

    deserializer.deserialize_any(MultiSpecVisitor)
}

impl TryFrom<&str> for GpuSpec {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "A10" => Ok(GpuSpec::A10),
            "G2" => Ok(GpuSpec::G2),
            "G3" => Ok(GpuSpec::G3),
            "P100" => Ok(GpuSpec::P100),
            "T4" => Ok(GpuSpec::T4),
            "V100M16" => Ok(GpuSpec::V100M16),
            "V100M32" => Ok(GpuSpec::V100M32),
            _ => Err(()),
        }
    }
}


#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::env;
    use std::fmt::Debug;
    use std::fs::File;
    use rstest::rstest;
    use super::*;

    #[rstest]
    #[case("sn,cpu_milli,memory_mib,gpu,model
        openb-node-0021,64000,262144,2,P100
        openb-node-0022,128000,786432,8,G3
        openb-node-0023,96000,786432,8,V100M32
        openb-node-0024,96000,786432,8,V100M32
        openb-node-0025,32000,131072,4,V100M16"
    )]
    #[case("sn,cpu_milli,memory_mib,gpu,model
        openb-node-1480,96000,524288,0,
        openb-node-1481,96000,524288,0,
        openb-node-1328,128000,1048576,1,A10
        openb-node-1329,128000,1048576,1,A10
        openb-node-0234,96000,393216,8,G2"
    )]
    fn read_node_spec_csv( #[case] node_spec_csv: &str ) {

        let buffer = node_spec_csv.as_bytes();

        process_csv(buffer, |i, record: NodeSpec| {
            println!("{}: {:?}", i, record);
            Ok(())
        }).unwrap();
    }

    #[rstest]
    #[case("name,cpu_milli,memory_mib,num_gpu,gpu_milli,gpu_spec,qos,pod_phase,creation_time,deletion_time,scheduled_time
        openb-pod-0095,4152,10600,1,810,,BE,Failed,10019860,10024488,10019861
        openb-pod-0096,18708,64512,1,1000,,LS,Pending,10019975,10020052,
        openb-pod-0097,8000,30517,1,320,,BE,Pending,10020010,10020025,
        openb-pod-0098,8000,30517,1,320,,BE,Running,10020315,10020891,10020315"
    )]
    #[case("name,cpu_milli,memory_mib,num_gpu,gpu_milli,gpu_spec,qos,pod_phase,creation_time,deletion_time,scheduled_time
        openb-pod-7561,3152,5600,1,810,,BE,Failed,12811529,12812203,12811775
        openb-pod-7562,11400,77824,1,1000,,LS,Running,12811533,12814085,12811570
        openb-pod-7563,11300,49152,1,1000,V100M16|V100M32,LS,Running,12811565,12811794,12811675"
    )]
    fn read_pod_spec_csv( #[case] node_spec_csv: &str ) {

        let buffer = node_spec_csv.as_bytes();

        process_csv(buffer, |i, record: PodSpec| {
            println!("{}: {:?}", i, record);
            Ok(())
        }).unwrap();
    }


    type NodeSpecKey = (MODEL, GPU);
    type NodeSpecValue = Vec<NODE>;

    type NodeHash = HashMap<NodeSpecKey, NodeSpecValue>;

    #[rstest]
    #[case("gpu_nodes.csv")]
    #[case("all_nodes.csv")]
    fn read_node_spec_csv_file(#[case] file_name: &str) {

        let prefix = "clusterdata/node_data/";
        let file_path = prefix.to_owned() + file_name;

        println!("file_path: {}", file_path);

        let file = File::open(file_path).expect("file not found" );

        let mut nodes_hash = NodeHash::new();
        let mut nodes: Vec<NodeSpec> = Vec::new();

        process_csv(file, |i, record: NodeSpec| {
            nodes.push(record.clone());

            let key = (record.model, record.num_gpu);
            let value = i;

            nodes_hash.entry(key).or_insert(Vec::new()).push(value);

            Ok(())

        }).unwrap();

        for (key, value) in nodes_hash.iter() {
            println!("{:?}: {} ", key, value.len());
        }
    }
}