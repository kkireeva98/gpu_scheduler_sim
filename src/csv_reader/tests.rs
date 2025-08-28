use std::collections::HashMap;
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
        openb-pod-7852,11300,49152,1,1000,V100M16|V100M32,LS,Running,12844303,12844554,12844441
        openb-pod-7854,11300,49152,1,1000,P100|V100M16|V100M32,LS,Running,12844338,12844584,12844441
        openb-pod-7855,11300,49152,1,1000,P100|T4|V100M16|V100M32,LS,Running,12844373,12844681,12844449
        openb-pod-7857,11300,49152,1,1000,T4,LS,Running,12844752,12845090,12844754
        openb-pod-7858,11300,49152,1,1000,G3,LS,Running,12844783,12844942,12844822"
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