#![allow(unused)]

use std::rc::Rc;
use bitflags::bitflags;

#[derive(Debug, Clone)]
#[derive(serde::Deserialize)]
#[derive(Eq, Hash, PartialEq)]
#[derive(Default)]
pub struct GpuSpec(u32);

bitflags! {
    impl GpuSpec: u32 {
        const A10 = 1;
        const G2 = 2;
        const G3 = 4;
        const P100 = 8;
        const T4 = 16;
        const V100M16 = 32;
        const V100M32 = 64;
    }
}


pub type NODE = usize;
pub type POD = usize;

pub type CPU = u32;
pub type MEM = u32;

pub type NUM = u32;
pub type GPU = u32;

pub type MODEL = GpuSpec;


#[derive(Debug, Clone)]
#[derive(serde::Deserialize)]
#[derive(PartialEq, Eq)]
#[public]
struct NodeSpec {
    cpu_milli: CPU,
    memory_mib: MEM,
    #[serde(rename = "gpu")]
    num_gpu: NUM,
    #[serde(deserialize_with = "crate::csv_reader::parse_gpu_spec")]
    model: MODEL,
}

#[public]
type PodSpec = Rc<PodSpecStruct>;

#[derive(Debug, Clone)]
#[derive(serde::Deserialize)]
#[derive(PartialEq, Eq)]
#[derive(Hash)]
#[public]
struct PodSpecStruct {
    cpu_milli: CPU,
    memory_mib: MEM,
    num_gpu: NUM,
    gpu_milli: GPU,
    #[serde(rename = "gpu_spec")]
    #[serde(deserialize_with = "crate::csv_reader::parse_gpu_spec")]
    #[serde(default)]
    model: MODEL
}

impl std::fmt::Display for GpuSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        bitflags::parser::to_writer(self, f)
    }
}

impl std::fmt::Display for NodeSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{: >4.1} cpu\t{: >4.1} GiB\t{: >4.1} GPU\t{: <4}",
               self.cpu_milli / 1000,
               self.memory_mib / 1024,
               self.num_gpu,
               match &self.model {
                   GpuSpec(0) => "_",
                   model => &model.to_string(),
               },
        )
    }
}

impl std::fmt::Display for PodSpecStruct {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{: >4.1} cpu\t{: >4.1} GiB\t{: >4.1} GPU\t{: <4}",
               self.cpu_milli as f64 / 1000_f64,
               self.memory_mib as f64 / 1024_f64,
               match self.num_gpu {
                   1 => self.gpu_milli as f64 / 1000_f64,
                   n => n as f64,
               },
               match &self.model {
                   GpuSpec(0) => "_",
                   model => &model.to_string(),
               },
        )
    }
}