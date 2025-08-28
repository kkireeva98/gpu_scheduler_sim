#![allow(unused)]

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
#[public]
struct NodeSpec {
    cpu_milli: CPU,
    memory_mib: MEM,
    #[serde(rename = "gpu")]
    num_gpu: NUM,
    #[serde(deserialize_with = "crate::csv_reader::parse_gpu_spec")]
    model: MODEL,
}


#[derive(Debug, Clone)]
#[derive(serde::Deserialize)]
#[public]
struct PodSpec {
    cpu_milli: CPU,
    memory_mib: MEM,
    num_gpu: NUM,
    gpu_milli: GPU,
    #[serde(rename = "gpu_spec")]
    #[serde(deserialize_with = "crate::csv_reader::parse_gpu_spec")]
    model: MODEL
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