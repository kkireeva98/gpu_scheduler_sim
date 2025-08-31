use std::cell::RefCell;
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

pub type NUM = usize;
pub type GPU = u32;

pub type MODEL = GpuSpec;

pub const GPU_MILLI : GPU = 1000;
pub const MEM_MIB : MEM = 1024;
pub const CPU_MILLI : CPU = 1000;

#[derive(Debug, Clone)]
#[derive(serde::Deserialize)]
#[derive(PartialEq, Eq)]
#[derive(Hash)]
#[public]
struct PodSpecStruct {
    #[serde(skip)]
    id: POD,

    // Data
    cpu_milli: CPU,
    memory_mib: MEM,
    num_gpu: NUM,
    gpu_milli: GPU,
    #[serde(rename = "gpu_spec")]
    #[serde(deserialize_with = "crate::csv_reader::parse_gpu_spec")]
    #[serde(default)]
    model: MODEL
}
pub type PodSpec = Rc<PodSpecStruct>;
pub type PodSpecKey = PodSpecStruct;


#[derive(Debug, Clone)]
#[derive(serde::Deserialize)]
#[derive(PartialEq, Eq)]
#[public]
struct NodeSpecStruct {
    #[serde(skip)]
    id: NODE,

    // Data
    cpu_milli: CPU,
    memory_mib: MEM,
    #[serde(rename = "gpu")]
    num_gpu: NUM,
    #[serde(skip)]
    gpu_milli: GPU,
    #[serde(deserialize_with = "crate::csv_reader::parse_gpu_spec")]
    model: MODEL,
}
pub type NodeSpec = Rc<NodeSpecStruct>;


#[derive(Debug, Clone)]
#[public]
struct NodeInfoStruct {
    spec: NodeSpec,

    cpu_rem: CPU,
    mem_rem: MEM,

    gpu_rem: Vec<GPU>,
    gpu_full: NUM,
    gpu_part: GPU,

    gpu_unallocated: GPU,
    gpu_frag: GPU,
}
pub type NodeInfo = Rc<RefCell<NodeInfoStruct>>;


impl std::fmt::Display for GpuSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        bitflags::parser::to_writer(self, f)
    }
}

impl std::fmt::Display for NodeSpecStruct {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{: >4.1} cpu\t{: >4.1} GiB\t{: >4.1} GPU\t{: <4}",
               self.cpu_milli as f64 / CPU_MILLI as f64,
               self.memory_mib as f64 / MEM_MIB as f64,
               self.num_gpu as f64,
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
               self.cpu_milli as f64 / CPU_MILLI as f64,
               self.memory_mib as f64 / MEM_MIB as f64,
               match self.num_gpu {
                   1 => self.gpu_milli as f64 / GPU_MILLI as f64,
                   n => n as f64,
               },
               match &self.model {
                   GpuSpec(0) => "_",
                   model => &model.to_string(),
               },
        )
    }
}

impl std::fmt::Display for NodeInfoStruct {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "SPECS:\t{}", self.spec)?;
        writeln!(f, "REMAIN:\t{: >4.1} cpu\t{: >4.1} GiB\t{: >4.1} GPU ({}, {:.1})",
             self.cpu_rem as f64 / CPU_MILLI as f64,
             self.mem_rem as f64 / MEM_MIB as f64,
             self.gpu_unallocated as f64 / GPU_MILLI as f64,
             self.gpu_full, self.gpu_part as f64 / GPU_MILLI as f64,
        )?;

        let gpu_str = | gpu: GPU | -> String  {
            let frac = (gpu / (GPU_MILLI / 10)) as usize;

            let free = vec!['▒'; frac];
            let used = vec!['▇'; 10 - frac];

            [used, free ].concat().iter().collect()
        };

        let write_gpu = | &gpu | -> std::fmt::Result {
            write!(f, "[\t{}\t]", gpu_str(gpu) )
        };

        self.gpu_rem.iter().try_for_each(write_gpu)
    }
}