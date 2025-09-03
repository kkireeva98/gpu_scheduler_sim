use std::cell::RefCell;
use std::rc::Rc;
use bitflags::bitflags;
use crate::evaluator::Evaluator;

pub type SCORE = u128;

pub type NODE = usize;
pub type POD = usize;

pub type CPU = u64;
pub type MEM = u64;

pub type NUM = usize;
pub type GPU = u64;

pub type MODEL = GpuSpec;

pub const GPU_MILLI : GPU = 1000;
pub const MEM_MIB : MEM = 1024;
pub const CPU_MILLI : CPU = 1000;

// Scheduler decides which node and gpu(s) to assing a task to
pub type SchedulingPick = (NodeInfo, Vec<GpuInfo>);
pub type ScheduleFunc = fn(evaluator: &Evaluator, task: PodSpec ) -> Option<SchedulingPick>;

// Decides when to deploy workload instead of waiting for more tasks
pub type DeployFunc = fn(evaluator: &Evaluator ) -> bool;

#[derive(Debug, Clone)]
#[derive(serde::Deserialize)]
#[derive(Eq, Hash, PartialEq)]
#[derive(Default)]
pub struct GpuSpec(u64);

bitflags! {
    impl GpuSpec: u64 {
        const A10 = 1;
        const G2 = 2;
        const G3 = 4;
        const P100 = 8;
        const T4 = 16;
        const V100M16 = 32;
        const V100M32 = 64;
    }
}

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

impl PodSpecStruct {
    pub fn single_gpu(&self) -> bool { self.num_gpu == 1 }
}


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

    gpu_rem: Vec<GpuInfo>,
    gpu_full: NUM,
    gpu_part: GPU,

    gpu_unallocated: GPU,
    gpu_frag: GPU,
}
pub type NodeInfo = Rc<RefCell<NodeInfoStruct>>;

#[derive(Debug, Clone)]
#[public]
struct GpuInfoStruct {
    id: NUM,
    gpu_milli: GPU,
}
pub type GpuInfo = Rc<RefCell<GpuInfoStruct>>;


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
               if self.single_gpu() {
                   self.gpu_milli as f64 / GPU_MILLI as f64
               } else { 
                   self.num_gpu as f64
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

        let write_gpu = | gpu: &GpuInfo | -> std::fmt::Result {
            let frac = (gpu.borrow().gpu_milli / (GPU_MILLI / 10)) as usize;

            let free = vec!['▒'; frac];
            let used = vec!['▇'; 10 - frac];

            let gpu_str: String = [used, free ].concat().iter().collect();
            write!(f, "[\t{}\t]", gpu_str )
        };

        self.gpu_rem.iter().try_for_each(write_gpu)
    }
}