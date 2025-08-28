

#[derive(Debug, Clone)]
#[derive(serde::Deserialize)]
#[derive(Eq, Hash, PartialEq)]
pub enum GpuSpec{
    A10,
    G2,
    G3,
    P100,
    T4,
    V100M16,
    V100M32,
}


pub type NODE = usize;
pub type POD = usize;
pub type CPU = u32;
pub type MEM = u32;
pub type GPU = u32;
pub type MODEL = Option<GpuSpec>;
pub type MODELS = Vec<GpuSpec>;


#[derive(Debug, Clone)]
#[derive(serde::Deserialize)]
#[public]
struct NodeSpec {
    cpu_milli: CPU,
    memory_mib: MEM,
    #[serde(rename = "gpu")]
    num_gpu: GPU,
    model: MODEL,
}


#[derive(Debug, Clone)]
#[derive(serde::Deserialize)]
#[public]
struct PodSpec {
    cpu_milli: CPU,
    memory_mib: MEM,
    num_gpu: GPU,
    gpu_milli: GPU,
    #[serde(rename = "gpu_spec")]
    #[serde(deserialize_with = "crate::csv_reader::parse_multi_spec")]
    model: MODELS
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