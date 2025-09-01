use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Read;
use std::rc::Rc;
use rand::distr::Uniform;
use rand::prelude::ThreadRng;
use crate::types::*;
use crate::evaluator::*;
use crate::evaluator::workload::*;

type FragDelta = HashMap<PodSpecKey, Vec<GPU>>;

#[derive(Debug, Clone)]
#[public]
struct ClusterStruct {
    name: String,

    rng: RefCell<ThreadRng>,

    workload: Workload,

    num_nodes: NODE,
    nodes: Vec<NodeInfo>,

    // Key Optimization:
    // We keep a precomputed vector of fragmentation deltas for each task per node,
    // and only update when binding a task to a node.
    frag_delta: FragDelta,

    metrics: RefCell<NodeMetrics>,

}
pub type Cluster = Rc<ClusterStruct>;


#[derive(Debug, Clone)]
#[derive(Default)]
#[public]
struct NodeMetrics {
    gpu_total: GPU,
    gpu_unallocated: GPU,
    frag_total: GPU,

    frag_rate: f64,     // frag_total / gpu_unallocated
    alloc_rate: f64,    // 1 - gpu_unallocated / gpu_total
}

impl ClusterStruct {

    pub fn node_specs_from_reader( node_reader: impl  Read ) -> Vec<NodeSpec> {
        let mut specs = Vec::new();

        process_csv( node_reader, | i, mut record: NodeSpecStruct | {
            record.id = i;
            record.gpu_milli = record.num_gpu as GPU * GPU_MILLI;
            let spec = Rc::new( record );

            specs.push( spec);
            Ok(())

        }).expect("Failed to process Cluster CSV");

        specs
    }


    pub fn new( name: String, specs: &Vec<NodeSpec>, workload: Workload ) -> Self {
        let num_nodes = specs.len();
        let mut nodes = Vec::with_capacity(num_nodes);

        let mut metrics = NodeMetrics::default();
        let frag_delta = Default::default();

        for spec in specs {

            metrics.gpu_unallocated += spec.gpu_milli;
            metrics.gpu_total += spec.gpu_milli;

            let node = NodeInfoStruct {
                spec: spec.clone(),
                cpu_rem: spec.cpu_milli,
                mem_rem: spec.memory_mib,
                gpu_rem: vec![GPU_MILLI; spec.num_gpu],

                gpu_full: spec.num_gpu,
                gpu_part: 0,

                gpu_unallocated: spec.gpu_milli,
                gpu_frag: 0,
            };

            nodes.push( Rc::new(RefCell::new(node)) );

            // TODO: calculate frag_delta
        }


        let rng = RefCell::new(rand::rng());
        let metrics = RefCell::new(metrics);

        Self {
            name,
            rng,
            workload,
            num_nodes, nodes,
            frag_delta,
            metrics,
        }
    }

    // Basic filtering pass. Checks availability of resources, and model specs if provided
    pub fn filter_nodes( &self, task: PodSpec ) -> impl Iterator<Item=&NodeInfo>  {
        self.nodes.iter().filter( move | node| {
            let node = node.borrow();
            let scalar_resources: bool =
                task.cpu_milli <= node.cpu_rem &&
                task.memory_mib <= node.mem_rem;

            let gpu_resources: bool =
                task.gpu_milli <= node.gpu_part ||
                task.num_gpu <= node.gpu_full;

            let model_match: bool =
                task.model.is_empty() ||
                task.model.intersects( node.spec.model.clone() );

            scalar_resources && gpu_resources && model_match
        })
    }

    pub fn bind_task(&self, task: PodSpec, n: NODE, opt_g: Option<NUM> ) {
        let mut node = self.nodes[n].borrow_mut();

        node.cpu_rem -= task.cpu_milli;
        node.mem_rem -= task.memory_mib;

        if task.num_gpu == 1 {
            // Consume a portion of gpu g
            let g = opt_g.unwrap();
            node.gpu_rem[g] -= task.gpu_milli;

        } else {
            // Consume the first num_gpu free GPUs
            let mut gpu_vec = node.gpu_rem.clone();

            node.gpu_rem.iter()
                .enumerate()
                .filter(|&(_, &gpu)| gpu == GPU_MILLI)
                .take(task.num_gpu)
                .for_each(|(i, _)| gpu_vec[i] = 0);

            node.gpu_rem = gpu_vec
        }

        node.gpu_full = node.gpu_rem.iter()
            .filter(|&&gpu| { gpu == GPU_MILLI })
            .count();

        node.gpu_part  = *node.gpu_rem.iter()
            .filter(|&&gpu| { gpu < GPU_MILLI })
            .max()
            .unwrap_or(&0);

        node.gpu_unallocated -= task.gpu_milli;

        // TODO: GPU frag and GPU frag delta


        // Cluster metrics
        let mut metrics = self.metrics.borrow_mut();

        metrics.gpu_unallocated -= task.gpu_milli;
        metrics.alloc_rate = 1f64 - (metrics.gpu_unallocated as f64 / metrics.gpu_total as f64);
    }

    pub fn deploy(&self) {
        println!("{}", self.metrics.borrow());

    }
}

impl std::fmt::Display for ClusterStruct {

    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f);
        writeln!(f, "Cluster ({})", self.name)?;

        let write_node = | node: &NodeInfo | -> std::fmt::Result {
            writeln!(f, "{}", node.borrow())
        };

        self.nodes.iter().try_for_each(write_node)
    }
}

impl std::fmt::Display for NodeMetrics {

    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Node Metrics:");

        writeln!(f, "Total GPU: {:.1}", self.gpu_total as f64 / GPU_MILLI as f64 )?;

        writeln!(f, "Unallocated GPU resources:  {:.1} -- ({:.2}% allocation rate)",
                 self.gpu_unallocated as f64 / GPU_MILLI as f64,
                 self.alloc_rate * 100.0 )
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use rand::prelude::{IndexedRandom, IteratorRandom};
    use rstest::{fixture, rstest};
    use rstest_reuse::{self, *};
    use super::*;


    #[fixture]
    fn workload() -> WorkloadStruct {
        let str =
        "name,cpu_milli,memory_mib,num_gpu,gpu_milli,gpu_spec,qos,pod_phase,creation_time,deletion_time,scheduled_time
        openb-pod-4447,8200,33792,1,1000,A10|T4,LS,Failed,11724207,11725419,11724207
        openb-pod-0615,11400,48128,1,1000,A10|G2|T4|V100M16|V100M32,LS,Running,10265011,10274316,10265045
        openb-pod-2449,4000,15258,1,50,P100,BE,Running,10907668,10950678,10907676
        openb-pod-0000,12000,16384,1,1000,,LS,Running,0,12537496,0
        openb-pod-2263,32200,132096,4,1000,,LS,Failed,10814729,10815277,10814729
        openb-pod-0017,88000,327680,8,1000,,Burstable,Succeeded,9437497,10769854,9437497
        openb-pod-0016,32000,65536,0,0,,LS,Running,8962274,12902960,8962277
        openb-pod-0025,4000,15258,1,110,,LS,Running,9823530,10197645,9823530
        openb-pod-0194,4000,15258,1,220,,BE,Running,10077065,10084784,10077066
        openb-pod-7432,8000,30517,1,470,,BE,Pending,12791960,12792838,
        openb-pod-0505,3152,5600,1,810,,BE,Failed,10212626,10212773,10212626";

        WorkloadStruct::new(String::from("workload"), str.as_bytes())
    }

    #[fixture]
    fn specs() -> Vec<NodeSpec> {
        let node_reader = File::open(&"clusterdata/node_data/all_nodes.csv").expect("node file not found");

        ClusterStruct::node_specs_from_reader(node_reader)
    }

    #[rstest]
    fn test_create( specs: Vec<NodeSpec>, workload: WorkloadStruct ) {
        let workload = Rc::new(workload);
        let mut cluster = ClusterStruct::new(String::from("cluster"), &specs, workload.clone());

        println!("Cluster: {}", cluster);

    }

    #[rstest]
    fn test_filter( specs: Vec<NodeSpec>, workload: WorkloadStruct ) {

        let workload = Rc::new(workload);
        let mut cluster = ClusterStruct::new(String::from("cluster"), &specs, workload.clone());

        for task in workload.tasks.iter() {
            let nodes: Vec<&NodeInfo> = cluster.filter_nodes( task.clone() ).collect();
            println!("Nodes available: {} \t--- task {}", nodes.len(), task);
            assert_ne!(nodes.len(), 0);
        }
    }

    #[rstest]
    fn test_bind( specs: Vec<NodeSpec>, workload: WorkloadStruct ) {

        let workload = Rc::new(workload);
        let mut cluster = ClusterStruct::new(String::from("cluster"), &specs, workload.clone());

        let task = workload.next_task();
        println!("Task:{}\n{}",task.id, task);

        let nodes = cluster.filter_nodes( task.clone() );
        let id = {
            let selected_node = nodes.choose( &mut cluster.rng.borrow_mut() ).unwrap().borrow();
            selected_node.spec.id
        };

        {
            let node = cluster.nodes[id].borrow();
            println!("Before bind:{}\n{}", node.spec.id, node);
        }

        let opt_ind = match task.num_gpu {
            1 => Some(0),
            _ => None,
        };

        cluster.bind_task( task.clone(), id, opt_ind);

        {
            let node = cluster.nodes[id].borrow();
            println!("Before bind:{}\n{}", node.spec.id, node);
        }

        println!("{}", cluster.metrics.borrow().alloc_rate)
    }
}