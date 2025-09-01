use std::io::Read;
use std::rc::Rc;
use rand::Rng;
use crate::csv_reader::process_csv;
use crate::types::*;

pub mod workload;
pub mod cluster;

use workload::*;
use cluster::*;


// Decides which node to bind the next task to
type Scheduler = fn( evaluator: &Evaluator, task: PodSpec ) -> Option<(NODE, Option<NUM>)>;

// Decides when to deploy workload instead of waiting for more tasks
type Decider = fn( evaluator: &Evaluator ) -> bool;



#[public]
struct Evaluator
{
    // Dependency injection functions that alter Evaluator behavior
    // Can make non-mutable queries on Cluster and Workload objects
    // Evaluator applies decision and evaluates performance metrics
    scheduler: Scheduler,
    decider: Decider,

    // Tasks to be scheduled
    workload: Workload,

    specs: Vec<NodeSpec>,
    cluster: Cluster,
}

const NUM_LOOPS: u32 = 100;

impl Evaluator {

    pub fn new(
        scheduler: Scheduler,
        decider: Decider,
        workload_reader: impl Read,
        cluster_reader: impl Read,
    ) -> Self {
        let workload = WorkloadStruct::new(String::from("workload"), workload_reader);
        let workload = Rc::new(workload);

        let specs = ClusterStruct::node_specs_from_reader(cluster_reader);
        let cluster= ClusterStruct::new(String::from("cluster"), &specs, workload.clone());
        let cluster = Rc::new(cluster);

        Self { scheduler, decider, workload, specs, cluster }
    }

    pub fn schedule_and_deploy(&mut self) {

        let decider_func = self.decider;
        let scheduler_func = self.scheduler;

        loop {
            // Sample task
            let task: PodSpec = self.workload.next_task();

            match scheduler_func(self, task.to_owned()) {
                None => {
                    // Scheduling failed. Add to backload for next deployment
                    self.workload.push_backlog(task.clone());

                    self.workload.update_metrics(task, false);
                },
                Some((n, opt_g)) => {
                    // Scheduling succeeded. Apply to cluster
                    self.cluster.bind_task(task.clone(), n, opt_g);

                    self.workload.update_metrics(task, true);
                },

            }
            // Interrogate cluster and update performance metrics

            if decider_func(self) { break; }
        }

        // Tasks "deployed"
        self.cluster.deploy();

        // Hack to reset the cluster to a new one with fresh data
        let cluster= ClusterStruct::new(String::from("cluster"), &self.specs, self.workload.clone());
        let cluster = Rc::new(cluster);

        self.cluster = cluster;

        self.workload.deploy();
    }

    pub fn evaluate(&mut self) {

        // Average metrics over a number of loops, to reduce statistical error
        for batch_num in 0..NUM_LOOPS {
            self.schedule_and_deploy();
            println!()
        }

    }

}