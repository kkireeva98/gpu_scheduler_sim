use std::io::Read;
use std::rc::Rc;
use num_traits::Num;
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

    // Compute Nodes
    cluster: Cluster,
}

const NUM_LOOPS: usize = 10;

impl Evaluator {

    pub fn new(
        scheduler: Scheduler,
        decider: Decider,
        workload_reader: impl Read,
        cluster_reader: impl Read,
    ) -> Self {
        let workload = WorkloadStruct::new(String::from("workload"), workload_reader);
        let workload = Rc::new(workload);

        let cluster= ClusterStruct::new(String::from("cluster"), cluster_reader, workload.clone());

        Self { scheduler, decider, workload, cluster }
    }

    pub fn schedule_and_deploy(&mut self) -> ( TaskMetrics, NodeMetrics ){

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

        // Tasks "deployed". Return metrics
        ( self.workload.deploy(), self.cluster.deploy() )
    }

    pub fn evaluate(&mut self) {

        let mut tasks_scheduled = Default::default();
        let mut gpu_unallocated = Default::default();

        // Average metrics over a number of loops, to reduce statistical error
        for batch_num in 0..NUM_LOOPS {
            let (task_m, node_m) = self.schedule_and_deploy();

            tasks_scheduled = update_average(tasks_scheduled, task_m.tasks_scheduled as f64, batch_num ) ;
            gpu_unallocated = update_average(gpu_unallocated, node_m.gpu_unallocated as f64, batch_num);

            println!("Batch {}: tasks scheduled: {}, allocation rate: {:.2}", batch_num + 1, task_m.tasks_scheduled, node_m.alloc_rate * 100.0);
        }

        let gpu_total = self.cluster.metrics.borrow().gpu_total;
        let alloc_ratio = 1.0 - gpu_unallocated / gpu_total as f64;


        println!("Average tasks scheduled: {:.0}", tasks_scheduled );
        println!("Average allocation rate : {:.2}", alloc_ratio * 100.0)
    }

}

fn update_average(prev_avg: f64, x: f64, n: usize ) -> f64 {
    if n == 1  { return x; }

    let frac: f64 = n as f64 / (n + 1) as f64;
    let contrib = x / (n + 1) as f64;

    frac * prev_avg + contrib
}