use std::io::Read;
use std::rc::Rc;
use crate::csv_reader::process_csv;
use crate::types::*;

pub mod workload;
pub mod cluster;

use workload::*;
use cluster::*;



#[public]
struct Evaluator
{
    // Dependency injection functions that alter Evaluator behavior
    // Can make non-mutable queries on Cluster and Workload objects
    // Evaluator applies decision and evaluates performance metrics
    scheduler: ScheduleFunc,
    decider: DeployFunc,

    // Tasks to be scheduled
    workload: Workload,

    // Compute Nodes
    cluster: Cluster,
}

const NUM_LOOPS: usize = 100;

impl Evaluator {

    pub fn new(
        scheduler: ScheduleFunc,
        decider: DeployFunc,
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
                Some(choice) => {
                    // Scheduling succeeded. Apply to cluster
                    self.cluster.bind_task(task.clone(), choice);

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