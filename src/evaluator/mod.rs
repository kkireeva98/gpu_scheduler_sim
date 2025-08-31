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
type Scheduler = fn( evaluator: &Evaluator, task: PodSpec ) -> Option<NODE>;

// Decides when to deploy workload instead of waiting for more tasks
type Decider = fn( evaluator: &Evaluator, metrics: &Metrics ) -> bool;


#[derive(Debug, Clone)]
#[derive(Default)]
#[public]
struct Metrics {
    tasks_scheduled: u32,
    tasks_delayed: u32,

    total_cpu: CPU,
    total_mem: MEM,
    total_gpu: GPU,
}

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
    cluster: Cluster,

    // Reset cluster by just replacing with copy of premade fresh cluster
    cluster_fresh: ClusterStruct
}

const NUM_LOOPS: u32 = 4;

impl Evaluator {

    pub fn new(
        scheduler: Scheduler,
        decider: Decider,
        workload_reader: impl Read,
        cluster_reader: impl Read,
    ) -> Self {
        let workload = WorkloadStruct::new(String::from("workload"), workload_reader);
        let workload = Rc::new(workload);

        let cluster_fresh= ClusterStruct::new(String::from("cluster"), cluster_reader, workload.clone());
        let cluster = Rc::new(cluster_fresh.clone());

        Self { scheduler, decider, workload, cluster, cluster_fresh }
    }

    fn schedule_and_deploy(&mut self) -> Metrics {

        // Performance metrics for this round
        let mut metrics = Metrics::default();

        let decider_func = self.decider;
        let scheduler_func = self.scheduler;

        loop {
            // Sample task
            let task: PodSpec = self.workload.next_task();

            match scheduler_func(self, task.to_owned()) {
                None => {
                    // Scheduling failed. Add to backload for next deployment
                    metrics.tasks_delayed += 1;
                    self.workload.push_backlog(task);

                    //println!("Task {} delayed", m);
                },
                Some(n) => {
                    // Scheduling succeeded. Apply to cluster
                    metrics.tasks_scheduled += 1;
                    metrics.total_cpu += task.cpu_milli;
                    metrics.total_mem += task.memory_mib;
                    metrics.total_gpu += task.gpu_milli;

                    //println!("Task scheduled {}", self.workload.task(m));
                },

            }
            // Interrogate cluster and update performance metrics

            if decider_func(self, &metrics) { break; }
        }

        // Tasks "deployed". Reset for next round
        self.workload.drain_backlog(true);
        self.cluster = Rc::new(self.cluster_fresh.clone());

        println!("{:?}", metrics);
        metrics
    }

    pub fn evaluate(&mut self) {

        // Average metrics over a number of loops, to reduce statistical error
        for batch_num in 0..NUM_LOOPS {
            self.schedule_and_deploy();

            // Update overall metrics
        }

        // Report overall metrics

        println!("{}", self.workload);

    }

}