use rand::Rng;
use crate::csv_reader::process_csv;
use crate::types::*;

pub mod workload;
use workload::*;


// Decides which node to bind the next task to
type Scheduler = fn( evaluator: &Evaluator, m: POD ) -> Option<NODE>;

// Decides when to deploy workload instead of waiting for more tasks
type Decider = fn( evaluator: &Evaluator, metrics: &Metrics ) -> bool;

#[derive(Default)]
pub struct Cluster {

}

#[derive(Debug, Clone)]
#[derive(Default)]
#[public]
struct Metrics {
    tasks_scheduled: u32,
    tasks_delayed: u32,
    tasks_arrived: u32,

    alloc_frac: f64,
    frag_frac: f64,
}


pub struct Evaluator
{
    // Dependency injection functions that alter Evaluator behavior
    // Can make non-mutable queries on Cluster and Workload objects
    // Evaluator applies decision and evaluates performance metrics
    scheduler: Scheduler,
    decider: Decider,

    // Tasks to be scheduled
    pub workload: Workload,

    // Nodes specs
    pub cluster: Cluster,
}

const NUM_LOOPS: u32 = 4;

impl Evaluator {

    pub fn new( scheduler: Scheduler, decider: Decider, workload: Workload, cluster: Cluster) -> Self {

        Self { scheduler, decider, workload, cluster }
    }

    fn schedule_and_deploy(&mut self) -> Metrics {

        // Performance metrics for this round
        let mut metrics = Metrics::default();

        let decider_func = self.decider;
        let scheduler_func = self.scheduler;

        loop {
            // Sample task
            let m: POD = self.workload.next_task();
            metrics.tasks_arrived += 1;

            match scheduler_func(self, m) {
                None => {
                    // Scheduling failed. Add to backload for next deployment
                    metrics.tasks_delayed += 1;
                    self.workload.add_to_backlog(m);

                    //println!("Task {} delayed", m);
                },
                Some(n) => {
                    // Scheduling succeeded. Apply to cluster
                    metrics.tasks_scheduled += 1;

                    //println!("Task scheduled {}", self.workload.task(m));
                },

            }
            // Interrogate cluster and update performance metrics

            if decider_func(self, &metrics) { break; }
        }

        // Tasks "deployed". Clear cluster for next round, reset backlog
        self.workload.unlock_backlog();

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