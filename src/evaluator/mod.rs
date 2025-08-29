use crate::csv_reader::process_csv;
use crate::types::*;

mod workload;
use workload::*;

trait Scheduler {
    fn new() -> Self;

    fn schedule();

    fn run_workload();

}



struct Cluster {

}


struct Evaluator<S>
where S : Scheduler
{

    // Scheduler implementing heuristic to be tested
    scheduler: S,

    // Tasks to be scheduled
    workload: Workload,

    // Nodes specs
    cluster: Cluster,

    // Performance metrics
    batch_num: u32,

    tasks_scheduled: u32,
    tasks_delayed: u32,
    tasks_arrived: u32,

    alloc_frac: f64,
    frag_frac: f64,
}


impl<S: Scheduler> Evaluator<S> {

    fn new() -> Self {

        todo!()
    }

}