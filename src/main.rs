#![allow(unused)]
#[macro_use]
extern crate public;


mod csv_reader;
mod types;


const NUM_LOOPS: u32 = 10;

fn sample_task() {
    // consume from backlog if not empty
    
    // Sample from trace file
}


fn schedule_workload() {
    let condition = true;
    
    while (condition) {
        sample_task();
        
        // heuristic.schedule(task)
        
        // update condition
        // update metrics
    }

    // return metrics
}


fn main() {
    // Init scheduler

    // Empty backlog queue

    // Evaluation loop
    for _ in 0..NUM_LOOPS {
        schedule_workload();

        // Update performance
    }

    // Report overall performance
}