#![allow(dead_code)]

use crate::evaluator::*;

mod score_by;
pub mod simple_schedulers;

// Simple Deciders

pub fn max_delayed( evaluator: &Evaluator ) -> bool {
    let metrics = evaluator.workload.metrics.borrow();

    metrics.tasks_delayed >= 500
}

pub fn max_tasks_arrived( evaluator: &Evaluator ) -> bool {
    let metrics = evaluator.workload.metrics.borrow();

    // Allow for release valve by checking that delayed tasks are not too high
    metrics.tasks_arrived >= 10000 || max_delayed( evaluator)
}