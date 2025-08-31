use rand::distr::Distribution;
use rand::{random, Rng};
use rand::distr::Uniform;
use crate::evaluator::*;
use crate::types::*;


// Simple Deciders

pub fn max_tasks_arrived( evaluator: &Evaluator, metrics: &Metrics ) -> bool {
    metrics.tasks_scheduled + metrics.tasks_delayed >= 100
}

pub fn backlog_size( evaluator: &Evaluator, metrics: &Metrics ) -> bool {
    evaluator.workload.backlog_size() >= 10
}


// Simple Scheduler

pub fn random_scheduler( evaluator: &Evaluator, task: PodSpec ) -> Option<NODE> {
    let mut rng = rand::rng();
    let uniform = Uniform::new(0, 1000).unwrap();

    let n : NODE = uniform.sample(&mut rng);
    if n > 990 { None } else { Some(n) }
}