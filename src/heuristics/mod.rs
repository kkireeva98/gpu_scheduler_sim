use rand::distr::Distribution;
use rand::{random, Rng};
use rand::distr::Uniform;
use rand::prelude::IteratorRandom;
use crate::evaluator::*;
use crate::types::*;


// Simple Deciders

pub fn max_tasks_arrived( evaluator: &Evaluator, metrics: &Metrics ) -> bool {
    metrics.tasks_scheduled + metrics.tasks_delayed >= 100
}

pub fn backlog_size( evaluator: &Evaluator, metrics: &Metrics ) -> bool {
    evaluator.workload.backlog_size() >= 1
}

// Simple Scheduler

pub fn random_scheduler( evaluator: &Evaluator, task: PodSpec ) -> Option<(NODE, Option<NUM>)> {
    let cluster = evaluator.cluster.clone();

    let nodes = cluster.filter_nodes( task.clone() );
    let selected_node = nodes.choose( &mut cluster.rng.borrow_mut() );

    // We have no nodes left to pick from!
    if selected_node.is_none() {
        return None;
    }

    let selected_node = selected_node.unwrap().borrow();
    let id = selected_node.spec.id;

    let opt_ind: Option<NUM> = if task.num_gpu != 1 { None } else {

        selected_node.gpu_rem.iter()
            .enumerate()
            .filter(|&(i, &gpu)| { task.gpu_milli <= gpu })
            .map(|(i, _)| i)
            .choose(&mut cluster.rng.borrow_mut())
    };

    Some((id, opt_ind))
}