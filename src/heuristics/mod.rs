use rand::distr::Distribution;
use rand::{random, Rng};
use rand::distr::Uniform;
use rand::prelude::IteratorRandom;
use crate::evaluator::*;
use crate::types::*;

type SCORE = u128;

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

// Simple Schedulers

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

const MODEL_PENALTY: SCORE = 10000;
pub fn dot_product_scheduler( evaluator: &Evaluator, task: PodSpec ) -> Option<(NODE, Option<NUM>)> {
    let task = task.to_owned();

    let cluster = evaluator.cluster.clone();
    let nodes = cluster.filter_nodes( task.clone() );

    let score = | node_ref: &NodeInfo | -> (NodeInfo, SCORE) {
        let node = node_ref.borrow();
        let mut score : SCORE = SCORE::default();

        score += (node.cpu_rem * task.cpu_milli) as SCORE;
        score += (node.mem_rem * task.memory_mib) as SCORE;
        score += (node.gpu_unallocated * task.gpu_milli ) as SCORE;

        // Prefer not to run on model machines
        if task.model.is_empty() && !node.spec.model.is_empty() {
            score -= MODEL_PENALTY;
        }

        (node_ref.clone(), score)
    };

    let node_opt = nodes
        .map( score )
        .max_by_key(|(node, score)| { *score });

    if node_opt.is_none() { return None; }

    let (selected_node, _ )  = node_opt.unwrap();
    let selected_node = selected_node.borrow();

    let id = selected_node.spec.id;

    let opt_ind: Option<NUM> = if task.num_gpu != 1 { None } else {
        selected_node.gpu_rem.iter()
            .enumerate()
            .filter(|&(i, &gpu)| { task.gpu_milli <= gpu })
            .max_by_key(|(i, gpu)| **gpu )
            .map(|(i, _)| i)
    };

    Some((id, opt_ind))
}