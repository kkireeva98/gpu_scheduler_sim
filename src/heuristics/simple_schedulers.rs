use rand::prelude::IteratorRandom;
use crate::evaluator::*;
use crate::heuristics::score_by::ScoreBy;
use crate::types::*;


// Simple Schedulers

pub fn random_scheduler( evaluator: &Evaluator, task: PodSpec ) -> Option<SchedulingPick> {
    let cluster = evaluator.cluster.clone();

    let nodes = cluster.filter_nodes( task.clone() );
    let node_opt = nodes.choose( &mut cluster.rng.borrow_mut() );

    // We have no nodes left to pick from!
    if node_opt.is_none() {
        return None;
    }

    let selected_node  = node_opt.unwrap();
    let node = selected_node.borrow().clone();

    let gpus = node
        .filter_gpus( task.clone() )
        .choose_multiple(&mut cluster.rng.borrow_mut(), task.num_gpu );

    Some((selected_node, gpus))
}


const MODEL_PENALTY: SCORE = 10000;
pub fn dot_product_scheduler( evaluator: &Evaluator, task: PodSpec ) -> Option<SchedulingPick> {
    let task = task.to_owned();

    let cluster = evaluator.cluster.clone();

    // Filter and Score nodes
    let nodes = cluster.filter_nodes( task.clone() );
    let score_func = | node_ref: NodeInfo | -> (NodeInfo, SCORE) {
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

    let node_opt = nodes.score_by_max(score_func);

    // We have no nodes left to pick from!
    if node_opt.is_none() { return None; }

    let selected_node  = node_opt.unwrap();
    let node = selected_node.borrow().clone();

    // Filter and Score GPUs
    let gpus = node.filter_gpus(task.clone());
    let gpus: Vec<GpuInfo> = if task.single_gpu() {
        vec![gpus.score_by_min( |gpu| {
                (gpu.clone(),gpu.borrow().gpu_milli as SCORE)
            }).unwrap()]
    } else {
        gpus.take(task.num_gpu).collect()
    };

    Some((selected_node, gpus))
}


pub fn best_fit_scheduler( evaluator: &Evaluator, task: PodSpec ) -> Option<SchedulingPick> {
    let task = task.to_owned();

    let cluster = evaluator.cluster.clone();

    // Filter and Score nodes
    let nodes = cluster.filter_nodes( task.clone() );
    let score_func = | node_ref: NodeInfo | -> (NodeInfo, SCORE) {
        let node = node_ref.borrow();
        let mut score : SCORE = SCORE::default();

        score += (node.cpu_rem - task.cpu_milli) as SCORE;
        score += (node.mem_rem - task.memory_mib) as SCORE;
        score += (node.gpu_unallocated - task.gpu_milli ) as SCORE;

        // Prefer not to run on model machines
        if task.model.is_empty() && !node.spec.model.is_empty() {
            score -= MODEL_PENALTY;
        }

        (node_ref.clone(), score)
    };

    let node_opt = nodes.score_by_min(score_func);

    // We have no nodes left to pick from!
    if node_opt.is_none() { return None; }

    let selected_node  = node_opt.unwrap();
    let node = selected_node.borrow().clone();

    // Filter and Score GPUs
    let gpus = node.filter_gpus(task.clone());
    let gpus: Vec<GpuInfo> = if task.single_gpu() {
        vec![gpus.score_by_min( |gpu| {
            (gpu.clone(),gpu.borrow().gpu_milli as SCORE)
        }).unwrap()]
    } else {
        gpus.take(task.num_gpu).collect()
    };


    Some((selected_node, gpus))
}