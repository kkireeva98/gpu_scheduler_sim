#![allow(unused)]

use std::collections::{HashMap, VecDeque};
use std::io::Read;
use rand::prelude::*;


use super::*;


type PodSpecKey = PodSpec;
type PodSpecValue = usize;

pub struct Workload {

    name : String,
    rng: ThreadRng,

    backlog: VecDeque<PodSpec>,

    num_tasks: usize,
    task_count: HashMap<PodSpecKey, PodSpecValue>,

    tasks: Vec<PodSpec>,
}

impl Workload {
    fn new( name: String, pod_csv : impl Read )  -> Self {

        let rng = rand::rng();
        let backlog = VecDeque::new();
        let mut task_count = HashMap::new();
        let mut tasks = Vec::new();

        process_csv(pod_csv, |_, record: PodSpec| {

            tasks.push(record.clone());
            *task_count.entry(record).or_insert(0) += 1;

            Ok(())

        }).expect("Failed to process Workload CSV");

        let num_tasks = tasks.len();

        Self {name, rng, backlog, num_tasks, task_count, tasks }
    }

    fn next_task(&mut self) -> PodSpec {
        if let Some(m) = self.backlog.pop_front() {
            return m;
        }

        // Alternative is to use rand::distr::weighted_index to sample based on counts
        // This is much simpler.
        self.tasks.choose(&mut self.rng).unwrap().clone()
    }

    fn task_count( &self, m : PodSpec ) -> usize {
        let &count = self.task_count.get(&m).unwrap_or(&0);
        count
    }


    fn task_freq( &self, m : PodSpec ) -> f64 {
        let &count = self.task_count.get(&m).unwrap_or(&0);
        count as f64 / self.num_tasks as f64
    }

    fn add_to_backlog(&mut self, m : PodSpec ) {
        self.backlog.push_back(m);
    }
}

impl std::fmt::Display for Workload {

    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Workload ({})", self.name)?;

        writeln!(f, "Backlog -- {} tasks", self.backlog.len())?;
        self.backlog.iter().try_for_each( |m| {
            writeln!(f, "{}", m)
        })?;

        let mut task_count: Vec<(&PodSpecKey, &PodSpecValue)> = self.task_count.iter().collect();
        task_count.sort_by_key(|(m, count) | {**count});


        writeln!(f, "Task Counts -- {} total, {} unique", self.num_tasks, task_count.len())?;
        task_count.iter().rev().try_for_each(|(m, count)| {
            writeln!(f, "\t{: <4.2} -->\t{}",  **count, m)
        } )?;

        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use std::fs::File;
    use rstest::{fixture, rstest};
    use rstest_reuse::{self, *};
    use super::*;

    #[fixture]
    #[once]
    fn prefix() -> &'static str {
        "clusterdata/pod_data/"
    }

    #[template]
    #[rstest]
    #[case("default.csv")]
    #[case("gpuspec33.csv")]
    #[case("multigpu50.csv")]
    fn test_workload(#[case] file_name: &str) {}

    #[apply(test_workload)]
    fn test_create(#[case] file_name: &str, prefix: &str) {

        let file_path = prefix.to_owned() + file_name;
        let file = File::open(&file_path)
            .expect( format!("{} file not found", file_path ).as_str() );

        let workload = Workload::new(file_path, file);

        println!("{}", workload);
    }

    #[apply(test_workload)]
    fn test_task_life_cycle(#[case] file_name: &str, prefix: &str) {
        let file_path = prefix.to_owned() + file_name;
        let file = File::open(&file_path)
            .expect( format!("{} file not found", file_path ).as_str() );

        let mut workload = Workload::new(file_path, file);

        // Simulate failing to schedule 3 tasks, then fetching them again from the backlog

        let (a, b, c) =
            (workload.next_task(),
            workload.next_task(),
            workload.next_task());

        workload.add_to_backlog(a.clone());
        workload.add_to_backlog(b.clone());
        workload.add_to_backlog(c.clone());

        assert_eq!(workload.next_task(), a);
        assert_eq!(workload.next_task(), b);
        assert_eq!(workload.next_task(), c);
    }
}