#![allow(unused)]

use std::collections::{HashMap, VecDeque};
use std::io::Read;
use rand::distr::Uniform;
use rand::prelude::*;


use super::*;


type PodSpecKey = PodSpec;
type PodSpecValue = usize;


pub struct Workload {

    name : String,

    drain_backlog: bool,
    backlog: VecDeque<POD>,

    num_tasks: POD,
    tasks: Vec<PodSpec>,
    task_count: HashMap<PodSpecKey, PodSpecValue>,

    rng: ThreadRng,
    uniform: Uniform<POD>,
}

impl Workload {
    pub fn new( name: String, pod_csv : impl Read )  -> Self {

        let drain_backlog = false;
        let backlog = VecDeque::new();
        let mut task_count = HashMap::new();
        let mut tasks = Vec::new();

        process_csv(pod_csv, |_, record: PodSpec| {

            tasks.push(record.clone());
            *task_count.entry(record).or_insert(0) += 1;

            Ok(())

        }).expect("Failed to process Workload CSV");

        let num_tasks = tasks.len();
        let rng = rand::rng();
        let uniform = Uniform::new(0, num_tasks).unwrap();


        Self {  name,
                rng, uniform,
                drain_backlog, backlog,
                num_tasks, task_count, tasks
        }
    }

    // MUTABLE

    pub fn next_task(&mut self) -> POD {
        if self.drain_backlog {
            if let Some(m) = self.backlog.pop_front() {
                return m;
            }

            // Completely emptied,
            self.lock_backlog()
        }

        // Select random index
        self.uniform.sample(&mut self.rng)
    }

    pub fn add_to_backlog(&mut self, m : POD ) {
        if self.backlog.is_empty() { self.lock_backlog() }

        self.backlog.push_back(m);
    }

    pub fn lock_backlog(&mut self) { self.drain_backlog = false; }

    pub fn unlock_backlog(&mut self) { self.drain_backlog = true; }

    // IMMUTABLE

    pub fn task(&self, m: POD ) -> &PodSpec {
        &self.tasks[m]
    }

    pub fn task_count( &self, task : &PodSpec ) -> usize {
        let &count = self.task_count.get(task).unwrap_or(&0);
        count
    }

    pub fn tasks_size(&self) -> usize {
        self.num_tasks
    }

    pub fn backlog_size(&self) -> usize {
        self.backlog.len()
    }
}

impl std::fmt::Display for Workload {

    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f);
        writeln!(f, "Workload ({})", self.name)?;


        writeln!(f, "Backlog -- {} tasks", self.backlog.len())?;
        self.backlog.iter().try_for_each( |&m| {
            writeln!(f, "[{}]\t{}", m, self.tasks[m])
        })?;


        let mut task_count: Vec<(&PodSpecKey, &PodSpecValue)> = self.task_count.iter().collect();
        task_count.sort_by_key(|(m, count) | {**count});


        writeln!(f, "Task Counts -- {} total, {} unique", self.num_tasks, task_count.len())?;
        task_count.iter().rev().take(10).try_for_each(|(task, count)| {
            writeln!(f, "({})\t{}", count, task)
        } )?;

        writeln!(f, ". . .");

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

        workload.add_to_backlog(a);
        workload.add_to_backlog(b);
        workload.add_to_backlog(c);

        assert_eq!(workload.next_task(), a);
        assert_eq!(workload.next_task(), b);
        assert_eq!(workload.next_task(), c);

        println!("{}", workload.task(a));
        println!("{}", workload.task(b));
        println!("{}", workload.task(c));
    }
}