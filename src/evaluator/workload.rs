#![allow(unused)]

use std::collections::{HashMap, VecDeque};
use std::io::Read;
use rand::distr::Uniform;
use rand::prelude::*;
use std::cell::RefCell;
use std::rc::Rc;
use super::*;


type PodSpecKey = PodSpecStruct;
type PodSpecValue = usize;


pub struct Workload {

    name : String,

    rng: RefCell<ThreadRng>,

    drain_backlog: RefCell<bool>,
    backlog: RefCell<VecDeque<PodSpec>>,

    num_tasks: POD,
    tasks: Vec<PodSpec>,
    task_count: HashMap<PodSpecKey, PodSpecValue>,
}

impl Workload {
    pub fn new( name: String, pod_csv : impl Read )  -> Self {

        let drain_backlog = RefCell::new(false);
        let backlog = RefCell::new(VecDeque::new());
        let mut task_count = HashMap::new();
        let mut tasks = Vec::new();

        process_csv(pod_csv, |_, record: PodSpecStruct | {

            tasks.push(Rc::new(record.clone()));
            *task_count.entry(record).or_insert(0) += 1;

            Ok(())

        }).expect("Failed to process Workload CSV");

        let num_tasks = tasks.len();
        let rng = RefCell::new(rand::rng());
        let uniform = Uniform::new(0, num_tasks).unwrap();


        Self {  name,
                rng,
                drain_backlog, backlog,
                num_tasks, task_count, tasks
        }
    }

    pub fn next_task(&self) -> PodSpec {
        if let Some(m) = self.pop_backlog() {
            return m;
        }

        // Select random task
        let task = self.tasks.choose(&mut self.rng.borrow_mut()).unwrap();
        task.to_owned()
    }

    pub fn push_backlog(&self, task: PodSpec ) {
        if self.backlog.borrow().is_empty() { self.drain_backlog(false) }

        self.backlog.borrow_mut().push_back(task);
    }

    pub fn pop_backlog(&self) -> Option<PodSpec>{
        if *self.drain_backlog.borrow() == false { return  None}

        let task_opt = self.backlog.borrow_mut().pop_front();
        if task_opt.is_none() {
            // Completely emptied,
            self.drain_backlog(false)
        }

        task_opt
    }

    pub fn drain_backlog(&self, drain : bool ) {
        *self.drain_backlog.borrow_mut() = drain;
    }

    pub fn task_count( &self, task : &PodSpec ) -> usize {
        let &count = self.task_count.get(task).unwrap_or(&0);
        count
    }

    pub fn tasks_size(&self) -> usize {
        self.num_tasks
    }

    pub fn backlog_size(&self) -> usize {
        self.backlog.borrow().len()
    }
}

impl std::fmt::Display for Workload {

    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f);
        writeln!(f, "Workload ({})", self.name)?;


        writeln!(f, "Backlog -- {} tasks", self.backlog_size())?;
        self.backlog.borrow().iter().try_for_each( |task| {
            writeln!(f, "\t{}", task)
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

        // Fetch 3 tasks
        let (a, b, c) =
            (workload.next_task(),
            workload.next_task(),
            workload.next_task());

        println!("{}", a);
        println!("{}", b);
        println!("{}", c);

        // Oops, we failed to schedule them this round
        workload.push_backlog(a.clone());
        workload.push_backlog(b.clone());
        workload.push_backlog(c.clone());

        // Begin next round... Workload should serve these three first.
        workload.drain_backlog(true);

        assert_eq!(workload.next_task(), a);
        assert_eq!(workload.next_task(), b);
        assert_eq!(workload.next_task(), c);
    }
}