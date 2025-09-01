
use std::collections::{HashMap, VecDeque};
use std::io::Read;
use rand::distr::Uniform;
use rand::prelude::*;
use std::cell::RefCell;
use std::rc::Rc;
use super::*;

type TaskCount = HashMap<PodSpecKey, usize>;

#[derive(Debug, Clone)]
#[public]
struct WorkloadStruct {

    name : String,

    rng: RefCell<ThreadRng>,

    drain_backlog: RefCell<bool>,
    backlog: RefCell<VecDeque<PodSpec>>,

    num_tasks: POD,
    tasks: Vec<PodSpec>,
    task_count: TaskCount,

    metrics: RefCell<TaskMetrics>
}
pub type Workload = Rc<WorkloadStruct>;


#[derive(Debug, Clone)]
#[derive(Default)]
#[public]
struct TaskMetrics {
    tasks_arrived: u32,
    tasks_scheduled: u32,
    tasks_delayed: u32,

    total_cpu: CPU,
    total_mem: MEM,
    total_gpu: GPU,
}

impl WorkloadStruct {
    pub fn new( name: String, pod_csv : impl Read )  -> Self {

        let drain_backlog = RefCell::new(false);
        let backlog = RefCell::new(VecDeque::new());
        let mut task_count = HashMap::new();
        let mut tasks = Vec::new();

        process_csv(pod_csv, |i, mut record: PodSpecStruct | {

            // Pre-process PodSpec so that gpu_milli can be used directly,
            // Assuming GPUs are always allocated as one fraction or an integer number.
            if record.num_gpu != 1 {
                record.gpu_milli = record.num_gpu as GPU * GPU_MILLI;
            }

            let task = {
                let mut record = record.clone();
                record.id = i;
                Rc::new(record)
            };

            tasks.push(task);
            *task_count.entry(record).or_insert(0) += 1;

            Ok(())

        }).expect("Failed to process Workload CSV");

        let num_tasks = tasks.len();
        let rng = RefCell::new(rand::rng());
        let metrics = RefCell::new(TaskMetrics::default());

        Self {
            name,
            rng,
            drain_backlog, backlog,
            num_tasks, tasks, task_count,
            metrics
        }
    }

    pub fn next_task(&self) -> PodSpec {
        if let Some(m) = self.pop_backlog() {
            return m;
        }

        // Select random task
        self.metrics.borrow_mut().tasks_arrived += 1;

        let task = self.tasks.choose(&mut self.rng.borrow_mut()).unwrap();
        task.to_owned()
    }

    pub fn task_count( &self, task : &PodSpec ) -> usize {
        let &count = self.task_count.get(task).unwrap_or(&0);
        count
    }

    pub fn push_backlog(&self, task: PodSpec ) {
        self.drain_backlog(false);
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
    pub fn is_drain( &self ) -> bool { *self.drain_backlog.borrow() }

    pub fn backlog_size(&self) -> usize {
        self.backlog.borrow().len()
    }

    pub fn deploy(&self) {
        // Reset backlog queue for draining again
        self.drain_backlog(true);

        // Report and reset metrics
        println!("{}", self.metrics.borrow());
        println!("Tasks in backlog: {}", self.backlog_size());
        *self.metrics.borrow_mut() = TaskMetrics::default();
    }

    pub fn update_metrics(&self, task: PodSpec, scheduled: bool ) {
        let mut metrics = self.metrics.borrow_mut();

        if !scheduled {
            metrics.tasks_delayed += 1;

        } else {
            metrics.tasks_scheduled +=1;

            metrics.total_cpu += task.cpu_milli;
            metrics.total_mem += task.memory_mib;
            metrics.total_gpu += task.gpu_milli;
        }


    }
}

impl std::fmt::Display for WorkloadStruct {

    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f);
        writeln!(f, "Workload ({})", self.name)?;


        writeln!(f, "Backlog -- {} tasks", self.backlog_size())?;
        self.backlog.borrow().iter().try_for_each( |task| {
            writeln!(f, "\t{}", task)
        })?;


        let mut task_count: Vec<(&PodSpecKey, &usize)> = self.task_count.iter().collect();
        task_count.sort_by_key(|(m, count) | {**count});


        writeln!(f, "Task Counts -- {} total, {} unique", self.num_tasks, task_count.len())?;
        task_count.iter().rev().take(10).try_for_each(|(task, count)| {
            writeln!(f, "({})\t{}", count, task)
        } )?;

        writeln!(f, ". . .");

        Ok(())
    }
}

impl std::fmt::Display for TaskMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Task Metrics:");

        writeln!(f, "Tasks arrived: {} = {}(scheduled) + {}(delayed)",
                 self.tasks_arrived, self.tasks_scheduled, self.tasks_delayed )?;

        write!(f, "Total resources consumed: {: >4.1} cpu\t{: >4.1} GiB\t{: >4.1} GPU",
               self.total_cpu as f64 / CPU_MILLI as f64,
               self.total_mem as f64 / MEM_MIB as f64,
               self.total_gpu as f64 / GPU_MILLI as f64 )

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

        let workload = WorkloadStruct::new(file_path, file);

        println!("{}", workload);
    }

    #[apply(test_workload)]
    fn test_task_life_cycle(#[case] file_name: &str, prefix: &str) {
        let file_path = prefix.to_owned() + file_name;
        let file = File::open(&file_path)
            .expect( format!("{} file not found", file_path ).as_str() );

        let mut workload = WorkloadStruct::new(file_path, file);

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