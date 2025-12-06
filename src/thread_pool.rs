//! A module for thread pool.
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex, mpsc},
    thread::{self},
};

use crate::error::Result;

/// A trait for thread pools.
///
/// This trait defines the interface for thread pools.
///
pub trait ThreadPool: Sized {
    /// Create a new thread pool.
    fn new(threads: u32) -> Result<Self>;
    /// Spawn a new job on the thread pool.
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}

/// A job is a function that can be executed by a thread.
pub type Job = Box<dyn FnOnce() + Send + 'static>;

/// A message is a message that can be sent to a worker.
pub enum Message {
    /// A new job to be executed.
    NewJob(Job),
    /// A terminate message to tell the worker to terminate.
    Terminate,
}

/// A naive thread pool.
pub struct NaiveThreadPool {
    workers: Vec<Worker>,
    sender: mpsc::Sender<Message>,
}

impl ThreadPool for NaiveThreadPool {
    /// Create a new naive thread pool.
    fn new(threads: u32) -> Result<Self> {
        let (sender, receiver) = mpsc::channel();
        let mut workers = Vec::new();
        let receiver = Arc::new(Mutex::new(receiver));
        for id in 0..threads {
            workers.push(Worker::new(id, receiver.clone()));
        }
        Ok(Self { workers, sender })
    }
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(job);
        self.sender.send(Message::NewJob(job)).unwrap();
    }
}

impl Drop for NaiveThreadPool {
    fn drop(&mut self) {
        for _ in &self.workers {
            self.sender.send(Message::Terminate).unwrap();
        }

        for worker in &mut self.workers {
            if let Some(thread) = worker.thread.take() {
                if let Err(e) = thread.join() {
                    eprintln!("Worker {} join failed: {:?}", worker.id, e);
                }
            }
        }
    }
}

/// A worker is a thread that can execute jobs.
pub struct Worker {
    id: u32,
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    /// new and run a worker thread.
    fn new(id: u32, receiver: Arc<Mutex<mpsc::Receiver<Message>>>) -> Self {
        let thread = thread::spawn(move || {
            loop {
                let msg = {
                    let receiver = receiver.lock().unwrap();
                    receiver.recv()
                };
                match msg {
                    Ok(Message::NewJob(job)) => {
                        let result = catch_unwind(AssertUnwindSafe(job));
                        if let Err(e) = result {
                            eprintln!("Worker {} job execution panicked: {:?}", id, e);
                        }
                    }
                    Ok(Message::Terminate) | Err(_) => break,
                }
            }
        });
        Self {
            id,
            thread: Some(thread),
        }
    }
}

pub type SharedQueueThreadPool = NaiveThreadPool;
