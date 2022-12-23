use std::env;
use std::fs;
use std::io::BufReader;
use std::io::IoSlice;
use std::io::prelude::*;
use std::io;
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc;
use std::thread;
use std::time;

use chrono::Local;

use simple_signal;
use simple_signal::Signal;

const ADDRESS: &str = "127.0.0.1:7878";
const MIN_THREADS: usize = 4;

fn prog() -> String {
    env::current_exe().unwrap()
        .file_name().unwrap()
        .to_str().unwrap()
        .to_owned()
}

macro_rules! log {
    ($($arg:tt)*) => {
        io::stderr().write_vectored(&[
            IoSlice::new(format!("[{}] {}: ", Local::now().format("%Y-%m-%d %H:%M:%S"), prog()).as_bytes()),
            IoSlice::new(format!($($arg)*).as_bytes()),
            IoSlice::new(format!("\n").as_bytes()),
        ]).unwrap();
    };
}

fn handle_connection(mut stream: TcpStream) {
    let buf_reader = BufReader::new(&mut stream);
    let request_line = buf_reader.lines().next().unwrap().unwrap();

    let (status_line, filename) = match &request_line[..] {
        "GET / HTTP/1.1" => {
            ("HTTP/1.1 200 OK", "hello.html")
        },
        "GET /sleep HTTP/1.1" => {
            thread::sleep(time::Duration::from_secs(5));
            ("HTTP/1.1 200 OK", "hello.html")
        }
        _ => {
            ("HTTP/1.1 404 NOT FOUND", "404.html")
        },
    };

    let mut file = fs::File::open(filename).unwrap();
    let metadata = file.metadata().unwrap();
    let length = metadata.len();

    if let Err(e) = stream.write_fmt(format_args!("{status_line}\r\nContent-Length: {length}\r\n\r\n")) {
        if e.kind() != io::ErrorKind::BrokenPipe {
            log!("Unable to write answer: {e:?}");
        }
        return;
    }

    loop {
        let mut buffer = [0; 256];

        match file.read(&mut buffer[..]) {
            Ok(n) => {
                if n == 0 {
                    break;
                }
                if let Err(e) = stream.write_all(&buffer) {
                    if e.kind() != io::ErrorKind::BrokenPipe {
                        log!("Unable to send buffer: {e:?}");
                    }
                    break;
                }
            },
            Err(e) => {
                log!("Unable to read file: {filename}: {e:?}");
                break;
            },
        };
    }
}

enum WorkerJob {
    Stream(TcpStream),
    Quit,
}

type Job = Box<WorkerJob>;

struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Job>>>) -> Worker {
        let thread = thread::spawn(move || {
            loop {
                let job = receiver.lock().unwrap().recv();

                let stream = match job {
                    Ok(x) => x,
                    Err(err) => {
                        log!("Worker {id}: Error: {err:?}");
                        continue;
                    },
                };

                match *stream {
                    WorkerJob::Stream(s) => {
                        log!("Worker {id} got a job.");
                        handle_connection(s);
                    },
                    WorkerJob::Quit => {
                        log!("Worker {id} going to quit.");
                        break;
                    },
                }
            }
        });

        Worker {
            id: id,
            thread: Some(thread),
        }
    }

    fn stop_thread(&mut self) {
        self.thread.take().map(|jh| {
            jh.join().expect("Couldn't join thread on the main thread.");
            log!("Worker {} thread is completed.", self.id);
        });
    }
}

pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: mpsc::Sender<Job>,
}

impl ThreadPool {
    pub fn new(size: usize) -> ThreadPool {
        assert!(size > 0);

        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));
        let mut workers = Vec::with_capacity(size);

        for id in 0..size {
            workers.push(Worker::new(id, Arc::clone(&receiver)));
        }

        ThreadPool { workers, sender }
    }

    pub fn execute(&self, f: TcpStream)
    {
        let job = Box::new(WorkerJob::Stream(f));
        self.sender.send(job).unwrap();
    }

    pub fn join(&mut self) {
        for _i in 0..self.workers.len() {
            let job = Box::new(WorkerJob::Quit);
            self.sender.send(job).unwrap();
        }

        for i in 0..self.workers.len() {
            let worker = &mut self.workers[i];
            worker.stop_thread();
        }
    }
}

impl Drop for ThreadPool {
    fn drop (&mut self) {
        self.join();
    }
}

fn main() -> std::io::Result<()> {
    let pool = ThreadPool::new(MIN_THREADS);
    let listener = TcpListener::bind(ADDRESS).unwrap();

    loop {
        if listener.set_nonblocking(true).is_ok() {
            break;
        }
    }

    log!("Bind to address: {}", ADDRESS);

    let signals = [Signal::Hup, Signal::Int, Signal::Quit, Signal::Abrt, Signal::Term];
    let (signal_tx, signal_rx) = mpsc::channel();

    simple_signal::set_handler(&signals, move |signals| {
        signal_tx.send(signals.to_owned()).unwrap();
    });

    loop {
        let sig = signal_rx.try_recv();
        if ! sig.is_err() {
            log!("Got SIGNAL: {:?}", sig.unwrap());
            break;
        }

        match listener.accept() {
            Ok((stream, addr)) => {
                log!("new client: {addr:?}");
                pool.execute(stream);
            },
            Err(err) => {
                if err.kind() != io::ErrorKind::WouldBlock {
                    log!("couldn't get client: {err:?}");
                    break;
                }
            },
        };
        thread::sleep(time::Duration::from_millis(10));
    }

    log!("terminating...");
    drop(listener);
    drop(pool);

    Ok(())
}
