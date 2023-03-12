use std::collections::HashMap;

use clap::Parser;
use fs2::FileExt;
use run_script::*;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = Some("/tmp/sector-merger.lock"))]
    lock_file: std::path::PathBuf,

    #[arg(long)]
    really_do_it: bool,

    /// The root path to merge the files
    path: std::path::PathBuf,
}

fn main() {
    let (tx, rx) = std::sync::mpsc::channel();
    ctrlc::set_handler(move || tx.send(()).unwrap()).unwrap();

    let args = Args::parse();

    let file = std::fs::File::create(args.lock_file).unwrap();
    file.try_lock_exclusive().unwrap();

    let mut options = ScriptOptions::new();
    options.working_directory = Some("/tmp".parse().unwrap());

    let path = args.path;
    let (mut res, _) = run_script_or_exit!(format!("find {}", path.to_str().unwrap()));
    // let mut res = std::fs::read_to_string("./aaa.txt").unwrap();

    res = res.replace("\r\n", "\n");

    let mut sealeds = HashMap::new();
    let mut caches = HashMap::new();

    let dirs: Vec<&str> = res.split("\n").collect();
    for dir in dirs {
        let spans: Vec<&str> = dir.split("/").collect();
        if spans.len() < 3 {
            continue;
        }
        {
            let tmp: Vec<&str> = spans[spans.len() - 1].split("-").collect();
            if tmp.len() != 3 || tmp[0] != "s" || tmp[2].parse::<u64>().is_err() {
                continue;
            }
            if !tmp[1].starts_with("t0") || tmp[1].replace("t0", "").parse::<u64>().is_err() {
                continue;
            }
        };

        let sector_id = spans[spans.len() - 1];
        let tp = spans[spans.len() - 2];
        if tp == "sealed" {
            sealeds.insert(sector_id, dir);
        } else if tp == "cache" {
            if !caches.contains_key(&sector_id) {
                caches.insert(sector_id, vec![dir]);
            } else {
                caches.get_mut(&sector_id).unwrap().push(dir);
            }
        }
    }

    let mut scripts = vec![];

    for (k, cache_files) in caches {
        let Some(sealed_file) = sealeds.get(k) else {
            continue;
        };
        let dst = sealed_file.replace("sealed", "cache");
        if dst.contains(" ") {
            continue;
        }

        for cache_file in cache_files {
            if cache_file == dst {
                continue;
            }
            if cache_file.contains(" ") {
                continue;
            }
            scripts.push(format!(
                "mkdir -p {}; mv {}/* {}; rmdir {};",
                dst, cache_file, dst, cache_file
            ));
        }
    }

    println!("total task: {}", scripts.len());

    for script in scripts {
        match rx.try_recv() {
            Ok(_) => break,
            Err(_) => {}
        }

        if args.really_do_it {
            run_script!(script).unwrap();
        } else {
            println!("todo: \"{}\"", script);
        }
    }

    println!("finished")
}
