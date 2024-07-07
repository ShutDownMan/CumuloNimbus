use std::path;

extern crate capnpc;

fn main() {
    println!("list all capnp schemas in folder");
    let paths = std::fs::read_dir("../intercom/capnp").unwrap();
    let paths = paths.filter_map(|entry| {
        // check if the entry is a file and has .capnp extension
        entry.ok().and_then(|e|
            e.path()
            // To string
            .to_str().map(|s| s.to_string())
            // Check if it ends with .capnp
            .and_then(|n| if n.ends_with(".capnp") { Some(n) } else { None }))
    }).collect::<Vec<String>>();

    print!("found schemas: ");
    for path in paths.clone() {
        println!("{:?}, ", path);
    }

    println!("compiling capnp schemas");
    for path in paths.clone() {
        let success = capnpc::CompilerCommand::new()
            .file(path)
            // .output_path("./src/schemas")
            .default_parent_module(vec!["schemas".to_string()])
            .run();
        if let Err(e) = success {
            panic!("error building capnp schemas: {:?}", e);
        }
    }

    println!("moving generated files to parent folder");
    let paths = std::fs::read_dir("../intercom/capnp").unwrap();
    let paths = paths.filter_map(|entry| {
        entry.ok().and_then(|e|
            e.path()
            .to_str().map(|s| s.to_string())
            .and_then(|n| if n.ends_with(".rs") { Some(n) } else { None }))
    }).collect::<Vec<String>>();
    for path in paths.clone() {
        let file_name = path::Path::new(&path).file_name().unwrap().to_str().unwrap();
        let new_path = format!("./src/schemas/{}", file_name);
        println!("moving {:?} to {:?}... ", path, new_path);
        std::fs::rename(path, new_path).unwrap();
    }

    println!("done");
}
