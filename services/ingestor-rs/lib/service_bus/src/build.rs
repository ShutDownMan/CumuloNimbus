extern crate capnpc;

fn main() {
    let schema_names = ["persistor"];

    println!("copying all capnp schemas");
    for schema_name in schema_names.iter() {
        let success = std::fs::copy(
            format!("../../../service-bus/capnp/{}.capnp", schema_name),
            format!("./src/schemas/{}.capnp", schema_name),
        );
        if let Err(e) = success {
            panic!("error copying capnp schemas: {:?}", e);
        }
    }

    println!("building capnp schemas");
    for schema_name in schema_names.iter() {
        let success = capnpc::CompilerCommand::new()
            .file(format!("./src/schemas/{}.capnp", schema_name))
            .output_path("./")
            .default_parent_module(vec!["schemas".to_string()])
            .run();
        if let Err(e) = success {
            panic!("error building capnp schemas: {:?}", e);
        }
    }

    println!("removing capnp schemas");
    for schema_name in schema_names.iter() {
        let success = std::fs::remove_file(format!("./src/schemas/{}.capnp", schema_name));
        if let Err(e) = success {
            panic!("error removing capnp schemas: {:?}", e);
        }
    }
}
