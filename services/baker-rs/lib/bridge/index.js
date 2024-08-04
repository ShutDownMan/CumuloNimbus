// import { greet } from './bridge.wasm';
bridge = fetch('bridge.wasm')
    .then(response => response.arrayBuffer())
    .then(bytes => WebAssembly.instantiate(bytes))
    .then(results => results.instance.exports)
;

bridge.then(bridge => bridge.greet('World'));

