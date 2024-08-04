import { DataSeries, get_dataseries, mirror_timestamps } from "./bridge.js";

export function compute() {
    let dataseries_a = get_dataseries("dataseries_a");
    let dataseries_b = get_dataseries("dataseries_b");

    let result = create_dataseries();

    let mirrored_timestamps = mirror_timestamps([dataseries_a, dataseries_b]);

    for (let i = 0; i < mirrored_timestamps.length; i++) {
        let current_timestamp = mirrored_timestamps[i];
        let a = dataseries_a.locf(current_timestamp);
        let b = dataseries_b.locf(current_timestamp);

        result.add(current_timestamp, a + b);
    }

    return result;
}
