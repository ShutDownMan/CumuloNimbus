

// class wrapper for DataSeries
class DataSeries {
    constructor(id, name, description, data_type) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.data_type = data_type;
    }

    locf(timestamp) {
        _locf(this.id, timestamp);
    }
}

export function create_dataseries() {
    return DataSeries(
        "result",
        "Result",
        "Result of the computation",
        0
    );
}

export function get_dataseries(id) {
    return new DataSeries(
        id,
        _get_dataseries_name(id),
        _get_dataseries_description(id),
        _get_dataseries_data_type(id)
    );
}

export function mirror_timestamps(dataseries) {
    let timestamps = [];
    for (let i = 0; i < dataseries.length; i++) {
        let current_dataseries = dataseries[i];
        for (let j = 0; j < current_dataseries.length; j++) {
            let current_timestamp = current_dataseries[j].timestamp;
            if (!timestamps.includes(current_timestamp)) {
                timestamps.push(current_timestamp);
            }
        }
    }

    return timestamps.sort();
}