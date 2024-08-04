#include <cstdint>
#include <string>
#include <vector>
#include <algorithm>
#include <stdexcept>

using namespace std;

extern "C" {
    int32_t _locf(const char* dataseries_id, const int64_t timestamp, int64_t time_window, void *out_value);
    // int32_t _lerp(const char* dataseries_id, const int64_t timestamp, void *out_value);
    char *_get_dataseries_id_by_alias(const char* id);
}

enum class InterpolationType {
    LINEAR,
    LOCF,
    NO_INTERPOLATION
};

enum class TimeUnit {
    NANOSECOND,
    MILLISECOND,
    SECOND,
    MINUTE,
    HOUR,
    DAY,
    WEEK,
    MONTH,
    YEAR
};

class NumericDataSeries {
public:
    string id;
    string alias;
    string description;
    string data_type;
    vector<int64_t> timestamps;
    vector<double> values;

    InterpolationType interpolation_type;
    int64_t time_window;

    NumericDataSeries() : interpolation_type(InterpolationType::NO_INTERPOLATION) {}

    #pragma region Mirroring

    vector<int64_t> mirror_timestamps() {
        vector<int64_t> mirrorred_timestamps;

        for (int64_t timestamp : this->timestamps) {
            mirrorred_timestamps.push_back(timestamp);
        }

        return mirrorred_timestamps;
    }

    NumericDataSeries *mirroring(vector<string> dataseries_ids) {
        NumericDataSeries *result = new NumericDataSeries();

        // merge timestamps
        result->timestamps.insert(result->timestamps.end(), this->timestamps.begin(), this->timestamps.end());

        for (string dataseries_id : dataseries_ids) {
            NumericDataSeries *dataseries = new NumericDataSeries();
            dataseries->id = dataseries_id;

            vector<int64_t> mirrorred_timestamps = dataseries->mirror_timestamps();
            result->timestamps.insert(result->timestamps.end(), mirrorred_timestamps.begin(), mirrorred_timestamps.end());
        }

        // sort timestamps
        sort(result->timestamps.begin(), result->timestamps.end());

        // merge values
        for (int64_t timestamp : result->timestamps) {
            double value = this->get_value(timestamp);
            result->values.push_back(value);
        }

        return result;
    }

    #pragma endregion

    #pragma region Time Window

    NumericDataSeries *within(int64_t time_window, TimeUnit time_unit) {
        this->time_window = time_window;

        switch (time_unit) {
            case TimeUnit::NANOSECOND:
                this->time_window *= 1;
                break;
            case TimeUnit::MILLISECOND:
                this->time_window *= 1000;
                break;
            case TimeUnit::SECOND:
                this->time_window *= 1000000;
                break;
            case TimeUnit::MINUTE:
                this->time_window *= 60000000;
                break;
            case TimeUnit::HOUR:
                this->time_window *= 3600000000;
                break;
            case TimeUnit::DAY:
                this->time_window *= 86400000000;
                break;
            case TimeUnit::WEEK:
                this->time_window *= 604800000000;
                break;
            case TimeUnit::MONTH:
                this->time_window *= 2628000000000;
                break;
            case TimeUnit::YEAR:
                this->time_window *= 31540000000000;
                break;
        }

        return this;
    }

    #pragma endregion

    #pragma region Interpolation

    double get_value(int64_t timestamp) const {
        double value;
        switch (this->interpolation_type) {
            // case InterpolationType::LINEAR:
            //     // interpolate value using linear interpolation
            //     bool status = _lerp(this->id.c_str(), timestamp, time_window, &value);

            //     if (!status) {
            //         // raise error
            //         throw runtime_error("Failed to interpolate value using linear interpolation");
            //     }
            //     break;
            case InterpolationType::LOCF:
                // interpolate value using locf
                bool status = _locf(this->id.c_str(), timestamp, time_window, &value);

                if (!status) {
                    // raise error
                    throw runtime_error("Failed to interpolate value using locf");
                }
                break;
            case InterpolationType::NO_INTERPOLATION:
                // return the value at the timestamp
                break;
        }

        return value;
    }

    NumericDataSeries *locf() {
        // set dataseries state to locf
        this->interpolation_type = InterpolationType::LOCF;

        return this;
    }

    NumericDataSeries *lerp() {
        // set dataseries state to lerp
        this->interpolation_type = InterpolationType::LINEAR;

        return this;
    }

    #pragma endregion

    #pragma region Operator Overloading

    NumericDataSeries *add(NumericDataSeries *dataseries) {
        NumericDataSeries *result = new NumericDataSeries();

        // merge timestamps
        result->timestamps.insert(result->timestamps.end(), this->timestamps.begin(), this->timestamps.end());
        result->timestamps.insert(result->timestamps.end(), dataseries->timestamps.begin(), dataseries->timestamps.end());

        // sort timestamps
        sort(result->timestamps.begin(), result->timestamps.end());

        // merge values
        for (int64_t timestamp : result->timestamps) {
            double value = this->get_value(timestamp) + dataseries->get_value(timestamp);
            result->values.push_back(value);
        }

        return result;
    }

    #pragma endregion

    #pragma region DataSeries info fetch

    static NumericDataSeries *fetch_dataseries_by_alias(string alias) {
        NumericDataSeries *dataseries = new NumericDataSeries();

        dataseries->alias = alias;

        char *id = _get_dataseries_id_by_alias(dataseries->alias.c_str());
        if (id != nullptr) {
            dataseries->id = id;
        }

        return dataseries;
    }

    #pragma endregion
};

/*
    function main(dataseries_a, dataseries_b):
        A = locf dataseries_a within 1 hour;
        B = locf dataseries_b within 1 hour;

        return A + B mirroring (dataseries_a, dataseries_b);

*/
NumericDataSeries *compute(NumericDataSeries *dataseries_a, NumericDataSeries *dataseries_b) {
    NumericDataSeries *A = dataseries_a->locf()->within(1, TimeUnit::HOUR);
    NumericDataSeries *B = dataseries_b->locf()->within(1, TimeUnit::HOUR);

    return (A->add(B))->mirroring({A->id, B->id});
}

int main() {
    NumericDataSeries *dataseries_a = NumericDataSeries::fetch_dataseries_by_alias("dataseries_a");
    NumericDataSeries *dataseries_b = NumericDataSeries::fetch_dataseries_by_alias("dataseries_b");

    NumericDataSeries *result = compute(dataseries_a, dataseries_b);


    return 0;
}
