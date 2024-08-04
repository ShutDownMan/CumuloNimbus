#include <stdint.h>

extern int64_t _get_dataseries_id_by_alias(const char* id);
extern int64_t _copy_dataseries(int64_t dataseries_id);
extern int64_t _set_interpolation_strategy(int64_t dataseries_id, int64_t interpolation_strategy);
extern int64_t _set_time_window(int64_t dataseries_id, int64_t time_window);
extern int64_t _add(int64_t dataseries_a_id, int64_t dataseries_b_id);
extern int64_t _mirroring(int64_t dataseries_id, int64_t *reference_dataseries_ids, int32_t reference_dataseries_ids_count);
extern void _set_result_dataseries(int64_t dataseries_id);

int64_t compute(int64_t dataseries_a_id, int64_t dataseries_b_id) {
    int64_t A = _copy_dataseries(dataseries_a_id);
    A = _set_interpolation_strategy(A, 1);
    A = _set_time_window(A, 1 * 60 * 60 * 1000 * 1000000);

    int64_t B = _copy_dataseries(dataseries_b_id);
    B = _set_interpolation_strategy(B, 1);
    B = _set_time_window(B, 1 * 60 * 60 * 1000 * 1000000);

    int64_t result = _add(A, B);
    int64_t mirror_dataseries_ids[] = {dataseries_a_id, dataseries_b_id};
    result = _mirroring(result, mirror_dataseries_ids, 2);

    return result;
}

int main() {
    int64_t dataseries_a_id = _get_dataseries_id_by_alias("dataseries_a");
    if (dataseries_a_id < 0) {
        return -1;
    }
    int64_t dataseries_b_id = _get_dataseries_id_by_alias("dataseries_b");
    if (dataseries_b_id < 0) {
        return -1;
    }

    int64_t result_dataseries_id = compute(dataseries_a_id, dataseries_b_id);

    _set_result_dataseries(result_dataseries_id);

    return 0;
}
