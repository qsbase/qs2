#include "benchmark_data.h"
#include "qdata.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <map>
#include <numeric>
#include <optional>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace {

using clock_type = std::chrono::steady_clock;
namespace fs = std::filesystem;

constexpr int benchmark_thread_grid[] = {1, 2, 3, 4, 8};
constexpr int benchmark_compress_grid[] = {3, 9};
constexpr std::size_t default_benchmark_rows = 10000000;
constexpr int default_max_reps = 5;

struct benchmark_case {
    int compress_level;
    int nthreads;
};

enum class benchmark_measure {
    size_bytes,
    write,
    io_read
};

struct benchmark_sample {
    benchmark_case config;
    int rep;
};

struct stats {
    double mean_seconds;
    double median_seconds;
    double min_seconds;
    double max_seconds;
    double stdev_seconds;
};

struct result_row {
    benchmark_measure measure;
    int compress_level;
    int nthreads;
    int reps;
    stats timing;
};

struct result_key {
    benchmark_measure measure;
    int compress_level;
    int nthreads;

    bool operator<(const result_key& other) const {
        if(nthreads != other.nthreads) {
            return nthreads < other.nthreads;
        }
        if(compress_level != other.compress_level) {
            return compress_level < other.compress_level;
        }
        return static_cast<int>(measure) < static_cast<int>(other.measure);
    }
};

struct temp_root_guard {
    fs::path path;

    ~temp_root_guard() {
        if(path.empty()) {
            return;
        }
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

[[noreturn]] void fail(const std::string& message) {
    throw std::runtime_error(message);
}

bool objects_equal(const qdata::object& lhs, const qdata::object& rhs);

bool attrs_equal(const std::vector<qdata::box<qdata::named_object>>& lhs,
                 const std::vector<qdata::box<qdata::named_object>>& rhs) {
    if(lhs.size() != rhs.size()) {
        return false;
    }
    for(std::size_t i = 0; i < lhs.size(); ++i) {
        if(lhs[i]->name != rhs[i]->name) {
            return false;
        }
        if(!objects_equal(lhs[i]->data, rhs[i]->data)) {
            return false;
        }
    }
    return true;
}

template <class T>
bool leaf_equal(const T& lhs, const T& rhs) {
    return lhs.values == rhs.values && attrs_equal(lhs.attrs, rhs.attrs);
}

bool leaf_equal(const qdata::string_vector& lhs, const qdata::string_vector& rhs) {
    if(lhs.size() != rhs.size()) {
        return false;
    }
    if(!attrs_equal(lhs.attrs, rhs.attrs)) {
        return false;
    }
    for(std::size_t i = 0; i < lhs.size(); ++i) {
        if(lhs.is_na(i) != rhs.is_na(i)) {
            return false;
        }
        if(!lhs.is_na(i) && lhs[i].view() != rhs[i].view()) {
            return false;
        }
    }
    return true;
}

bool list_equal(const qdata::list_vector& lhs, const qdata::list_vector& rhs) {
    if(lhs.values.size() != rhs.values.size()) {
        return false;
    }
    if(!attrs_equal(lhs.attrs, rhs.attrs)) {
        return false;
    }
    for(std::size_t i = 0; i < lhs.values.size(); ++i) {
        if(!objects_equal(*lhs.values[i], *rhs.values[i])) {
            return false;
        }
    }
    return true;
}

bool objects_equal(const qdata::object& lhs, const qdata::object& rhs) {
    if(lhs.data.index() != rhs.data.index()) {
        return false;
    }

    return std::visit(
        [&](const auto& left_value) -> bool {
            using value_type = std::decay_t<decltype(left_value)>;
            const auto& right_value = std::get<value_type>(rhs.data);
            if constexpr (std::is_same_v<value_type, qdata::nil_value>) {
                return true;
            } else if constexpr (std::is_same_v<value_type, qdata::list_vector>) {
                return list_equal(left_value, right_value);
            } else {
                return leaf_equal(left_value, right_value);
            }
        },
        lhs.data
    );
}

std::size_t parse_rows_arg(const char* arg) {
    try {
        std::size_t parsed_chars = 0;
        const auto value = static_cast<std::size_t>(std::stoull(arg, &parsed_chars));
        if(parsed_chars != std::string_view(arg).size()) {
            fail(std::string("invalid rows argument: ") + arg + " (expected a base-10 integer)");
        }
        if(value == 0) {
            fail("rows must be greater than zero");
        }
        return value;
    } catch(const std::exception&) {
        fail(std::string("invalid rows argument: ") + arg);
    }
}

int parse_reps_arg(const char* arg) {
    try {
        std::size_t parsed_chars = 0;
        const int value = std::stoi(arg, &parsed_chars);
        if(parsed_chars != std::string_view(arg).size()) {
            fail(std::string("invalid reps argument: ") + arg + " (expected a base-10 integer)");
        }
        if(value <= 0) {
            fail("reps must be greater than zero");
        }
        return value;
    } catch(const std::exception&) {
        fail(std::string("invalid reps argument: ") + arg);
    }
}

std::vector<benchmark_sample> shuffled_samples(const int reps) {
    std::vector<benchmark_sample> samples;
    samples.reserve(
        static_cast<std::size_t>(reps) *
        std::size(benchmark_compress_grid) *
        std::size(benchmark_thread_grid)
    );

    std::random_device rd;
    std::mt19937 rng(rd());
    for(int rep = 0; rep < reps; ++rep) {
        for(const int compress_level : benchmark_compress_grid) {
            for(const int nthreads : benchmark_thread_grid) {
                samples.push_back({{compress_level, nthreads}, rep});
            }
        }
    }

    std::shuffle(samples.begin(), samples.end(), rng);
    return samples;
}

stats compute_stats(std::vector<double> seconds) {
    if(seconds.empty()) {
        fail("cannot compute stats for empty timing vector");
    }

    std::sort(seconds.begin(), seconds.end());
    const double mean = std::accumulate(seconds.begin(), seconds.end(), 0.0) /
                        static_cast<double>(seconds.size());
    const double median =
        seconds.size() % 2 == 0
            ? 0.5 * (seconds[seconds.size() / 2 - 1] + seconds[seconds.size() / 2])
            : seconds[seconds.size() / 2];
    double sumsq = 0.0;
    for(const double value : seconds) {
        const double delta = value - mean;
        sumsq += delta * delta;
    }

    return {
        mean,
        median,
        seconds.front(),
        seconds.back(),
        std::sqrt(sumsq / static_cast<double>(seconds.size()))
    };
}

double elapsed_seconds(const clock_type::time_point start, const clock_type::time_point end) {
    return std::chrono::duration<double>(end - start).count();
}

double to_milliseconds(const double seconds) {
    return seconds * 1000.0;
}

double to_mebibytes(const double bytes) {
    return bytes / (1024.0 * 1024.0);
}

struct save_result {
    double elapsed_seconds;
    double file_size_bytes;
};

save_result benchmark_save_once(const qdata::object& dataset,
                                const fs::path& output_file,
                                const int compress_level,
                                const bool shuffle,
                                const int nthreads) {
    const auto start = clock_type::now();
    qdata::save(output_file.string(), dataset, compress_level, shuffle, nthreads);
    return {
        elapsed_seconds(start, clock_type::now()),
        static_cast<double>(fs::file_size(output_file))
    };
}

double benchmark_read_once(const qdata::object& dataset,
                           const fs::path& input_file,
                           const bool validate_checksum,
                           const int nthreads) {
    const auto start = clock_type::now();
    const auto roundtrip = qdata::read(input_file.string(), validate_checksum, nthreads);
    const auto elapsed = elapsed_seconds(start, clock_type::now());
    if(!objects_equal(roundtrip, dataset)) {
        fail("read benchmark roundtrip mismatch");
    }
    return elapsed;
}

void remove_if_exists(const fs::path& path) {
    std::error_code ec;
    fs::remove(path, ec);
    if(ec) {
        fail("failed to remove benchmark sample file: " + path.string() + ": " + ec.message());
    }
}

void print_results(const std::vector<result_row>& rows) {
    std::map<result_key, result_row> row_map;
    for(const auto& row : rows) {
        row_map.emplace(
            result_key{row.measure, row.compress_level, row.nthreads},
            row
        );
    }

    const auto median_for = [&](const benchmark_measure measure,
                                const int compress_level,
                                const int nthreads) -> std::optional<double> {
        const auto it = row_map.find(result_key{measure, compress_level, nthreads});
        if(it == row_map.end()) {
            return std::nullopt;
        }
        return it->second.timing.median_seconds;
    };

    const auto required_median_for =
        [&](const benchmark_measure measure, const int compress_level, const int nthreads) {
            const auto value = median_for(measure, compress_level, nthreads);
            if(!value.has_value()) {
                fail("missing aggregated benchmark row");
            }
            return *value;
        };

    const auto format_milliseconds_cell =
        [&](const std::optional<double> seconds) -> std::string {
            if(!seconds.has_value()) {
                return "NA";
            }
            std::ostringstream out;
            out << std::fixed << std::setprecision(3) << to_milliseconds(*seconds);
            return out.str();
        };

    std::cout << "\nBenchmark medians (times in ms, size in MiB)\n";
    std::cout << std::fixed << std::setprecision(3);
    for(const int nthreads : benchmark_thread_grid) {
        std::cout << "\nthreads = " << nthreads << "\n";
        std::cout << std::left
                  << std::setw(8) << "cl"
                  << std::setw(14) << "size_mib"
                  << std::setw(18) << "write"
                  << std::setw(18) << "io_read"
                  << "\n";
        std::cout << std::string(58, '-') << "\n";

        for(const int compress_level : benchmark_compress_grid) {
            std::cout << std::left
                      << std::setw(8) << compress_level
                      << std::setw(14) << to_mebibytes(required_median_for(benchmark_measure::size_bytes, compress_level, nthreads))
                      << std::setw(18) << format_milliseconds_cell(median_for(benchmark_measure::write, compress_level, nthreads))
                      << std::setw(18) << format_milliseconds_cell(median_for(benchmark_measure::io_read, compress_level, nthreads))
                      << "\n";
        }
    }
}

} // namespace

int main(int argc, char** argv) try {
    const std::size_t benchmark_rows = argc >= 2 ? parse_rows_arg(argv[1]) : default_benchmark_rows;
    const int max_reps = argc >= 3 ? parse_reps_arg(argv[2]) : default_max_reps;
    if(argc > 3) {
        fail("usage: qdata_benchmark [rows] [max_reps]");
    }

    const auto total_start = clock_type::now();
    const auto temp_root = fs::temp_directory_path() /
        ("qdata_benchmark_" +
         std::to_string(static_cast<long long>(
             std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::system_clock::now().time_since_epoch()
             ).count()
         )));

    std::error_code ec;
    fs::create_directories(temp_root, ec);
    if(ec) {
        throw std::runtime_error("failed to create benchmark temp directory: " + ec.message());
    }
    temp_root_guard cleanup_guard{temp_root};

    std::cout << "Building benchmark object...\n";
    qdata::object dataset = benchmark_data::build_dataframe_object(benchmark_rows);

    std::cout << "Rows: " << benchmark_rows
              << ", reps per configuration: " << max_reps
              << ", shuffle: true"
              << ", thread grid: {1,2,3,4,8}"
              << ", compress grid: {3,9}\n";

    std::map<result_key, std::vector<double>> timings_by_key;
    const auto samples = shuffled_samples(max_reps);

    std::cout << "\n";
    const auto total_samples = samples.size();
    const fs::path sample_file = temp_root / "benchmark_sample.qdata";
    for(std::size_t sample_index = 0; sample_index < total_samples; ++sample_index) {
        const auto& sample = samples[sample_index];
        const auto config = sample.config;
        remove_if_exists(sample_file);

        auto& write_timings = timings_by_key[result_key{benchmark_measure::write, config.compress_level, config.nthreads}];
        if(write_timings.empty()) {
            write_timings.reserve(static_cast<std::size_t>(max_reps));
        }
        const auto save = benchmark_save_once(dataset, sample_file, config.compress_level, true, config.nthreads);
        write_timings.push_back(save.elapsed_seconds);

        auto& size_timings = timings_by_key[result_key{benchmark_measure::size_bytes, config.compress_level, config.nthreads}];
        if(size_timings.empty()) {
            size_timings.reserve(static_cast<std::size_t>(max_reps));
        }
        size_timings.push_back(save.file_size_bytes);

        auto& read_timings = timings_by_key[result_key{benchmark_measure::io_read, config.compress_level, config.nthreads}];
        if(read_timings.empty()) {
            read_timings.reserve(static_cast<std::size_t>(max_reps));
        }
        read_timings.push_back(benchmark_read_once(dataset, sample_file, false, config.nthreads));

        remove_if_exists(sample_file);

        if(sample_index == 0 || (sample_index + 1) % 8U == 0 || (sample_index + 1) == total_samples) {
            std::cout << "Completed " << (sample_index + 1)
                      << "/" << total_samples
                      << " benchmark samples...\n";
        }
    }

    std::vector<result_row> rows;
    rows.reserve(timings_by_key.size());
    for(const auto& entry : timings_by_key) {
        const auto& key = entry.first;
        const auto& timings = entry.second;
        rows.push_back({
            key.measure,
            key.compress_level,
            key.nthreads,
            static_cast<int>(timings.size()),
            compute_stats(timings)
        });
    }

    print_results(rows);
    std::cout << "\nTotal benchmark time: "
              << std::fixed << std::setprecision(3)
              << to_milliseconds(elapsed_seconds(total_start, clock_type::now()))
              << " ms\n";
    return 0;
} catch(const std::exception& e) {
    std::cerr << "Benchmark failed: " << e.what() << "\n";
    return 1;
}
