#ifndef QDATA_BENCHMARK_DATA_H
#define QDATA_BENCHMARK_DATA_H

#include "qdata_format/core_types.h"

#include <array>
#include <cmath>
#include <cstdint>
#include <limits>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace benchmark_data {

constexpr std::string_view alnum_chars =
    "0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz";
constexpr double two_pi = 6.28318530717958647692;

inline qdata::box<qdata::named_object> make_named_attr(std::string name, qdata::object value) {
    qdata::named_object out;
    out.name = std::move(name);
    out.data = std::move(value);
    return qdata::box<qdata::named_object>(std::move(out));
}

inline qdata::object make_names_attr() {
    qdata::string_vector names{
        std::string("numeric_col"),
        std::string("integer_col"),
        std::string("string_col")
    };
    return qdata::object(std::move(names));
}

inline qdata::object make_row_names_attr(const std::size_t nrows) {
    if(nrows > static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())) {
        throw std::runtime_error("benchmark row count exceeds compact R row.names limit");
    }

    qdata::integer_vector row_names;
    row_names.values = {
        qdata::na_int32,
        -static_cast<std::int32_t>(nrows)
    };
    return qdata::object(std::move(row_names));
}

inline qdata::object make_class_attr() {
    qdata::string_vector klass{std::string("data.frame")};
    return qdata::object(std::move(klass));
}

inline std::string random_alnum_token(std::mt19937_64& rng, const int min_length, const int max_length) {
    std::uniform_int_distribution<int> length_dist(min_length, max_length);
    std::uniform_int_distribution<std::size_t> char_dist(0, alnum_chars.size() - 1);

    const auto length = static_cast<std::size_t>(length_dist(rng));
    std::string out(length, '\0');
    for(std::size_t i = 0; i < length; ++i) {
        out[i] = alnum_chars[char_dist(rng)];
    }
    return out;
}

inline double patterned_numeric_value(const std::size_t row_index,
                                      std::mt19937_64& rng,
                                      std::normal_distribution<double>& noise_dist) {
    const double t = static_cast<double>(row_index);
    const double trend = 25.0 + 0.00003 * t;
    const double slow_wave = 4.5 * std::sin(two_pi * t / 2048.0);
    const double fast_wave = 1.2 * std::sin(two_pi * t / 127.0);
    const double regime_step = 2.0 * static_cast<double>((row_index / 250000U) % 6U);
    return trend + regime_step + slow_wave + fast_wave + noise_dist(rng);
}

inline std::int32_t patterned_integer_value(const std::size_t row_index,
                                            std::mt19937_64& rng,
                                            std::normal_distribution<double>& noise_dist) {
    const double t = static_cast<double>(row_index);
    const double baseline = 18.0 + 1.5 * static_cast<double>((row_index / 50000U) % 5U);
    const double seasonal = 5.0 * (1.0 + std::sin(two_pi * t / 1440.0)) +
                            2.0 * std::cos(two_pi * t / 96.0);
    const auto value = static_cast<long long>(std::llround(baseline + seasonal + noise_dist(rng)));
    return static_cast<std::int32_t>(std::max<long long>(0, value));
}

inline std::string patterned_string_value(const std::size_t row_index, std::mt19937_64& rng) {
    static constexpr std::array<std::string_view, 8> regions = {
        "north", "south", "east", "west", "central", "coastal", "metro", "rural"
    };
    static constexpr std::array<std::string_view, 8> families = {
        "retail", "finance", "health", "media", "logistics", "travel", "energy", "public"
    };
    static constexpr std::array<std::string_view, 5> channels = {
        "web", "store", "partner", "field", "mobile"
    };
    static constexpr std::array<std::string_view, 4> tiers = {
        "standard", "plus", "pro", "enterprise"
    };

    std::uniform_int_distribution<int> region_jitter(0, 1);
    std::uniform_int_distribution<int> family_jitter(0, 2);
    std::uniform_int_distribution<int> channel_jitter(0, 1);
    std::uniform_int_distribution<int> tier_jitter(0, 1);
    std::uniform_int_distribution<int> bucket_jitter(0, 7);
    std::bernoulli_distribution add_note(0.12);

    const auto region = regions[(row_index / 250000U + static_cast<std::size_t>(region_jitter(rng))) % regions.size()];
    const auto family = families[(row_index / 16384U + static_cast<std::size_t>(family_jitter(rng))) % families.size()];
    const auto channel = channels[(row_index / 65536U + static_cast<std::size_t>(channel_jitter(rng))) % channels.size()];
    const auto tier = tiers[(row_index / 32768U + static_cast<std::size_t>(tier_jitter(rng))) % tiers.size()];
    const auto week_bucket = static_cast<int>((row_index / 2048U) % 52U);
    const auto batch_bucket = static_cast<int>(((row_index / 512U) + static_cast<std::size_t>(bucket_jitter(rng))) % 64U);

    std::string out;
    out.reserve(72);
    out += "region=";
    out += region;
    out += "|family=";
    out += family;
    out += "|channel=";
    out += channel;
    out += "|tier=";
    out += tier;
    out += "|wk=";
    out += std::to_string(week_bucket);
    out += "|batch=";
    out += std::to_string(batch_bucket);
    if(add_note(rng)) {
        out += "|note=";
        out += random_alnum_token(rng, 4, 10);
    }
    return out;
}

inline qdata::object build_dataframe_object(const std::size_t nrows) {
    std::random_device rd;
    std::mt19937_64 rng(rd());
    std::normal_distribution<double> numeric_noise(0.0, 0.8);
    std::normal_distribution<double> integer_noise(0.0, 1.75);

    qdata::real_vector numeric_col;
    numeric_col.values.resize(nrows);
    for(std::size_t i = 0; i < numeric_col.values.size(); ++i) {
        numeric_col.values[i] = patterned_numeric_value(i, rng, numeric_noise);
    }

    qdata::integer_vector integer_col;
    integer_col.values.resize(nrows);
    for(std::size_t i = 0; i < integer_col.values.size(); ++i) {
        integer_col.values[i] = patterned_integer_value(i, rng, integer_noise);
    }

    std::vector<std::optional<std::string>> string_values(nrows);
    for(std::size_t i = 0; i < string_values.size(); ++i) {
        string_values[i] = patterned_string_value(i, rng);
    }
    qdata::string_vector string_col(std::move(string_values));

    qdata::list_vector data_frame;
    data_frame.values.reserve(3);
    data_frame.values.emplace_back(qdata::object(std::move(numeric_col)));
    data_frame.values.emplace_back(qdata::object(std::move(integer_col)));
    data_frame.values.emplace_back(qdata::object(std::move(string_col)));

    data_frame.attrs.reserve(3);
    data_frame.attrs.emplace_back(make_named_attr("names", make_names_attr()));
    data_frame.attrs.emplace_back(make_named_attr("row.names", make_row_names_attr(nrows)));
    data_frame.attrs.emplace_back(make_named_attr("class", make_class_attr()));

    return qdata::object(std::move(data_frame));
}

} // namespace benchmark_data

#endif
