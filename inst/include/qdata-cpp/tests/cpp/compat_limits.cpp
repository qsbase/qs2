#include <cstdint>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "qdata.h"
#include "qdata_format/detail/r_compat_limits.h"

namespace {

void debug_log(const char* message) {
    std::cerr << "[qdata_compat_limits] " << message << '\n';
    std::cerr.flush();
}

template <class Fn>
void expect_runtime_error(Fn&& fn, const std::string& needle) {
    try {
        std::forward<Fn>(fn)();
    } catch(const std::runtime_error& err) {
        if(std::string(err.what()).find(needle) == std::string::npos) {
            throw std::runtime_error("unexpected error message: " + std::string(err.what()));
        }
        return;
    }
    throw std::runtime_error("expected std::runtime_error");
}

struct huge_integer_vector {};
struct huge_string_vector {};
struct too_many_attrs_vector {};

qdata::object make_nested_object(const std::size_t wrappers) {
    qdata::object current(qdata::nil_value{});
    for(std::size_t i = 0; i < wrappers; ++i) {
        qdata::list_vector next;
        next.values.emplace_back(std::move(current));
        current = qdata::object(std::move(next));
    }
    return current;
}

void expect_huge_integer_vector_rejected() {
    if constexpr (std::numeric_limits<std::size_t>::max() > qdata::detail::max_r_compatible_vector_length) {
        const auto bytes = qdata::serialize(huge_integer_vector{});
        (void) bytes;
        throw std::runtime_error("oversized integer vector unexpectedly serialized");
    }
}

void expect_huge_string_value_rejected() {
    const auto bytes = qdata::serialize(huge_string_vector{});
    (void) bytes;
    throw std::runtime_error("oversized string unexpectedly serialized");
}

void expect_too_many_attrs_rejected() {
    const auto bytes = qdata::serialize(too_many_attrs_vector{});
    (void) bytes;
    throw std::runtime_error("oversized attribute count unexpectedly serialized");
}

void expect_modest_nesting_succeeds() {
    const auto bytes = qdata::serialize(make_nested_object(16));
    if(bytes.empty()) {
        throw std::runtime_error("modestly nested object did not serialize");
    }
}

void expect_deep_nesting_rejected() {
    const auto bytes = qdata::serialize(make_nested_object(600));
    (void) bytes;
    throw std::runtime_error("deeply nested object unexpectedly serialized");
}

void expect_deep_nesting_override_succeeds() {
    const auto bytes = qdata::serialize(make_nested_object(600), 3, true, 1, 1024);
    if(bytes.empty()) {
        throw std::runtime_error("deeply nested object did not serialize with raised max_depth");
    }
    const auto roundtrip = qdata::deserialize(bytes, false, 1, 1024);
    if(!qdata::holds_alternative<qdata::list_vector>(roundtrip)) {
        throw std::runtime_error("deeply nested object did not roundtrip as a list");
    }
}

void expect_deep_deserialize_default_rejected() {
    const auto bytes = qdata::serialize(make_nested_object(600), 3, true, 1, 1024);
    const auto roundtrip = qdata::deserialize(bytes);
    (void) roundtrip;
    throw std::runtime_error("deeply nested bytes unexpectedly deserialized under default max_depth");
}

} // namespace

namespace qdata {

template <>
struct INTSXP_traits<huge_integer_vector> {
    static constexpr bool direct = false;

    static std::size_t size(const huge_integer_vector&) {
        if constexpr (std::numeric_limits<std::size_t>::max() > detail::max_r_compatible_vector_length) {
            return static_cast<std::size_t>(detail::max_r_compatible_vector_length + 1ULL);
        } else {
            return 0;
        }
    }

    static std::int32_t get(const huge_integer_vector&, std::size_t) {
        return 0;
    }
};

template <>
struct STRSXP_traits<huge_string_vector> {
    static std::size_t size(const huge_string_vector&) {
        return 1;
    }

    static bool is_na(const huge_string_vector&, std::size_t) {
        return false;
    }

    static std::string_view get(const huge_string_vector&, std::size_t) {
        static constexpr char dummy = 'x';
        return std::string_view(&dummy, static_cast<std::size_t>(detail::max_r_compatible_string_length) + 1U);
    }
};

template <>
struct INTSXP_traits<too_many_attrs_vector> {
    static constexpr bool direct = false;

    static std::size_t size(const too_many_attrs_vector&) {
        return 0;
    }

    static std::int32_t get(const too_many_attrs_vector&, std::size_t) {
        return 0;
    }
};

template <>
struct ATTRSXP_traits<too_many_attrs_vector> {
    static constexpr bool has_attributes = true;

    static std::size_t size(const too_many_attrs_vector&) {
        return static_cast<std::size_t>(detail::max_r_compatible_attr_count) + 1U;
    }

    static std::string_view name(const too_many_attrs_vector&, std::size_t) {
        return "attr";
    }

    static const writable& get(const too_many_attrs_vector&, std::size_t) {
        static const writable empty_attr = writable::own(std::vector<std::int32_t>{});
        return empty_attr;
    }
};

} // namespace qdata

int main() {
    debug_log("start");

    if constexpr (std::numeric_limits<std::size_t>::max() > qdata::detail::max_r_compatible_vector_length) {
        debug_log("oversized vector length is rejected");
        expect_runtime_error(&expect_huge_integer_vector_rejected, "vector length limit");
    }

    debug_log("oversized string length is rejected");
    expect_runtime_error(&expect_huge_string_value_rejected, "string length limit");

    debug_log("oversized attribute count is rejected");
    expect_runtime_error(&expect_too_many_attrs_rejected, "attribute count exceeds");

    debug_log("modest nesting succeeds");
    expect_modest_nesting_succeeds();

    debug_log("deep nesting is rejected");
    expect_runtime_error(&expect_deep_nesting_rejected, "nesting depth exceeds");

    debug_log("default deserialize limit rejects deep input");
    expect_runtime_error(&expect_deep_deserialize_default_rejected, "nesting depth exceeds");

    debug_log("explicit max_depth override succeeds");
    expect_deep_nesting_override_succeeds();

    debug_log("done");
    return 0;
}
