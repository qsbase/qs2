#ifndef QDATA_FORMAT_CORE_TYPES_H
#define QDATA_FORMAT_CORE_TYPES_H

#include "detail/constants.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <complex>
#include <cstring>
#include <initializer_list>
#include <iterator>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace qdata {

inline constexpr std::int32_t na_int32 = std::numeric_limits<std::int32_t>::min();
inline constexpr std::int32_t false_logical = 0;
inline constexpr std::int32_t true_logical = 1;
inline constexpr std::int32_t na_logical = std::numeric_limits<std::int32_t>::min();

template <class T>
struct box {
    std::unique_ptr<T> ptr;

    box() : ptr(std::make_unique<T>()) {}
    box(const T& value) : ptr(std::make_unique<T>(value)) {}
    box(T&& value) : ptr(std::make_unique<T>(std::move(value))) {}
    box(const box& other) :
    ptr(other.ptr ? std::make_unique<T>(*other.ptr) : nullptr) {}
    box(box&&) noexcept = default;

    box& operator=(const box& other) {
        if(this != &other) {
            ptr = other.ptr ? std::make_unique<T>(*other.ptr) : nullptr;
        }
        return *this;
    }
    box& operator=(box&&) noexcept = default;

    T& operator*() { return *ptr; }
    const T& operator*() const { return *ptr; }
    T* operator->() { return ptr.get(); }
    const T* operator->() const { return ptr.get(); }
    T* get() { return ptr.get(); }
    const T* get() const { return ptr.get(); }
};

struct object;
struct named_object;

struct nil_value {};

struct string_storage {
    std::vector<std::unique_ptr<char[]>> slabs;
    std::size_t capacity_bytes = 0;
};

struct string_ref {
    const char* data = nullptr;
    std::uint32_t size = NA_STRING_LENGTH;

    bool is_na() const noexcept { return size == NA_STRING_LENGTH; }

    std::string_view view() const noexcept {
        if(is_na() || size == 0) {
            return {};
        }
        return std::string_view(data, size);
    }

    operator std::string_view() const noexcept {
        return view();
    }
};

namespace detail {

inline constexpr std::size_t min_multi_string_slice_bytes = 64;
inline constexpr std::size_t max_initial_string_slice_bytes = 16U << 10;
inline constexpr std::size_t max_regular_string_slice_bytes = 256U << 10;
inline constexpr std::size_t string_slice_alignment = 64;
inline constexpr std::size_t string_vector_length_scale = 4;

inline std::size_t initial_string_slice_bytes(const std::size_t vector_length) noexcept {
    if(vector_length <= 1) {
        return 0;
    }
    if(vector_length >= max_initial_string_slice_bytes / string_vector_length_scale) {
        return max_initial_string_slice_bytes;
    }

    const auto scaled = vector_length * string_vector_length_scale;
    std::size_t rounded = 1;
    while(rounded < scaled) {
        rounded <<= 1;
    }
    return std::min(
        std::max(min_multi_string_slice_bytes, rounded),
        max_initial_string_slice_bytes
    );
}

inline std::size_t round_up_string_slice_bytes(const std::size_t bytes) {
    const auto remainder = bytes % string_slice_alignment;
    if(remainder == 0) {
        return bytes;
    }
    const auto padding = string_slice_alignment - remainder;
    if(bytes > std::numeric_limits<std::size_t>::max() - padding) {
        throw std::bad_alloc();
    }
    return bytes + padding;
}

class string_storage_builder {
public:
    string_storage_builder() : storage_(std::make_shared<string_storage>()) {}
    string_storage_builder(const string_storage_builder&) = delete;
    string_storage_builder& operator=(const string_storage_builder&) = delete;
    string_storage_builder(string_storage_builder&&) = delete;
    string_storage_builder& operator=(string_storage_builder&&) = delete;

    string_ref append_string(const std::optional<std::string>& value,
                             const std::size_t vector_length_hint = 0) {
        if(!value) {
            return string_ref{};
        }

        if(value->size() >= static_cast<std::size_t>(NA_STRING_LENGTH)) {
            throw std::length_error("string length collides with the NA string sentinel");
        }
        const auto string_length = static_cast<std::uint32_t>(value->size());
        char* const payload = allocate_bytes(value->size(), vector_length_hint);
        if(string_length > 0) {
            std::memcpy(payload, value->data(), string_length);
        }
        return string_ref{payload, string_length};
    }

    char* allocate_bytes(const std::size_t bytes, const std::size_t vector_length_hint = 0) {
        if(bytes == 0) {
            return nullptr;
        }

        if(current_slab_ == nullptr || current_capacity_ - current_used_ < bytes) {
            const auto initial = initial_string_slice_bytes(vector_length_hint);
            const auto growth_base = std::min(
                payload_bytes_ / 2,
                max_regular_string_slice_bytes
            );
            const auto regular = std::min(
                round_up_string_slice_bytes(std::max(initial, growth_base)),
                max_regular_string_slice_bytes
            );
            const auto next_capacity = round_up_string_slice_bytes(std::max(regular, bytes));

            if(storage_->capacity_bytes > std::numeric_limits<std::size_t>::max() - next_capacity) {
                throw std::bad_alloc();
            }
            std::unique_ptr<char[]> next_slab(new char[next_capacity]);
            storage_->slabs.push_back(std::move(next_slab));
            storage_->capacity_bytes += next_capacity;
            current_slab_ = storage_->slabs.back().get();
            current_used_ = 0;
            current_capacity_ = next_capacity;
        }

        char* const output = current_slab_ + current_used_;
        current_used_ += bytes;
        payload_bytes_ += bytes;
        return output;
    }

    std::shared_ptr<const string_storage> storage() const noexcept {
        return storage_;
    }

    std::shared_ptr<const string_storage> freeze() const noexcept { return storage(); }

private:
    std::shared_ptr<string_storage> storage_;
    char* current_slab_ = nullptr;
    std::size_t current_used_ = 0;
    std::size_t current_capacity_ = 0;
    std::size_t payload_bytes_ = 0;
};

} // namespace detail

struct logical_vector {
    std::vector<std::int32_t> values;
    std::vector<box<named_object>> attrs;

    std::size_t size() const noexcept { return values.size(); }
    bool empty() const noexcept { return values.empty(); }
    decltype(auto) operator[](const std::size_t i) { return (values[i]); }
    decltype(auto) operator[](const std::size_t i) const { return (values[i]); }
    auto begin() noexcept { return values.begin(); }
    auto end() noexcept { return values.end(); }
    auto begin() const noexcept { return values.begin(); }
    auto end() const noexcept { return values.end(); }
};

struct integer_vector {
    std::vector<std::int32_t> values;
    std::vector<box<named_object>> attrs;

    std::size_t size() const noexcept { return values.size(); }
    bool empty() const noexcept { return values.empty(); }
    decltype(auto) operator[](const std::size_t i) { return (values[i]); }
    decltype(auto) operator[](const std::size_t i) const { return (values[i]); }
    auto begin() noexcept { return values.begin(); }
    auto end() noexcept { return values.end(); }
    auto begin() const noexcept { return values.begin(); }
    auto end() const noexcept { return values.end(); }
};

struct real_vector {
    std::vector<double> values;
    std::vector<box<named_object>> attrs;

    std::size_t size() const noexcept { return values.size(); }
    bool empty() const noexcept { return values.empty(); }
    decltype(auto) operator[](const std::size_t i) { return (values[i]); }
    decltype(auto) operator[](const std::size_t i) const { return (values[i]); }
    auto begin() noexcept { return values.begin(); }
    auto end() noexcept { return values.end(); }
    auto begin() const noexcept { return values.begin(); }
    auto end() const noexcept { return values.end(); }
};

struct complex_vector {
    std::vector<std::complex<double>> values;
    std::vector<box<named_object>> attrs;

    std::size_t size() const noexcept { return values.size(); }
    bool empty() const noexcept { return values.empty(); }
    decltype(auto) operator[](const std::size_t i) { return (values[i]); }
    decltype(auto) operator[](const std::size_t i) const { return (values[i]); }
    auto begin() noexcept { return values.begin(); }
    auto end() noexcept { return values.end(); }
    auto begin() const noexcept { return values.begin(); }
    auto end() const noexcept { return values.end(); }
};

struct string_vector {
    std::shared_ptr<const string_storage> storage;
    std::vector<string_ref> records;
    std::vector<box<named_object>> attrs;

    string_vector() = default;

    explicit string_vector(std::vector<std::optional<std::string>> values_in) {
        assign_strings(values_in);
    }

    string_vector(std::vector<std::optional<std::string>> values_in,
                  std::vector<box<named_object>> attrs_in) :
    attrs(std::move(attrs_in)) {
        assign_strings(values_in);
    }

    string_vector(std::initializer_list<std::optional<std::string>> values_in,
                  std::vector<box<named_object>> attrs_in = {}) :
    string_vector(std::vector<std::optional<std::string>>(values_in), std::move(attrs_in)) {}

    std::size_t size() const noexcept { return records.size(); }
    bool empty() const noexcept { return records.empty(); }
    bool is_na(const std::size_t i) const noexcept { return records[i].is_na(); }
    string_ref operator[](const std::size_t i) const noexcept { return records[i]; }

    class const_iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = string_ref;
        using difference_type = std::ptrdiff_t;

        const_iterator() = default;
        explicit const_iterator(std::vector<string_ref>::const_iterator current) : current_(current) {}

        string_ref operator*() const noexcept { return *current_; }
        const_iterator& operator++() noexcept { ++current_; return *this; }
        const_iterator operator++(int) noexcept {
            const_iterator copy(*this);
            ++current_;
            return copy;
        }
        bool operator==(const const_iterator& other) const noexcept { return current_ == other.current_; }
        bool operator!=(const const_iterator& other) const noexcept { return current_ != other.current_; }

    private:
        std::vector<string_ref>::const_iterator current_;
    };

    const_iterator begin() const noexcept { return const_iterator(records.begin()); }
    const_iterator end() const noexcept { return const_iterator(records.end()); }

private:
    void assign_strings(const std::vector<std::optional<std::string>>& values_in) {
        detail::string_storage_builder builder;
        records.reserve(values_in.size());
        for(const auto& value : values_in) {
            records.push_back(builder.append_string(value, values_in.size()));
        }
        storage = builder.freeze();
    }
};

struct raw_vector {
    std::vector<std::byte> values;
    std::vector<box<named_object>> attrs;

    std::size_t size() const noexcept { return values.size(); }
    bool empty() const noexcept { return values.empty(); }
    decltype(auto) operator[](const std::size_t i) { return (values[i]); }
    decltype(auto) operator[](const std::size_t i) const { return (values[i]); }
    auto begin() noexcept { return values.begin(); }
    auto end() noexcept { return values.end(); }
    auto begin() const noexcept { return values.begin(); }
    auto end() const noexcept { return values.end(); }
};

struct list_vector {
    std::vector<box<object>> values;
    std::vector<box<named_object>> attrs;

    std::size_t size() const noexcept { return values.size(); }
    bool empty() const noexcept { return values.empty(); }
    object& operator[](const std::size_t i) noexcept { return *values[i]; }
    const object& operator[](const std::size_t i) const noexcept { return *values[i]; }
    auto begin() noexcept { return values.begin(); }
    auto end() noexcept { return values.end(); }
    auto begin() const noexcept { return values.begin(); }
    auto end() const noexcept { return values.end(); }
};

struct object {
    using data_type = std::variant<
        nil_value,
        logical_vector,
        integer_vector,
        real_vector,
        complex_vector,
        string_vector,
        raw_vector,
        list_vector
    >;

    data_type data;

    object() = default;
    object(const object&) = default;
    object(object&&) noexcept = default;
    object& operator=(const object&) = default;
    object& operator=(object&&) noexcept = default;

    template <class T,
              std::enable_if_t<!std::is_same<std::decay_t<T>, object>::value, int> = 0>
    object(T&& x) : data(std::forward<T>(x)) {}
};

template <class T>
decltype(auto) get(object& x) {
    return std::get<T>(x.data);
}

template <class T>
decltype(auto) get(const object& x) {
    return std::get<T>(x.data);
}

template <class T>
decltype(auto) get(object&& x) {
    return std::get<T>(std::move(x.data));
}

template <class T>
decltype(auto) get(const object&& x) {
    return std::get<T>(std::move(x.data));
}

template <class T>
T* get_if(object* x) noexcept {
    return x ? std::get_if<T>(&x->data) : nullptr;
}

template <class T>
const T* get_if(const object* x) noexcept {
    return x ? std::get_if<T>(&x->data) : nullptr;
}

template <class T>
bool holds_alternative(const object& x) noexcept {
    return std::holds_alternative<T>(x.data);
}

struct named_object {
    std::string name;
    object data;
};

} // namespace qdata

#endif
