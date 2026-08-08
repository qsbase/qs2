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
#include <optional>
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
};

struct string_ref {
    const char* data = nullptr;
    std::uint32_t size = NA_STRING_LENGTH;

    bool is_na() const noexcept { return size == NA_STRING_LENGTH; }

    std::string_view view() const noexcept {
        if(is_na()) {
            return {};
        }
        return std::string_view(data, size);
    }

    operator std::string_view() const noexcept {
        return view();
    }
};

namespace detail {

inline constexpr std::size_t default_string_storage_bytes = 1U << 20;

class string_storage_builder {
public:
    explicit string_storage_builder(const std::size_t default_slab_bytes = default_string_storage_bytes) :
    storage_(std::make_shared<string_storage>()),
    default_slab_bytes_(default_slab_bytes),
    current_slab_(),
    used_(0),
    capacity_(0) {}

    string_ref append_string(const std::optional<std::string>& value) {
        if(!value) {
            return string_ref{};
        }

        char header[1 + sizeof(std::uint32_t)] = {};
        const auto string_length = static_cast<std::uint32_t>(value->size());
        const auto header_size = encode_string_header(string_length, header);
        const auto payload_size = value->size();
        char* const record = allocate_record(header_size + payload_size);
        std::memcpy(record, header, header_size);
        if(payload_size > 0) {
            std::memcpy(record + header_size, value->data(), payload_size);
        }
        return string_ref{record + header_size, string_length};
    }

    std::shared_ptr<const string_storage> freeze() {
        finalize_current_slab();
        return storage_;
    }

private:
    std::shared_ptr<string_storage> storage_;
    std::size_t default_slab_bytes_;
    std::unique_ptr<char[]> current_slab_;
    std::size_t used_;
    std::size_t capacity_;

    static std::uint32_t encode_string_header(const std::uint32_t string_length,
                                              char* const header_bytes) noexcept {
        if(string_length < MAX_STRING_8_BIT_LENGTH) {
            header_bytes[0] = static_cast<char>(string_length);
            return 1;
        }
        if(string_length < MAX_STRING_16_BIT_LENGTH) {
            header_bytes[0] = static_cast<char>(string_header_16);
            const auto narrow = static_cast<std::uint16_t>(string_length);
            std::memcpy(header_bytes + 1, &narrow, sizeof(narrow));
            return 1 + static_cast<std::uint32_t>(sizeof(narrow));
        }
        header_bytes[0] = static_cast<char>(string_header_32);
        std::memcpy(header_bytes + 1, &string_length, sizeof(string_length));
        return 1 + static_cast<std::uint32_t>(sizeof(string_length));
    }

    char* allocate_record(const std::size_t bytes) {
        if(!current_slab_ || capacity_ - used_ < bytes) {
            finalize_current_slab();
            capacity_ = std::max(default_slab_bytes_, bytes);
            current_slab_ = std::make_unique<char[]>(capacity_);
            used_ = 0;
        }

        char* const record = current_slab_.get() + used_;
        used_ += bytes;
        return record;
    }

    void finalize_current_slab() {
        if(current_slab_) {
            storage_->slabs.push_back(std::move(current_slab_));
            used_ = 0;
            capacity_ = 0;
        }
    }
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
            records.push_back(builder.append_string(value));
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
