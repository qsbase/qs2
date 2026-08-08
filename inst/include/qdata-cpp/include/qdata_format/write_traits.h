#ifndef QDATA_FORMAT_WRITE_TRAITS_H
#define QDATA_FORMAT_WRITE_TRAITS_H

#include "traits.h"
#include "detail/r_compat_limits.h"

#include <cstddef>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <utility>

namespace qdata {

class serializer;
class writable;

namespace detail {

using erased_write_fn = void (*)(serializer&, const void*);

} // namespace detail

template <class T>
void write_object(serializer& s, const T& x);

class serializer {
public:
    explicit serializer(const std::size_t max_nesting_depth = detail::default_qdata_max_nesting_depth) :
    max_nesting_depth_(detail::checked_max_nesting_depth(max_nesting_depth)) {}

    virtual ~serializer() = default;

    virtual void write_nil() = 0;

    virtual void begin_logical_vector(std::size_t size, std::size_t attr_count) = 0;
    virtual void begin_integer_vector(std::size_t size, std::size_t attr_count) = 0;
    virtual void begin_real_vector(std::size_t size, std::size_t attr_count) = 0;
    virtual void begin_complex_vector(std::size_t size, std::size_t attr_count) = 0;
    virtual void begin_string_vector(std::size_t size, std::size_t attr_count) = 0;
    virtual void begin_raw_vector(std::size_t size, std::size_t attr_count) = 0;
    virtual void begin_list_vector(std::size_t size, std::size_t attr_count) = 0;

    virtual void write_logical_data(const std::int32_t* data, std::size_t size) = 0;
    virtual void write_integer_data(const std::int32_t* data, std::size_t size) = 0;
    virtual void write_real_data(const double* data, std::size_t size) = 0;
    virtual void write_complex_data(const std::complex<double>* data, std::size_t size) = 0;
    virtual void write_raw_data(const std::byte* data, std::size_t size) = 0;

    virtual void write_logical_value(std::int32_t value) = 0;
    virtual void write_integer_value(std::int32_t value) = 0;
    virtual void write_real_value(double value) = 0;
    virtual void write_complex_value(const std::complex<double>& value) = 0;
    virtual void write_string_value(std::string_view value, bool is_na) = 0;
    virtual void write_raw_value(std::byte value) = 0;

    virtual void write_attribute_name(std::string_view name) = 0;
    virtual void defer_integer_payload(const void* object_ptr, detail::erased_write_fn emit_fn) = 0;
    virtual void defer_real_payload(const void* object_ptr, detail::erased_write_fn emit_fn) = 0;
    virtual void defer_complex_payload(const void* object_ptr, detail::erased_write_fn emit_fn) = 0;
    virtual void defer_string_payload(const void* object_ptr, detail::erased_write_fn emit_fn) = 0;
    virtual void defer_raw_payload(const void* object_ptr, detail::erased_write_fn emit_fn) = 0;

    void enter_object_scope() {
        if(current_nesting_depth_ >= max_nesting_depth_) {
            throw std::runtime_error("qdata nesting depth exceeds configured max_depth");
        }
        ++current_nesting_depth_;
    }

    void leave_object_scope() noexcept {
        --current_nesting_depth_;
    }

private:
    std::size_t max_nesting_depth_;
    std::size_t current_nesting_depth_ = 0;
};

namespace detail {

class serializer_depth_guard {
public:
    explicit serializer_depth_guard(serializer& serializer) : serializer_(serializer) {
        serializer_.enter_object_scope();
    }

    ~serializer_depth_guard() {
        serializer_.leave_object_scope();
    }

private:
    serializer& serializer_;
};

template <class T>
using decay_t = std::decay_t<T>;

template <class T, class = void>
struct has_logical_traits : std::false_type {};

template <class T>
struct has_logical_traits<T, std::void_t<
    decltype(LGLSXP_traits<decay_t<T>>::direct),
    decltype(LGLSXP_traits<decay_t<T>>::size(std::declval<const decay_t<T>&>()))
>> : std::true_type {};

template <class T, class = void>
struct has_integer_traits : std::false_type {};

template <class T>
struct has_integer_traits<T, std::void_t<
    decltype(INTSXP_traits<decay_t<T>>::direct),
    decltype(INTSXP_traits<decay_t<T>>::size(std::declval<const decay_t<T>&>()))
>> : std::true_type {};

template <class T, class = void>
struct has_real_traits : std::false_type {};

template <class T>
struct has_real_traits<T, std::void_t<
    decltype(REALSXP_traits<decay_t<T>>::direct),
    decltype(REALSXP_traits<decay_t<T>>::size(std::declval<const decay_t<T>&>()))
>> : std::true_type {};

template <class T, class = void>
struct has_complex_traits : std::false_type {};

template <class T>
struct has_complex_traits<T, std::void_t<
    decltype(CPLXSXP_traits<decay_t<T>>::direct),
    decltype(CPLXSXP_traits<decay_t<T>>::size(std::declval<const decay_t<T>&>()))
>> : std::true_type {};

template <class T, class = void>
struct has_string_traits : std::false_type {};

template <class T>
struct has_string_traits<T, std::void_t<
    decltype(STRSXP_traits<decay_t<T>>::size(std::declval<const decay_t<T>&>())),
    decltype(STRSXP_traits<decay_t<T>>::get(std::declval<const decay_t<T>&>(), std::size_t{}))
>> : std::true_type {};

template <class T, class = void>
struct has_string_na_traits : std::false_type {};

template <class T>
struct has_string_na_traits<T, std::void_t<
    decltype(STRSXP_traits<decay_t<T>>::is_na(std::declval<const decay_t<T>&>(), std::size_t{}))
>> : std::true_type {};

template <class T, class = void>
struct has_raw_traits : std::false_type {};

template <class T>
struct has_raw_traits<T, std::void_t<
    decltype(RAWSXP_traits<decay_t<T>>::direct),
    decltype(RAWSXP_traits<decay_t<T>>::size(std::declval<const decay_t<T>&>()))
>> : std::true_type {};

template <class T>
struct has_leaf_traits : std::bool_constant<
    has_logical_traits<T>::value ||
    has_integer_traits<T>::value ||
    has_real_traits<T>::value ||
    has_complex_traits<T>::value ||
    has_string_traits<T>::value ||
    has_raw_traits<T>::value
> {};

template <class T, class = void>
struct has_internal_list_traits : std::false_type {};

template <class T>
struct has_internal_list_traits<T, std::void_t<
    decltype(VECSXP_traits<decay_t<T>>::size(std::declval<const decay_t<T>&>())),
    decltype(VECSXP_traits<decay_t<T>>::get(std::declval<const decay_t<T>&>(), std::size_t{}))
>> : std::true_type {};

template <class T, class = void>
struct is_sized_iterable : std::false_type {};

template <class T>
struct is_sized_iterable<T, std::void_t<
    decltype(std::begin(std::declval<const T&>())),
    decltype(std::end(std::declval<const T&>())),
    decltype(std::size(std::declval<const T&>()))
>> : std::true_type {};

template <class T>
using iter_value_t = decay_t<decltype(*std::begin(std::declval<const T&>()))>;

template <class T>
struct is_writable;

template <class T, class = void>
struct is_generic_list : std::false_type {};

template <class T>
struct is_generic_list<T, std::void_t<
    decltype(std::begin(std::declval<const T&>())),
    decltype(std::end(std::declval<const T&>())),
    decltype(std::size(std::declval<const T&>()))
>> : std::bool_constant<
    !has_leaf_traits<T>::value &&
    !has_internal_list_traits<T>::value &&
    is_writable<iter_value_t<T>>::value
> {};

template <class T>
struct is_writable : std::bool_constant<
    std::is_same<decay_t<T>, nil_value>::value ||
    std::is_same<decay_t<T>, object>::value ||
    std::is_same<decay_t<T>, writable>::value ||
    has_leaf_traits<T>::value ||
    has_internal_list_traits<T>::value ||
    is_generic_list<T>::value
> {};

template <class T>
std::size_t attr_count(const T& x) {
    using traits = ATTRSXP_traits<decay_t<T>>;
    if constexpr (traits::has_attributes) {
        return traits::size(x);
    } else {
        return 0;
    }
}

template <class T>
void write_attributes(serializer& s, const T& x) {
    using traits = ATTRSXP_traits<decay_t<T>>;
    if constexpr (traits::has_attributes) {
        const auto n = traits::size(x);
        for(std::size_t i = 0; i < n; ++i) {
            s.write_attribute_name(traits::name(x, i));
            write_object(s, traits::get(x, i));
        }
    }
}

template <class T>
void emit_logical_payload(serializer& s, const void* ptr) {
    const auto& x = *static_cast<const decay_t<T>*>(ptr);
    using traits = LGLSXP_traits<decay_t<T>>;
    const auto n = traits::size(x);
    if constexpr (traits::direct) {
        s.write_logical_data(traits::data(x), n);
    } else {
        for(std::size_t i = 0; i < n; ++i) {
            s.write_logical_value(traits::get(x, i));
        }
    }
}

template <class T>
void write_logical_vector(serializer& s, const T& x) {
    using traits = LGLSXP_traits<decay_t<T>>;
    const auto n = traits::size(x);
    s.begin_logical_vector(n, attr_count(x));
    write_attributes(s, x);
    if(n > 0) {
        s.defer_integer_payload(std::addressof(x), &emit_logical_payload<decay_t<T>>);
    }
}

template <class T>
void emit_integer_payload(serializer& s, const void* ptr) {
    const auto& x = *static_cast<const decay_t<T>*>(ptr);
    using traits = INTSXP_traits<decay_t<T>>;
    const auto n = traits::size(x);
    if constexpr (traits::direct) {
        s.write_integer_data(traits::data(x), n);
    } else {
        for(std::size_t i = 0; i < n; ++i) {
            s.write_integer_value(traits::get(x, i));
        }
    }
}

template <class T>
void write_integer_vector(serializer& s, const T& x) {
    using traits = INTSXP_traits<decay_t<T>>;
    const auto n = traits::size(x);
    s.begin_integer_vector(n, attr_count(x));
    write_attributes(s, x);
    if(n > 0) {
        s.defer_integer_payload(std::addressof(x), &emit_integer_payload<decay_t<T>>);
    }
}

template <class T>
void emit_real_payload(serializer& s, const void* ptr) {
    const auto& x = *static_cast<const decay_t<T>*>(ptr);
    using traits = REALSXP_traits<decay_t<T>>;
    const auto n = traits::size(x);
    if constexpr (traits::direct) {
        s.write_real_data(traits::data(x), n);
    } else {
        for(std::size_t i = 0; i < n; ++i) {
            s.write_real_value(traits::get(x, i));
        }
    }
}

template <class T>
void write_real_vector(serializer& s, const T& x) {
    using traits = REALSXP_traits<decay_t<T>>;
    const auto n = traits::size(x);
    s.begin_real_vector(n, attr_count(x));
    write_attributes(s, x);
    if(n > 0) {
        s.defer_real_payload(std::addressof(x), &emit_real_payload<decay_t<T>>);
    }
}

template <class T>
void emit_complex_payload(serializer& s, const void* ptr) {
    const auto& x = *static_cast<const decay_t<T>*>(ptr);
    using traits = CPLXSXP_traits<decay_t<T>>;
    const auto n = traits::size(x);
    if constexpr (traits::direct) {
        s.write_complex_data(traits::data(x), n);
    } else {
        for(std::size_t i = 0; i < n; ++i) {
            s.write_complex_value(traits::get(x, i));
        }
    }
}

template <class T>
void write_complex_vector(serializer& s, const T& x) {
    using traits = CPLXSXP_traits<decay_t<T>>;
    const auto n = traits::size(x);
    s.begin_complex_vector(n, attr_count(x));
    write_attributes(s, x);
    if(n > 0) {
        s.defer_complex_payload(std::addressof(x), &emit_complex_payload<decay_t<T>>);
    }
}

template <class T>
void emit_string_payload(serializer& s, const void* ptr) {
    const auto& x = *static_cast<const decay_t<T>*>(ptr);
    using traits = STRSXP_traits<decay_t<T>>;
    const auto n = traits::size(x);
    for(std::size_t i = 0; i < n; ++i) {
        bool is_na = false;
        if constexpr (has_string_na_traits<T>::value) {
            is_na = traits::is_na(x, i);
        }
        s.write_string_value(traits::get(x, i), is_na);
    }
}

template <class T>
void write_string_vector(serializer& s, const T& x) {
    using traits = STRSXP_traits<decay_t<T>>;
    const auto n = traits::size(x);
    s.begin_string_vector(n, attr_count(x));
    write_attributes(s, x);
    if(n > 0) {
        s.defer_string_payload(std::addressof(x), &emit_string_payload<decay_t<T>>);
    }
}

template <class T>
void emit_raw_payload(serializer& s, const void* ptr) {
    const auto& x = *static_cast<const decay_t<T>*>(ptr);
    using traits = RAWSXP_traits<decay_t<T>>;
    const auto n = traits::size(x);
    if constexpr (traits::direct) {
        s.write_raw_data(reinterpret_cast<const std::byte*>(traits::data(x)), n);
    } else {
        for(std::size_t i = 0; i < n; ++i) {
            s.write_raw_value(traits::get(x, i));
        }
    }
}

template <class T>
void write_raw_vector(serializer& s, const T& x) {
    using traits = RAWSXP_traits<decay_t<T>>;
    const auto n = traits::size(x);
    s.begin_raw_vector(n, attr_count(x));
    write_attributes(s, x);
    if(n > 0) {
        s.defer_raw_payload(std::addressof(x), &emit_raw_payload<decay_t<T>>);
    }
}

template <class T>
void write_internal_list(serializer& s, const T& x) {
    using traits = VECSXP_traits<decay_t<T>>;
    const auto n = traits::size(x);
    s.begin_list_vector(n, attr_count(x));
    write_attributes(s, x);
    for(std::size_t i = 0; i < n; ++i) {
        write_object(s, traits::get(x, i));
    }
}

template <class T>
void write_iterable_list(serializer& s, const T& x) {
    const auto n = std::size(x);
    s.begin_list_vector(n, attr_count(x));
    write_attributes(s, x);
    for(const auto& value : x) {
        write_object(s, value);
    }
}

template <class T>
void write_erased(serializer& s, const void* ptr) {
    write_object(s, *static_cast<const T*>(ptr));
}

} // namespace detail

class writable {
public:
    const void* ptr;
    void (*write_fn)(serializer&, const void*);
    std::shared_ptr<const void> owner;

    writable() : ptr(nullptr), write_fn(nullptr), owner() {}

    void write(serializer& s) const {
        if(write_fn == nullptr || ptr == nullptr) {
            throw std::logic_error("qdata::writable is not initialized");
        }
        write_fn(s, ptr);
    }

    template <class T>
    static writable ref(const T& x) {
        return writable(std::addressof(x), &detail::write_erased<detail::decay_t<T>>, {});
    }

    template <class T, std::enable_if_t<!std::is_lvalue_reference<T>::value, int> = 0>
    static writable ref(T&&) = delete;

    template <class T>
    static writable own(T x) {
        using stored_type = detail::decay_t<T>;
        auto p = std::make_shared<stored_type>(std::move(x));
        return writable(p.get(), &detail::write_erased<stored_type>, p);
    }

private:
    writable(const void* ptr_in,
             void (*write_fn_in)(serializer&, const void*),
             std::shared_ptr<const void> owner_in) :
        ptr(ptr_in), write_fn(write_fn_in), owner(std::move(owner_in)) {}
};

inline void write_object(serializer& s, const nil_value&) {
    detail::serializer_depth_guard depth_guard(s);
    s.write_nil();
}

inline void write_object(serializer& s, const object& x) {
    std::visit([&](const auto& value) {
        write_object(s, value);
    }, x.data);
}

inline void write_object(serializer& s, const writable& x) {
    x.write(s);
}

template <class T>
void write_object(serializer& s, const T& x) {
    detail::serializer_depth_guard depth_guard(s);
    using decayed_type = detail::decay_t<T>;
    if constexpr (detail::has_logical_traits<decayed_type>::value) {
        detail::write_logical_vector(s, x);
    } else if constexpr (detail::has_integer_traits<decayed_type>::value) {
        detail::write_integer_vector(s, x);
    } else if constexpr (detail::has_real_traits<decayed_type>::value) {
        detail::write_real_vector(s, x);
    } else if constexpr (detail::has_complex_traits<decayed_type>::value) {
        detail::write_complex_vector(s, x);
    } else if constexpr (detail::has_string_traits<decayed_type>::value) {
        detail::write_string_vector(s, x);
    } else if constexpr (detail::has_raw_traits<decayed_type>::value) {
        detail::write_raw_vector(s, x);
    } else if constexpr (detail::has_internal_list_traits<decayed_type>::value) {
        detail::write_internal_list(s, x);
    } else if constexpr (detail::is_generic_list<decayed_type>::value) {
        detail::write_iterable_list(s, x);
    } else {
        static_assert(dependent_false<T>::value, "qdata::write_object does not support this type");
    }
}

} // namespace qdata

#endif
