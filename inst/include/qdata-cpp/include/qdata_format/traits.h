#ifndef QDATA_FORMAT_TRAITS_H
#define QDATA_FORMAT_TRAITS_H

#include "core_types.h"

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace qdata {

template <class T>
struct dependent_false : std::false_type {};

template <class T, class Enable = void>
struct ATTRSXP_traits {
    static constexpr bool has_attributes = false;
};

template <class T>
struct ATTRSXP_traits<T, std::void_t<decltype(std::declval<const T&>().attrs)>> {
    static constexpr bool has_attributes = true;

    static std::size_t size(const T& x) {
        return x.attrs.size();
    }

    static std::string_view name(const T& x, std::size_t i) {
        return x.attrs[i]->name;
    }

    static decltype(auto) get(const T& x, std::size_t i) {
        return (x.attrs[i]->data);
    }
};

template <class T, class Enable = void>
struct LGLSXP_traits {};

template <class T, class Enable = void>
struct INTSXP_traits {};

template <class T, class Enable = void>
struct REALSXP_traits {};

template <class T, class Enable = void>
struct CPLXSXP_traits {};

template <class T, class Enable = void>
struct STRSXP_traits {};

template <class T, class Enable = void>
struct RAWSXP_traits {};

template <>
struct LGLSXP_traits<logical_vector> {
    static constexpr bool direct = true;

    static std::size_t size(const logical_vector& x) { return x.values.size(); }
    static const std::int32_t* data(const logical_vector& x) { return x.values.data(); }
};

template <>
struct LGLSXP_traits<std::vector<bool>> {
    static constexpr bool direct = false;

    static std::size_t size(const std::vector<bool>& x) { return x.size(); }
    static std::int32_t get(const std::vector<bool>& x, std::size_t i) { return x[i] ? true_logical : false_logical; }
};

template <>
struct LGLSXP_traits<std::vector<std::optional<bool>>> {
    static constexpr bool direct = false;

    static std::size_t size(const std::vector<std::optional<bool>>& x) { return x.size(); }
    static std::int32_t get(const std::vector<std::optional<bool>>& x, std::size_t i) {
        if(!x[i]) return na_logical;
        return *x[i] ? true_logical : false_logical;
    }
};

template <>
struct INTSXP_traits<integer_vector> {
    static constexpr bool direct = true;

    static std::size_t size(const integer_vector& x) { return x.values.size(); }
    static const std::int32_t* data(const integer_vector& x) { return x.values.data(); }
};

template <>
struct INTSXP_traits<std::vector<std::int32_t>> {
    static constexpr bool direct = true;

    static std::size_t size(const std::vector<std::int32_t>& x) { return x.size(); }
    static const std::int32_t* data(const std::vector<std::int32_t>& x) { return x.data(); }
};

template <class T>
struct INTSXP_traits<std::vector<T>, std::enable_if_t<std::is_same<T, int>::value &&
                                                      !std::is_same<T, std::int32_t>::value>> {
    static constexpr bool direct = false;

    static std::size_t size(const std::vector<T>& x) { return x.size(); }
    static std::int32_t get(const std::vector<T>& x, std::size_t i) { return static_cast<std::int32_t>(x[i]); }
};

template <>
struct INTSXP_traits<std::vector<std::optional<std::int32_t>>> {
    static constexpr bool direct = false;

    static std::size_t size(const std::vector<std::optional<std::int32_t>>& x) { return x.size(); }
    static std::int32_t get(const std::vector<std::optional<std::int32_t>>& x, std::size_t i) {
        return x[i] ? *x[i] : na_int32;
    }
};

template <>
struct REALSXP_traits<real_vector> {
    static constexpr bool direct = true;

    static std::size_t size(const real_vector& x) { return x.values.size(); }
    static const double* data(const real_vector& x) { return x.values.data(); }
};

template <>
struct REALSXP_traits<std::vector<double>> {
    static constexpr bool direct = true;

    static std::size_t size(const std::vector<double>& x) { return x.size(); }
    static const double* data(const std::vector<double>& x) { return x.data(); }
};

template <>
struct REALSXP_traits<std::vector<float>> {
    static constexpr bool direct = false;

    static std::size_t size(const std::vector<float>& x) { return x.size(); }
    static double get(const std::vector<float>& x, std::size_t i) { return static_cast<double>(x[i]); }
};

template <>
struct CPLXSXP_traits<complex_vector> {
    static constexpr bool direct = true;

    static std::size_t size(const complex_vector& x) { return x.values.size(); }
    static const std::complex<double>* data(const complex_vector& x) { return x.values.data(); }
};

template <>
struct CPLXSXP_traits<std::vector<std::complex<double>>> {
    static constexpr bool direct = true;

    static std::size_t size(const std::vector<std::complex<double>>& x) { return x.size(); }
    static const std::complex<double>* data(const std::vector<std::complex<double>>& x) { return x.data(); }
};

template <>
struct STRSXP_traits<string_vector> {
    static std::size_t size(const string_vector& x) { return x.size(); }
    static bool is_na(const string_vector& x, std::size_t i) { return x.is_na(i); }
    static std::string_view get(const string_vector& x, std::size_t i) { return x[i].view(); }
};

template <>
struct STRSXP_traits<std::vector<std::string>> {
    static std::size_t size(const std::vector<std::string>& x) { return x.size(); }
    static bool is_na(const std::vector<std::string>&, std::size_t) { return false; }
    static std::string_view get(const std::vector<std::string>& x, std::size_t i) {
        return std::string_view(x[i].data(), x[i].size());
    }
};

template <>
struct STRSXP_traits<std::vector<std::optional<std::string>>> {
    static std::size_t size(const std::vector<std::optional<std::string>>& x) { return x.size(); }
    static bool is_na(const std::vector<std::optional<std::string>>& x, std::size_t i) { return !x[i].has_value(); }
    static std::string_view get(const std::vector<std::optional<std::string>>& x, std::size_t i) {
        return x[i] ? std::string_view(x[i]->data(), x[i]->size()) : std::string_view{};
    }
};

template <>
struct STRSXP_traits<std::vector<std::string_view>> {
    static std::size_t size(const std::vector<std::string_view>& x) { return x.size(); }
    static bool is_na(const std::vector<std::string_view>&, std::size_t) { return false; }
    static std::string_view get(const std::vector<std::string_view>& x, std::size_t i) { return x[i]; }
};

template <>
struct RAWSXP_traits<raw_vector> {
    static constexpr bool direct = true;

    static std::size_t size(const raw_vector& x) { return x.values.size(); }
    static const std::byte* data(const raw_vector& x) { return x.values.data(); }
};

template <>
struct RAWSXP_traits<std::vector<std::byte>> {
    static constexpr bool direct = true;

    static std::size_t size(const std::vector<std::byte>& x) { return x.size(); }
    static const std::byte* data(const std::vector<std::byte>& x) { return x.data(); }
};

template <>
struct RAWSXP_traits<std::vector<std::uint8_t>> {
    static constexpr bool direct = true;

    static std::size_t size(const std::vector<std::uint8_t>& x) { return x.size(); }
    static const std::uint8_t* data(const std::vector<std::uint8_t>& x) { return x.data(); }
};

namespace detail {

template <class T, class Enable = void>
struct VECSXP_traits {};

template <>
struct VECSXP_traits<list_vector> {
    static std::size_t size(const list_vector& x) { return x.values.size(); }

    static decltype(auto) get(const list_vector& x, std::size_t i) {
        return (*x.values[i]);
    }
};

} // namespace detail

} // namespace qdata

#endif
