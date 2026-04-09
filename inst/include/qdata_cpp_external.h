#ifndef QDATA_CPP_EXTERNAL_H
#define QDATA_CPP_EXTERNAL_H

#include <R_ext/Rdynload.h>

#include "qdata-cpp/include/qdata_format/write_traits.h"
#include "qdata-cpp/include/qdata_format/detail/byte_buffer.h"

#include <cstddef>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace qdata_ext {

using qdata::nil_value;
using qdata::logical_vector;
using qdata::integer_vector;
using qdata::real_vector;
using qdata::complex_vector;
using qdata::string_vector;
using qdata::raw_vector;
using qdata::list_vector;
using qdata::named_object;
using qdata::object;
using qdata::writable;
using qdata::get;
using qdata::get_if;
using qdata::holds_alternative;

inline constexpr std::size_t default_max_depth = qdata::detail::default_qdata_max_nesting_depth;

namespace detail {

using save_erased_fn = void (*)(const std::string&,
                                const void*,
                                qdata::detail::erased_write_fn,
                                int,
                                bool,
                                int,
                                std::size_t);

using serialize_erased_fn = void (*)(void*,
                                     qdata::detail::output_buffer_ops,
                                     const void*,
                                     qdata::detail::erased_write_fn,
                                     int,
                                     bool,
                                     int,
                                     std::size_t);

using read_fn = qdata::object (*)(const std::string&, bool, int, std::size_t);
using deserialize_fn = qdata::object (*)(const void*, std::size_t, bool, int, std::size_t);

template <class Fn>
inline Fn get_qs2_callable(const char* const name) {
    return reinterpret_cast<Fn>(R_GetCCallable("qs2", name));
}

} // namespace detail

template <class T>
inline void save(const std::string& file,
                 const T& object_value,
                 const int compress_level = 3,
                 const bool shuffle = true,
                 const int nthreads = 1,
                 const std::size_t max_depth = default_max_depth) {
    const auto fun = detail::get_qs2_callable<detail::save_erased_fn>("qdata_cpp_save_erased");
    fun(
        file,
        std::addressof(object_value),
        &qdata::detail::write_erased<std::decay_t<T>>,
        compress_level,
        shuffle,
        nthreads,
        max_depth
    );
}

template <class Buffer = std::vector<std::byte>, class T>
inline Buffer serialize(const T& object_value,
                        const int compress_level = 3,
                        const bool shuffle = true,
                        const int nthreads = 1,
                        const std::size_t max_depth = default_max_depth) {
    qdata::detail::validate_output_buffer<Buffer>();
    Buffer output;
    const auto fun = detail::get_qs2_callable<detail::serialize_erased_fn>("qdata_cpp_serialize_erased");
    fun(
        static_cast<void*>(std::addressof(output)),
        qdata::detail::make_output_buffer_ops<Buffer>(),
        std::addressof(object_value),
        &qdata::detail::write_erased<std::decay_t<T>>,
        compress_level,
        shuffle,
        nthreads,
        max_depth
    );
    return output;
}

inline object read(const std::string& file,
                   const bool validate_checksum = false,
                   const int nthreads = 1,
                   const std::size_t max_depth = default_max_depth) {
    const auto fun = detail::get_qs2_callable<detail::read_fn>("qdata_cpp_read");
    return fun(file, validate_checksum, nthreads, max_depth);
}

inline object deserialize(const void* const data,
                          const std::size_t size,
                          const bool validate_checksum = false,
                          const int nthreads = 1,
                          const std::size_t max_depth = default_max_depth) {
    const auto fun = detail::get_qs2_callable<detail::deserialize_fn>("qdata_cpp_deserialize");
    return fun(data, size, validate_checksum, nthreads, max_depth);
}

template <class Buffer,
          std::enable_if_t<qdata::detail::is_byte_input_buffer<Buffer>::value, int> = 0>
inline object deserialize(const Buffer& data,
                          const bool validate_checksum = false,
                          const int nthreads = 1,
                          const std::size_t max_depth = default_max_depth) {
    return deserialize(
        qdata::detail::buffer_data(data),
        qdata::detail::buffer_size_bytes(data),
        validate_checksum,
        nthreads,
        max_depth
    );
}

} // namespace qdata_ext

#endif
