#ifndef QDATA_FORMAT_QDATA_H
#define QDATA_FORMAT_QDATA_H

#include "core_types.h"
#include "traits.h"
#include "write_traits.h"

#include "detail/qdata_deserializer.h"
#include "detail/qdata_serializer.h"
#include "detail/byte_buffer.h"

#include <cstddef>
#include <string>
#include <type_traits>
#include <vector>

namespace qdata {

template <class T>
inline void save(const std::string& file,
                 const T& object,
                 const int compress_level = 3,
                 const bool shuffle = true,
                 const int nthreads = 1,
                 const std::size_t max_depth = detail::default_qdata_max_nesting_depth) {
    detail::save_erased(
        file,
        std::addressof(object),
        &detail::write_erased<std::decay_t<T>>,
        compress_level,
        shuffle,
        nthreads,
        max_depth
    );
}

template <class Buffer = std::vector<std::byte>, class T>
inline Buffer serialize(const T& object,
                        const int compress_level = 3,
                        const bool shuffle = true,
                        const int nthreads = 1,
                        const std::size_t max_depth = detail::default_qdata_max_nesting_depth) {
    detail::validate_output_buffer<Buffer>();
    return detail::serialize_erased<Buffer>(
        std::addressof(object),
        &detail::write_erased<std::decay_t<T>>,
        compress_level,
        shuffle,
        nthreads,
        max_depth
    );
}

inline object read(const std::string& file,
                   const bool validate_checksum = false,
                   const int nthreads = 1,
                   const std::size_t max_depth = detail::default_qdata_max_nesting_depth) {
    return detail::read_file_impl(file, validate_checksum, nthreads, max_depth);
}

inline object deserialize(const void* data,
                          const std::size_t size,
                          const bool validate_checksum = false,
                          const int nthreads = 1,
                          const std::size_t max_depth = detail::default_qdata_max_nesting_depth) {
    return detail::deserialize_impl(data, size, validate_checksum, nthreads, max_depth);
}

template <class Buffer,
          std::enable_if_t<detail::is_byte_input_buffer<Buffer>::value, int> = 0>
inline object deserialize(const Buffer& data,
                          const bool validate_checksum = false,
                          const int nthreads = 1,
                          const std::size_t max_depth = detail::default_qdata_max_nesting_depth) {
    return detail::deserialize_impl(
        detail::buffer_data(data),
        detail::buffer_size_bytes(data),
        validate_checksum,
        nthreads,
        max_depth
    );
}

} // namespace qdata

#endif
