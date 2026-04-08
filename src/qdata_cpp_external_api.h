#ifndef QDATA_CPP_EXTERNAL_API_H
#define QDATA_CPP_EXTERNAL_API_H

#include "qdata_format/qdata.h"

#include <R_ext/Rdynload.h>

#include <cstddef>
#include <string>

inline void qdata_cpp_save_erased(const std::string& file,
                                  const void* object_ptr,
                                  const qdata::detail::erased_write_fn write_fn,
                                  const int compress_level,
                                  const bool shuffle,
                                  const int nthreads,
                                  const std::size_t max_depth) {
    qdata::detail::save_erased(file, object_ptr, write_fn, compress_level, shuffle, nthreads, max_depth);
}

inline void qdata_cpp_serialize_erased(void* const buffer_ctx,
                                       const qdata::detail::output_buffer_ops buffer_ops,
                                       const void* object_ptr,
                                       const qdata::detail::erased_write_fn write_fn,
                                       const int compress_level,
                                       const bool shuffle,
                                       const int nthreads,
                                       const std::size_t max_depth) {
    qdata::detail::serialize_erased_impl(
        buffer_ctx,
        buffer_ops,
        object_ptr,
        write_fn,
        compress_level,
        shuffle,
        nthreads,
        max_depth
    );
}

inline qdata::object qdata_cpp_read(const std::string& file,
                                    const bool validate_checksum,
                                    const int nthreads,
                                    const std::size_t max_depth) {
    return qdata::read(file, validate_checksum, nthreads, max_depth);
}

inline qdata::object qdata_cpp_deserialize(const void* const data,
                                           const std::size_t size,
                                           const bool validate_checksum,
                                           const int nthreads,
                                           const std::size_t max_depth) {
    return qdata::deserialize(data, size, validate_checksum, nthreads, max_depth);
}

inline void register_qdata_cpp_external_callables() {
    R_RegisterCCallable("qs2", "qdata_cpp_save_erased", (DL_FUNC)&qdata_cpp_save_erased);
    R_RegisterCCallable("qs2", "qdata_cpp_serialize_erased", (DL_FUNC)&qdata_cpp_serialize_erased);
    R_RegisterCCallable("qs2", "qdata_cpp_read", (DL_FUNC)&qdata_cpp_read);
    R_RegisterCCallable("qs2", "qdata_cpp_deserialize", (DL_FUNC)&qdata_cpp_deserialize);
}

#endif
