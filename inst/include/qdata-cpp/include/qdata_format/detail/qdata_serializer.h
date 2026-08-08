#ifndef QDATA_FORMAT_DETAIL_QDATA_SERIALIZER_H
#define QDATA_FORMAT_DETAIL_QDATA_SERIALIZER_H

#include "../write_traits.h"
#include "file_headers.h"
#include "memory_stream.h"
#include "r_compat_limits.h"

#include "../../io/block_module.h"
#include "../../io/filestream_module.h"
#include "../../io/zstd_module.h"

#ifdef QIO_HAS_TBB
#include <tbb/global_control.h>
#include "../../io/multithreaded_block_module.h"
#endif

#include <complex>
#include <cstring>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace qdata {
namespace detail {

inline std::size_t checked_serialized_size(const std::uint64_t value, const char* const what) {
    if(value > static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
        throw std::runtime_error(std::string(what) + " exceeds this platform's size_t range");
    }
    return static_cast<std::size_t>(value);
}

inline std::uint32_t checked_attr_count(const std::size_t value) {
    if(value > static_cast<std::size_t>(max_r_compatible_attr_count)) {
        throw std::runtime_error("attribute count exceeds qdata's R-compatible INT_MAX limit");
    }
    return static_cast<std::uint32_t>(value);
}

inline std::uint32_t checked_string_length(const std::size_t value) {
    if(value > static_cast<std::size_t>(max_r_compatible_string_length)) {
        throw std::runtime_error("string length exceeds qdata's R-compatible string length limit");
    }
    return static_cast<std::uint32_t>(value);
}

inline std::uint64_t checked_object_length(const std::size_t value, const char* const what) {
    if(static_cast<std::uint64_t>(value) > max_r_compatible_vector_length) {
        throw std::runtime_error(std::string(what) + " exceeds qdata's R-compatible vector length limit");
    }
    return static_cast<std::uint64_t>(value);
}

inline int normalized_write_nthreads(const int value) {
    return value > 1 ? value : 1;
}

inline void validate_write_arguments(const int compress_level) {
    if(compress_level > ZSTD_maxCLevel() || compress_level < ZSTD_minCLevel()) {
        throw std::runtime_error(
            "compress_level must be between " +
            std::to_string(ZSTD_minCLevel()) +
            " and " +
            std::to_string(ZSTD_maxCLevel())
        );
    }
}

template <class BlockWriter>
class qdata_stream_writer final : public serializer {
public:
    explicit qdata_stream_writer(BlockWriter& writer, const std::size_t max_depth = default_qdata_max_nesting_depth) :
    serializer(max_depth),
    writer_(writer),
    string_payloads_(),
    complex_payloads_(),
    real_payloads_(),
    integer_payloads_(),
    raw_payloads_(),
    flushing_payloads_(false) {}

    void flush_payloads() {
        flushing_payloads_ = true;
        flush_payload_group(string_payloads_);
        flush_payload_group(complex_payloads_);
        flush_payload_group(real_payloads_);
        flush_payload_group(integer_payloads_);
        flush_payload_group(raw_payloads_);
        flushing_payloads_ = false;
    }

    void write_nil() override {
        writer_.push_pod(nil_header);
    }

    void begin_logical_vector(std::size_t size, std::size_t attr_count) override {
        write_header_lglsxp(checked_object_length(size, "logical vector length"), checked_attr_count(attr_count));
    }

    void begin_integer_vector(std::size_t size, std::size_t attr_count) override {
        write_header_intsxp(checked_object_length(size, "integer vector length"), checked_attr_count(attr_count));
    }

    void begin_real_vector(std::size_t size, std::size_t attr_count) override {
        write_header_realsxp(checked_object_length(size, "real vector length"), checked_attr_count(attr_count));
    }

    void begin_complex_vector(std::size_t size, std::size_t attr_count) override {
        write_header_cplxsxp(checked_object_length(size, "complex vector length"), checked_attr_count(attr_count));
    }

    void begin_string_vector(std::size_t size, std::size_t attr_count) override {
        write_header_strsxp(checked_object_length(size, "string vector length"), checked_attr_count(attr_count));
    }

    void begin_raw_vector(std::size_t size, std::size_t attr_count) override {
        write_header_rawsxp(checked_object_length(size, "raw vector length"), checked_attr_count(attr_count));
    }

    void begin_list_vector(std::size_t size, std::size_t attr_count) override {
        write_header_vecsxp(checked_object_length(size, "list length"), checked_attr_count(attr_count));
    }

    void write_logical_data(const std::int32_t* data, std::size_t size) override {
        require_payload_flush("write_logical_data");
        if(size > 0) {
            writer_.push_data(reinterpret_cast<const char*>(data), size * sizeof(std::int32_t));
        }
    }

    void write_integer_data(const std::int32_t* data, std::size_t size) override {
        require_payload_flush("write_integer_data");
        if(size > 0) {
            writer_.push_data(reinterpret_cast<const char*>(data), size * sizeof(std::int32_t));
        }
    }

    void write_real_data(const double* data, std::size_t size) override {
        require_payload_flush("write_real_data");
        if(size > 0) {
            writer_.push_data(reinterpret_cast<const char*>(data), size * sizeof(double));
        }
    }

    void write_complex_data(const std::complex<double>* data, std::size_t size) override {
        require_payload_flush("write_complex_data");
        if(size > 0) {
            writer_.push_data(reinterpret_cast<const char*>(data), size * sizeof(std::complex<double>));
        }
    }

    void write_raw_data(const std::byte* data, std::size_t size) override {
        require_payload_flush("write_raw_data");
        if(size > 0) {
            writer_.push_data(reinterpret_cast<const char*>(data), size * sizeof(std::byte));
        }
    }

    void write_logical_value(std::int32_t value) override {
        require_payload_flush("write_logical_value");
        writer_.push_data(reinterpret_cast<const char*>(std::addressof(value)), sizeof(value));
    }

    void write_integer_value(std::int32_t value) override {
        require_payload_flush("write_integer_value");
        writer_.push_data(reinterpret_cast<const char*>(std::addressof(value)), sizeof(value));
    }

    void write_real_value(double value) override {
        require_payload_flush("write_real_value");
        writer_.push_data(reinterpret_cast<const char*>(std::addressof(value)), sizeof(value));
    }

    void write_complex_value(const std::complex<double>& value) override {
        require_payload_flush("write_complex_value");
        writer_.push_data(reinterpret_cast<const char*>(std::addressof(value)), sizeof(value));
    }

    void write_string_value(std::string_view value, bool is_na) override {
        require_payload_flush("write_string_value");
        if(is_na) {
            writer_.push_pod(string_header_NA);
        } else {
            write_string_header(checked_string_length(value.size()));
            if(!value.empty()) {
                writer_.push_data(value.data(), value.size());
            }
        }
    }

    void write_raw_value(std::byte value) override {
        require_payload_flush("write_raw_value");
        writer_.push_data(reinterpret_cast<const char*>(std::addressof(value)), sizeof(value));
    }

    void write_attribute_name(std::string_view name) override {
        write_string_header(checked_string_length(name.size()));
        if(!name.empty()) {
            writer_.push_data(name.data(), name.size());
        }
    }

    void defer_integer_payload(const void* object_ptr, const erased_write_fn emit_fn) override {
        integer_payloads_.push_back({object_ptr, emit_fn});
    }

    void defer_real_payload(const void* object_ptr, const erased_write_fn emit_fn) override {
        real_payloads_.push_back({object_ptr, emit_fn});
    }

    void defer_complex_payload(const void* object_ptr, const erased_write_fn emit_fn) override {
        complex_payloads_.push_back({object_ptr, emit_fn});
    }

    void defer_string_payload(const void* object_ptr, const erased_write_fn emit_fn) override {
        string_payloads_.push_back({object_ptr, emit_fn});
    }

    void defer_raw_payload(const void* object_ptr, const erased_write_fn emit_fn) override {
        raw_payloads_.push_back({object_ptr, emit_fn});
    }

private:
    struct deferred_payload {
        const void* object_ptr;
        erased_write_fn emit_fn;
    };

    BlockWriter& writer_;
    std::vector<deferred_payload> string_payloads_;
    std::vector<deferred_payload> complex_payloads_;
    std::vector<deferred_payload> real_payloads_;
    std::vector<deferred_payload> integer_payloads_;
    std::vector<deferred_payload> raw_payloads_;
    bool flushing_payloads_;

    void flush_payload_group(const std::vector<deferred_payload>& payloads) {
        for(const auto& payload : payloads) {
            payload.emit_fn(*this, payload.object_ptr);
        }
    }

    void require_payload_flush(const char* const what) {
        if(!flushing_payloads_) {
            throw std::logic_error(std::string(what) + " called before deferred payload flush");
        }
    }

    void write_attr_header(const std::uint32_t length) {
        if(length < MAX_5_BIT_LENGTH) {
            writer_.push_pod(static_cast<std::uint8_t>(attribute_header_5 | static_cast<std::uint8_t>(length)));
        } else if(length < MAX_8_BIT_LENGTH) {
            writer_.push_pod(attribute_header_8);
            writer_.push_pod_contiguous(static_cast<std::uint8_t>(length));
        } else {
            writer_.push_pod(attribute_header_32);
            writer_.push_pod_contiguous(length);
        }
    }

    void write_header_lglsxp(const std::uint64_t length, const std::uint32_t attr_length) {
        const bool has_attrs = attr_length > 0;
        if(has_attrs) {
            write_attr_header(attr_length);
        }
        if(length < MAX_5_BIT_LENGTH) {
            writer_.push_pod(static_cast<std::uint8_t>(logical_header_5 | static_cast<std::uint8_t>(length)), has_attrs);
        } else if(length < MAX_8_BIT_LENGTH) {
            writer_.push_pod(logical_header_8, has_attrs);
            writer_.push_pod_contiguous(static_cast<std::uint8_t>(length));
        } else if(length < MAX_16_BIT_LENGTH) {
            writer_.push_pod(logical_header_16, has_attrs);
            writer_.push_pod_contiguous(static_cast<std::uint16_t>(length));
        } else if(length < MAX_32_BIT_LENGTH) {
            writer_.push_pod(logical_header_32, has_attrs);
            writer_.push_pod_contiguous(static_cast<std::uint32_t>(length));
        } else {
            writer_.push_pod(logical_header_64, has_attrs);
            writer_.push_pod_contiguous(length);
        }
    }

    void write_header_intsxp(const std::uint64_t length, const std::uint32_t attr_length) {
        const bool has_attrs = attr_length > 0;
        if(has_attrs) {
            write_attr_header(attr_length);
        }
        if(length < MAX_5_BIT_LENGTH) {
            writer_.push_pod(static_cast<std::uint8_t>(integer_header_5 | static_cast<std::uint8_t>(length)), has_attrs);
        } else if(length < MAX_8_BIT_LENGTH) {
            writer_.push_pod(integer_header_8, has_attrs);
            writer_.push_pod_contiguous(static_cast<std::uint8_t>(length));
        } else if(length < MAX_16_BIT_LENGTH) {
            writer_.push_pod(integer_header_16, has_attrs);
            writer_.push_pod_contiguous(static_cast<std::uint16_t>(length));
        } else if(length < MAX_32_BIT_LENGTH) {
            writer_.push_pod(integer_header_32, has_attrs);
            writer_.push_pod_contiguous(static_cast<std::uint32_t>(length));
        } else {
            writer_.push_pod(integer_header_64, has_attrs);
            writer_.push_pod_contiguous(length);
        }
    }

    void write_header_realsxp(const std::uint64_t length, const std::uint32_t attr_length) {
        const bool has_attrs = attr_length > 0;
        if(has_attrs) {
            write_attr_header(attr_length);
        }
        if(length < MAX_5_BIT_LENGTH) {
            writer_.push_pod(static_cast<std::uint8_t>(numeric_header_5 | static_cast<std::uint8_t>(length)), has_attrs);
        } else if(length < MAX_8_BIT_LENGTH) {
            writer_.push_pod(numeric_header_8, has_attrs);
            writer_.push_pod_contiguous(static_cast<std::uint8_t>(length));
        } else if(length < MAX_16_BIT_LENGTH) {
            writer_.push_pod(numeric_header_16, has_attrs);
            writer_.push_pod_contiguous(static_cast<std::uint16_t>(length));
        } else if(length < MAX_32_BIT_LENGTH) {
            writer_.push_pod(numeric_header_32, has_attrs);
            writer_.push_pod_contiguous(static_cast<std::uint32_t>(length));
        } else {
            writer_.push_pod(numeric_header_64, has_attrs);
            writer_.push_pod_contiguous(length);
        }
    }

    void write_header_cplxsxp(const std::uint64_t length, const std::uint32_t attr_length) {
        const bool has_attrs = attr_length > 0;
        if(has_attrs) {
            write_attr_header(attr_length);
        }
        if(length < MAX_32_BIT_LENGTH) {
            writer_.push_pod(complex_header_32, has_attrs);
            writer_.push_pod_contiguous(static_cast<std::uint32_t>(length));
        } else {
            writer_.push_pod(complex_header_64, has_attrs);
            writer_.push_pod_contiguous(length);
        }
    }

    void write_header_strsxp(const std::uint64_t length, const std::uint32_t attr_length) {
        const bool has_attrs = attr_length > 0;
        if(has_attrs) {
            write_attr_header(attr_length);
        }
        if(length < MAX_5_BIT_LENGTH) {
            writer_.push_pod(static_cast<std::uint8_t>(character_header_5 | static_cast<std::uint8_t>(length)), has_attrs);
        } else if(length < MAX_8_BIT_LENGTH) {
            writer_.push_pod(character_header_8, has_attrs);
            writer_.push_pod_contiguous(static_cast<std::uint8_t>(length));
        } else if(length < MAX_16_BIT_LENGTH) {
            writer_.push_pod(character_header_16, has_attrs);
            writer_.push_pod_contiguous(static_cast<std::uint16_t>(length));
        } else if(length < MAX_32_BIT_LENGTH) {
            writer_.push_pod(character_header_32, has_attrs);
            writer_.push_pod_contiguous(static_cast<std::uint32_t>(length));
        } else {
            writer_.push_pod(character_header_64, has_attrs);
            writer_.push_pod_contiguous(length);
        }
    }

    void write_header_vecsxp(const std::uint64_t length, const std::uint32_t attr_length) {
        const bool has_attrs = attr_length > 0;
        if(has_attrs) {
            write_attr_header(attr_length);
        }
        if(length < MAX_5_BIT_LENGTH) {
            writer_.push_pod(static_cast<std::uint8_t>(list_header_5 | static_cast<std::uint8_t>(length)), has_attrs);
        } else if(length < MAX_8_BIT_LENGTH) {
            writer_.push_pod(list_header_8, has_attrs);
            writer_.push_pod_contiguous(static_cast<std::uint8_t>(length));
        } else if(length < MAX_16_BIT_LENGTH) {
            writer_.push_pod(list_header_16, has_attrs);
            writer_.push_pod_contiguous(static_cast<std::uint16_t>(length));
        } else if(length < MAX_32_BIT_LENGTH) {
            writer_.push_pod(list_header_32, has_attrs);
            writer_.push_pod_contiguous(static_cast<std::uint32_t>(length));
        } else {
            writer_.push_pod(list_header_64, has_attrs);
            writer_.push_pod_contiguous(length);
        }
    }

    void write_header_rawsxp(const std::uint64_t length, const std::uint32_t attr_length) {
        const bool has_attrs = attr_length > 0;
        if(has_attrs) {
            write_attr_header(attr_length);
        }
        if(length < MAX_32_BIT_LENGTH) {
            writer_.push_pod(raw_header_32, has_attrs);
            writer_.push_pod_contiguous(static_cast<std::uint32_t>(length));
        } else {
            writer_.push_pod(raw_header_64, has_attrs);
            writer_.push_pod_contiguous(length);
        }
    }

    void write_string_header(const std::uint32_t length) {
        if(length < MAX_STRING_8_BIT_LENGTH) {
            writer_.push_pod(static_cast<std::uint8_t>(length));
        } else if(length < MAX_STRING_16_BIT_LENGTH) {
            writer_.push_pod(string_header_16);
            writer_.push_pod_contiguous(static_cast<std::uint16_t>(length));
        } else {
            writer_.push_pod(string_header_32);
            writer_.push_pod_contiguous(length);
        }
    }
};

template <class StreamWriter, class Compressor>
inline std::uint64_t write_single_thread(StreamWriter& stream,
                                         const int compress_level,
                                         const void* object_ptr,
                                         const erased_write_fn write_fn,
                                         const std::size_t max_depth) {
    BlockCompressWriter<StreamWriter, Compressor, xxHashEnv, StdErrorPolicy, true> block_writer(stream, compress_level);
    qdata_stream_writer<decltype(block_writer)> stream_writer(block_writer, max_depth);
    write_fn(stream_writer, object_ptr);
    stream_writer.flush_payloads();
    return block_writer.finish();
}

#ifdef QIO_HAS_TBB
template <class StreamWriter, class Compressor>
inline std::uint64_t write_multi_thread(StreamWriter& stream,
                                        const int compress_level,
                                        const int nthreads,
                                        const void* object_ptr,
                                        const erased_write_fn write_fn,
                                        const std::size_t max_depth) {
    tbb::global_control gc(tbb::global_control::parameter::max_allowed_parallelism, normalized_write_nthreads(nthreads));
    BlockCompressWriterMT<StreamWriter, Compressor, xxHashEnv, StdErrorPolicy, true> block_writer(stream, compress_level);
    qdata_stream_writer<decltype(block_writer)> stream_writer(block_writer, max_depth);
    write_fn(stream_writer, object_ptr);
    stream_writer.flush_payloads();
    return block_writer.finish();
}
#endif

template <class StreamWriter>
inline std::uint64_t write_qdata_object(StreamWriter& stream,
                                        const void* object_ptr,
                                        const erased_write_fn write_fn,
                                        const int compress_level,
                                        const bool shuffle,
                                        const int nthreads,
                                        const std::size_t max_depth) {
    validate_write_arguments(compress_level);
    if(shuffle) {
#ifdef QIO_HAS_TBB
        if(nthreads > 1) {
            return write_multi_thread<StreamWriter, ZstdShuffleCompressor>(
                stream,
                compress_level,
                nthreads,
                object_ptr,
                write_fn,
                max_depth
            );
        }
#endif
        return write_single_thread<StreamWriter, ZstdShuffleCompressor>(
            stream,
            compress_level,
            object_ptr,
            write_fn,
            max_depth
        );
    }

#ifdef QIO_HAS_TBB
    if(nthreads > 1) {
        return write_multi_thread<StreamWriter, ZstdCompressor>(
            stream,
            compress_level,
            nthreads,
            object_ptr,
            write_fn,
            max_depth
        );
    }
#endif
    return write_single_thread<StreamWriter, ZstdCompressor>(
        stream,
        compress_level,
        object_ptr,
        write_fn,
        max_depth
    );
}

inline void save_erased(const std::string& file,
                        const void* object_ptr,
                        const erased_write_fn write_fn,
                        const int compress_level,
                        const bool shuffle,
                        const int nthreads,
                        const std::size_t max_depth) {
    OfStreamWriter stream(file.c_str());
    if(!stream.isValid()) {
        throw std::runtime_error("failed to open file for writing: " + file);
    }
    write_qdata_header(stream, shuffle);
    const auto hash = write_qdata_object(stream, object_ptr, write_fn, compress_level, shuffle, nthreads, max_depth);
    write_qx_hash(stream, hash);
}

inline void serialize_erased_impl(void* const buffer_ctx,
                                  const output_buffer_ops buffer_ops,
                                  const void* object_ptr,
                                  const erased_write_fn write_fn,
                                  const int compress_level,
                                  const bool shuffle,
                                  const int nthreads,
                                  const std::size_t max_depth) {
    erased_memory_writer stream(buffer_ctx, buffer_ops);
    write_qdata_header(stream, shuffle);
    const auto hash = write_qdata_object(stream, object_ptr, write_fn, compress_level, shuffle, nthreads, max_depth);
    const auto end_position = stream.tellp();
    write_qx_hash(stream, hash);
    stream.seekp(end_position);
    buffer_ops.resize_fn(buffer_ctx, checked_serialized_size(end_position, "serialized qdata size"));
}

template <class Buffer>
inline Buffer serialize_erased(const void* object_ptr,
                               const erased_write_fn write_fn,
                               const int compress_level,
                               const bool shuffle,
                               const int nthreads,
                               const std::size_t max_depth) {
    Buffer output;
    serialize_erased_impl(
        static_cast<void*>(std::addressof(output)),
        make_output_buffer_ops<Buffer>(),
        object_ptr,
        write_fn,
        compress_level,
        shuffle,
        nthreads,
        max_depth
    );
    return output;
}

} // namespace detail
} // namespace qdata

#endif
