#ifndef QDATA_FORMAT_DETAIL_BYTE_BUFFER_H
#define QDATA_FORMAT_DETAIL_BYTE_BUFFER_H

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace qdata {
namespace detail {

using output_buffer_size_fn = std::size_t (*)(void*);
using output_buffer_resize_fn = void (*)(void*, std::size_t);
using output_buffer_data_fn = void* (*)(void*);

struct output_buffer_ops {
    output_buffer_size_fn size_fn;
    output_buffer_resize_fn resize_fn;
    output_buffer_data_fn data_fn;
};

template <class Buffer, class Enable = void>
struct is_byte_input_buffer : std::false_type {};

template <class Buffer>
struct is_byte_input_buffer<Buffer, std::void_t<
    typename Buffer::value_type,
    decltype(std::declval<const Buffer&>().data()),
    decltype(std::declval<const Buffer&>().size())
>> : std::bool_constant<
    sizeof(typename Buffer::value_type) == 1 &&
    std::is_trivially_copyable<typename Buffer::value_type>::value &&
    std::is_convertible<decltype(std::declval<const Buffer&>().data()),
                        const typename Buffer::value_type*>::value &&
    std::is_convertible<decltype(std::declval<const Buffer&>().size()), std::size_t>::value
> {};

template <class Buffer, class Enable = void>
struct is_byte_output_buffer : std::false_type {};

template <class Buffer>
struct is_byte_output_buffer<Buffer, std::void_t<
    typename Buffer::value_type,
    decltype(std::declval<Buffer&>().data()),
    decltype(std::declval<const Buffer&>().size()),
    decltype(std::declval<Buffer&>().resize(std::declval<std::size_t>()))
>> : std::bool_constant<
    is_byte_input_buffer<Buffer>::value &&
    std::is_default_constructible<Buffer>::value &&
    std::is_convertible<decltype(std::declval<Buffer&>().data()),
                        typename Buffer::value_type*>::value
> {};

template <class Buffer>
inline void validate_input_buffer() {
    static_assert(
        is_byte_input_buffer<Buffer>::value,
        "Buffer must expose a contiguous 1-byte trivially copyable value_type with data() and size()"
    );
}

template <class Buffer>
inline void validate_output_buffer() {
    static_assert(
        is_byte_output_buffer<Buffer>::value,
        "Buffer must expose a contiguous 1-byte trivially copyable value_type with data(), size(), resize(), and a default constructor"
    );
}

template <class Buffer>
inline std::size_t buffer_size_bytes(const Buffer& buffer) {
    validate_input_buffer<Buffer>();
    return static_cast<std::size_t>(buffer.size()) * sizeof(typename Buffer::value_type);
}

template <class Buffer>
inline const void* buffer_data(const Buffer& buffer) {
    validate_input_buffer<Buffer>();
    return static_cast<const void*>(buffer.data());
}

template <class Buffer>
inline std::size_t erased_output_buffer_size(void* const buffer_ptr) {
    validate_output_buffer<Buffer>();
    return static_cast<Buffer*>(buffer_ptr)->size();
}

template <class Buffer>
inline void erased_output_buffer_resize(void* const buffer_ptr, const std::size_t size) {
    validate_output_buffer<Buffer>();
    static_cast<Buffer*>(buffer_ptr)->resize(size);
}

template <class Buffer>
inline void* erased_output_buffer_data(void* const buffer_ptr) {
    validate_output_buffer<Buffer>();
    return static_cast<void*>(static_cast<Buffer*>(buffer_ptr)->data());
}

template <class Buffer>
inline output_buffer_ops make_output_buffer_ops() {
    validate_output_buffer<Buffer>();
    return output_buffer_ops{
        &erased_output_buffer_size<Buffer>,
        &erased_output_buffer_resize<Buffer>,
        &erased_output_buffer_data<Buffer>
    };
}

inline std::size_t checked_required_capacity(const std::size_t position,
                                             const std::uint64_t additional_size) {
    if(additional_size > static_cast<std::uint64_t>(std::size_t(-1)) - position) {
        throw std::length_error("serialized qdata exceeds addressable buffer size");
    }
    return position + static_cast<std::size_t>(additional_size);
}

} // namespace detail
} // namespace qdata

#endif
