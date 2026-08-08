#ifndef QDATA_FORMAT_DETAIL_READ_COMMON_H
#define QDATA_FORMAT_DETAIL_READ_COMMON_H

#include "constants.h"
#include "../../io/io_common.h"

#include <cstdint>
#include <cstring>

namespace qdata {
namespace detail {

inline constexpr std::uint32_t max_qdata_string_header_bytes = 5;
inline constexpr std::uint32_t max_qdata_object_header_bytes = 14;

static_assert(BLOCK_RESERVE >= max_qdata_string_header_bytes,
              "qdata string headers must fit within the reserved contiguous block tail");
static_assert(BLOCK_RESERVE >= max_qdata_object_header_bytes,
              "qdata object headers must fit within the reserved contiguous block tail");

template <class Reader>
inline void decode_object_header(Reader& reader,
                                 const std::uint8_t header_byte,
                                 qstype& type,
                                 std::uint64_t& len) {
    const std::uint8_t header_byte_5 = header_byte & bitmask_type_5;
    if(header_byte_5 == 0) {
        switch(header_byte) {
            case nil_header:
                type = qstype::NIL;
                return;
            case logical_header_8:
                type = qstype::LOGICAL;
                len = reader.template get_pod_contiguous<std::uint8_t>();
                return;
            case logical_header_16:
                type = qstype::LOGICAL;
                len = reader.template get_pod_contiguous<std::uint16_t>();
                return;
            case logical_header_32:
                type = qstype::LOGICAL;
                len = reader.template get_pod_contiguous<std::uint32_t>();
                return;
            case logical_header_64:
                type = qstype::LOGICAL;
                len = reader.template get_pod_contiguous<std::uint64_t>();
                return;
            case integer_header_8:
                type = qstype::INTEGER;
                len = reader.template get_pod_contiguous<std::uint8_t>();
                return;
            case integer_header_16:
                type = qstype::INTEGER;
                len = reader.template get_pod_contiguous<std::uint16_t>();
                return;
            case integer_header_32:
                type = qstype::INTEGER;
                len = reader.template get_pod_contiguous<std::uint32_t>();
                return;
            case integer_header_64:
                type = qstype::INTEGER;
                len = reader.template get_pod_contiguous<std::uint64_t>();
                return;
            case numeric_header_8:
                type = qstype::REAL;
                len = reader.template get_pod_contiguous<std::uint8_t>();
                return;
            case numeric_header_16:
                type = qstype::REAL;
                len = reader.template get_pod_contiguous<std::uint16_t>();
                return;
            case numeric_header_32:
                type = qstype::REAL;
                len = reader.template get_pod_contiguous<std::uint32_t>();
                return;
            case numeric_header_64:
                type = qstype::REAL;
                len = reader.template get_pod_contiguous<std::uint64_t>();
                return;
            case complex_header_32:
                type = qstype::COMPLEX;
                len = reader.template get_pod_contiguous<std::uint32_t>();
                return;
            case complex_header_64:
                type = qstype::COMPLEX;
                len = reader.template get_pod_contiguous<std::uint64_t>();
                return;
            case character_header_8:
                type = qstype::CHARACTER;
                len = reader.template get_pod_contiguous<std::uint8_t>();
                return;
            case character_header_16:
                type = qstype::CHARACTER;
                len = reader.template get_pod_contiguous<std::uint16_t>();
                return;
            case character_header_32:
                type = qstype::CHARACTER;
                len = reader.template get_pod_contiguous<std::uint32_t>();
                return;
            case character_header_64:
                type = qstype::CHARACTER;
                len = reader.template get_pod_contiguous<std::uint64_t>();
                return;
            case list_header_8:
                type = qstype::LIST;
                len = reader.template get_pod_contiguous<std::uint8_t>();
                return;
            case list_header_16:
                type = qstype::LIST;
                len = reader.template get_pod_contiguous<std::uint16_t>();
                return;
            case list_header_32:
                type = qstype::LIST;
                len = reader.template get_pod_contiguous<std::uint32_t>();
                return;
            case list_header_64:
                type = qstype::LIST;
                len = reader.template get_pod_contiguous<std::uint64_t>();
                return;
            case raw_header_32:
                type = qstype::RAW;
                len = reader.template get_pod_contiguous<std::uint32_t>();
                return;
            case raw_header_64:
                type = qstype::RAW;
                len = reader.template get_pod_contiguous<std::uint64_t>();
                return;
            case attribute_header_8:
                type = qstype::ATTRIBUTE;
                len = reader.template get_pod_contiguous<std::uint8_t>();
                return;
            case attribute_header_32:
                type = qstype::ATTRIBUTE;
                len = reader.template get_pod_contiguous<std::uint32_t>();
                return;
            default:
                reader.cleanup_and_throw("Unknown qdata header type");
        }
    } else {
        len = header_byte & bitmask_length_5;
        switch(header_byte_5) {
            case logical_header_5:
                type = qstype::LOGICAL;
                return;
            case integer_header_5:
                type = qstype::INTEGER;
                return;
            case numeric_header_5:
                type = qstype::REAL;
                return;
            case character_header_5:
                type = qstype::CHARACTER;
                return;
            case list_header_5:
                type = qstype::LIST;
                return;
            case attribute_header_5:
                type = qstype::ATTRIBUTE;
                return;
            default:
                reader.cleanup_and_throw("Unknown qdata header type");
        }
    }
}

template <class Reader>
inline void read_object_header(Reader& reader,
                               qstype& type,
                               std::uint64_t& object_length,
                               std::uint32_t& attr_length) {
    attr_length = 0;
    auto header_byte = reader.template get_pod<std::uint8_t>();
    decode_object_header(reader, header_byte, type, object_length);
    if(type == qstype::ATTRIBUTE) {
        attr_length = static_cast<std::uint32_t>(object_length);
        header_byte = reader.template get_pod_contiguous<std::uint8_t>();
        decode_object_header(reader, header_byte, type, object_length);
        if(type == qstype::ATTRIBUTE) {
            reader.cleanup_and_throw("Malformed qdata header sequence");
        }
    }
}

template <class Reader>
inline void read_string_header(Reader& reader, std::uint32_t& string_len) {
    string_len = reader.template get_pod<std::uint8_t>();
    switch(string_len) {
        case string_header_16:
            string_len = reader.template get_pod_contiguous<std::uint16_t>();
            break;
        case string_header_32:
            string_len = reader.template get_pod_contiguous<std::uint32_t>();
            break;
        case string_header_NA:
            string_len = NA_STRING_LENGTH;
            break;
        default:
            break;
    }
}

inline void read_string_header(const char* const header,
                               std::uint32_t& string_len,
                               std::uint32_t& header_size) {
    const auto first = static_cast<std::uint8_t>(header[0]);
    string_len = first;
    header_size = 1;
    switch(first) {
        case string_header_16: {
            std::uint16_t narrow = 0;
            std::memcpy(&narrow, header + 1, sizeof(narrow));
            string_len = narrow;
            header_size += static_cast<std::uint32_t>(sizeof(narrow));
            break;
        }
        case string_header_32:
            std::memcpy(&string_len, header + 1, sizeof(string_len));
            header_size += static_cast<std::uint32_t>(sizeof(string_len));
            break;
        case string_header_NA:
            string_len = NA_STRING_LENGTH;
            break;
        default:
            break;
    }
}

template <class Reader>
inline void read_string_header_record(Reader& reader,
                                      std::uint32_t& string_len,
                                      char* const header_bytes,
                                      std::uint32_t& header_size) {
    const auto first = reader.template get_pod<std::uint8_t>();
    header_bytes[0] = static_cast<char>(first);
    header_size = 1;
    string_len = first;
    switch(first) {
        case string_header_16: {
            const auto narrow = reader.template get_pod_contiguous<std::uint16_t>();
            std::memcpy(header_bytes + 1, &narrow, sizeof(narrow));
            string_len = narrow;
            header_size += static_cast<std::uint32_t>(sizeof(narrow));
            break;
        }
        case string_header_32: {
            const auto wide = reader.template get_pod_contiguous<std::uint32_t>();
            std::memcpy(header_bytes + 1, &wide, sizeof(wide));
            string_len = wide;
            header_size += static_cast<std::uint32_t>(sizeof(wide));
            break;
        }
        case string_header_NA:
            string_len = NA_STRING_LENGTH;
            break;
        default:
            break;
    }
}

} // namespace detail
} // namespace qdata

#endif
