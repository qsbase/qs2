#ifndef _QS_QS_QD_FILE_HEADERS_H_
#define _QS_QS_QD_FILE_HEADERS_H_


#include <array>
#include <cstring>
#include "u8_literal.h"

static constexpr uint8_t QS2_CURRENT_FORMAT_VER = 1_u8;
static constexpr uint8_t QDATA_CURRENT_FORMAT_VER = 1_u8;

static constexpr uint8_t ZSTD_COMPRESSION_FLAG = 1_u8;
static constexpr uint8_t BIG_ENDIAN_FLAG = 1_u8;
static constexpr uint8_t LITTLE_ENDIAN_FLAG = 2_u8;
static constexpr uint8_t NO_SHUFFLE_FLAG = 0_u8;
static constexpr uint8_t YES_SHUFFLE_FLAG = 1_u8;

static const std::array<uint8_t,4> QS2_MAGIC_BITS = {0x0B,0x0E,0x0A,0xC1};
static const std::array<uint8_t,4> QDATA_MAGIC_BITS = {0x0B,0x0E,0x0A,0xC2};
static const std::array<uint8_t,16> RESERVED_BITS = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}; // unused bits reserved for future use

// https://stackoverflow.com/a/1001373
inline bool is_big_endian() {
    union {
    uint32_t i;
    char c[4];
    } bint = {0x01020304};
    return bint.c[0] == 1;
}

// input raw constant pointers
inline bool checkMagicNumber(uint8_t const * const bits, uint8_t const * const magic_bits) {
    if(bits[0] != magic_bits[0]) return false;
    if(bits[1] != magic_bits[1]) return false;
    if(bits[2] != magic_bits[2]) return false;
    if(bits[3] != magic_bits[3]) return false;
    return true;
}

template <typename stream_writer>
inline void write_qs2_header(stream_writer & writer, const bool shuffle) {
    std::array<uint8_t, 24> bits;
    std::memcpy(bits.data(), QS2_MAGIC_BITS.data(), 4);
    bits[4] = QS2_CURRENT_FORMAT_VER;
    bits[5] = ZSTD_COMPRESSION_FLAG; // compress algorithm, currently zstd only
    bits[6] = is_big_endian() ? BIG_ENDIAN_FLAG : LITTLE_ENDIAN_FLAG;
    bits[7] = shuffle ? YES_SHUFFLE_FLAG : NO_SHUFFLE_FLAG;
    std::memcpy(bits.data() + 8, RESERVED_BITS.data(), RESERVED_BITS.size());
    writer.write(reinterpret_cast<char*>(bits.data()), bits.size());
}

template <typename stream_reader>
inline void read_qs2_header(stream_reader & reader, bool & shuffle) {
    std::array<uint8_t, 24> bits;
    reader.read(reinterpret_cast<char*>(bits.data()), bits.size());
    if(! checkMagicNumber(bits.data(), QS2_MAGIC_BITS.data())) {
        throw std::runtime_error("qs2 format not detected");
    }
    uint8_t format_ver = bits[4];
    if(format_ver > QS2_CURRENT_FORMAT_VER) {
        throw std::runtime_error("qs2 format may be newer; please update qs2 to latest version");
    }
    uint8_t compress_alg = bits[5];
    if(compress_alg != ZSTD_COMPRESSION_FLAG) {
        throw std::runtime_error("Unknown compression algorithm detected in qs2 format");
    }
    uint8_t file_endian = bits[6];
    uint8_t system_endian = is_big_endian() ? BIG_ENDIAN_FLAG : LITTLE_ENDIAN_FLAG;
    if(file_endian != system_endian) {
        throw std::runtime_error("File and system endian mismatch");
    }
    uint8_t shuffle_bit = bits[7];
    shuffle = shuffle_bit != NO_SHUFFLE_FLAG;
}

template <typename stream_writer>
inline void write_qdata_header(stream_writer & writer, const bool shuffle) {
    std::array<uint8_t, 24> bits;
    std::memcpy(bits.data(), QDATA_MAGIC_BITS.data(), 4);
    bits[4] = QDATA_CURRENT_FORMAT_VER;
    bits[5] = ZSTD_COMPRESSION_FLAG; // compress algorithm, currently zstd only
    bits[6] = is_big_endian() ? BIG_ENDIAN_FLAG : LITTLE_ENDIAN_FLAG;
    bits[7] = shuffle ? YES_SHUFFLE_FLAG : NO_SHUFFLE_FLAG;
    std::memcpy(bits.data() + 8, RESERVED_BITS.data(), RESERVED_BITS.size());
    writer.write(reinterpret_cast<char*>(bits.data()), bits.size());
}

template <typename stream_reader>
inline void read_qdata_header(stream_reader & reader, bool & shuffle) {
    std::array<uint8_t, 24> bits;
    reader.read(reinterpret_cast<char*>(bits.data()), bits.size());
    if(! checkMagicNumber(bits.data(), QDATA_MAGIC_BITS.data())) {
        throw std::runtime_error("qs2 format not detected");
    }
    uint8_t format_ver = bits[4];
    if(format_ver > QS2_CURRENT_FORMAT_VER) {
        throw std::runtime_error("qs2 format may be newer; please update qs2 to latest version");
    }
    uint8_t compress_alg = bits[5];
    if(compress_alg != ZSTD_COMPRESSION_FLAG) {
        throw std::runtime_error("Unknown compression algorithm detected in qs2 format");
    }
    uint8_t file_endian = bits[6];
    uint8_t system_endian = is_big_endian() ? BIG_ENDIAN_FLAG : LITTLE_ENDIAN_FLAG;
    if(file_endian != system_endian) {
        throw std::runtime_error("File and system endian mismatch");
    }
    uint8_t shuffle_bit = bits[7];
    shuffle = shuffle_bit != NO_SHUFFLE_FLAG;
}


#endif

