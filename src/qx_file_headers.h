#ifndef _QS2_QX_FILE_HEADERS_H_
#define _QS2_QX_FILE_HEADERS_H_


#include <array>
#include <cstring>

static constexpr uint8_t QS2_CURRENT_FORMAT_VER = 1_u8;
static constexpr uint8_t QDATA_CURRENT_FORMAT_VER = 1_u8;

static constexpr uint8_t ZSTD_COMPRESSION_FLAG = 1_u8;
static constexpr uint8_t BIG_ENDIAN_FLAG = 1_u8;
static constexpr uint8_t LITTLE_ENDIAN_FLAG = 2_u8;
static constexpr uint8_t NO_SHUFFLE_FLAG = 0_u8;
static constexpr uint8_t YES_SHUFFLE_FLAG = 1_u8;

static constexpr uint64_t HEADER_HASH_POSITION = 16;

static const std::array<uint8_t,4> QS2_MAGIC_BITS = {0x0B,0x0E,0x0A,0xC1};
static const std::array<uint8_t,4> QDATA_MAGIC_BITS = {0x0B,0x0E,0x0A,0xCD};
static const std::array<uint8_t,4> QS_LEGACY_MAGIC_BITS = {0x0B,0x0E,0x0A,0x0C};
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
    std::array<uint8_t, 24> bits = {};
    std::memcpy(bits.data(), QS2_MAGIC_BITS.data(), 4);
    bits[4] = QS2_CURRENT_FORMAT_VER;
    bits[5] = ZSTD_COMPRESSION_FLAG; // compress algorithm, currently zstd only
    bits[6] = is_big_endian() ? BIG_ENDIAN_FLAG : LITTLE_ENDIAN_FLAG;
    bits[7] = shuffle ? YES_SHUFFLE_FLAG : NO_SHUFFLE_FLAG;
    std::memcpy(bits.data() + 8, RESERVED_BITS.data(), RESERVED_BITS.size());
    writer.write(reinterpret_cast<char*>(bits.data()), bits.size());
}

template <typename stream_reader>
inline void read_qs2_header(stream_reader & reader, bool & shuffle, uint64_t & hash) {
    std::array<uint8_t, 24> bits = {};
    reader.read(reinterpret_cast<char*>(bits.data()), bits.size());
    if(! checkMagicNumber(bits.data(), QS2_MAGIC_BITS.data())) {
        // check for qdata or qs-legacy
        if(checkMagicNumber(bits.data(), QDATA_MAGIC_BITS.data())) {
            throw std::runtime_error("qdata format detected, use qs2::qd_read");
        } else if(checkMagicNumber(bits.data(), QS_LEGACY_MAGIC_BITS.data())) {
            throw std::runtime_error("qs-legacy format detected, use qs::qread");
        }
        throw std::runtime_error("Unknown file format detected");
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

    // stored hash
    std::memcpy(&hash, bits.data() + HEADER_HASH_POSITION, 8);
}

template <typename stream_writer>
inline void write_qdata_header(stream_writer & writer, const bool shuffle) {
    std::array<uint8_t, 24> bits = {};
    std::memcpy(bits.data(), QDATA_MAGIC_BITS.data(), 4);
    bits[4] = QDATA_CURRENT_FORMAT_VER;
    bits[5] = ZSTD_COMPRESSION_FLAG; // compress algorithm, currently zstd only
    bits[6] = is_big_endian() ? BIG_ENDIAN_FLAG : LITTLE_ENDIAN_FLAG;
    bits[7] = shuffle ? YES_SHUFFLE_FLAG : NO_SHUFFLE_FLAG;
    std::memcpy(bits.data() + 8, RESERVED_BITS.data(), RESERVED_BITS.size());
    writer.write(reinterpret_cast<char*>(bits.data()), bits.size());
}

// only valid for stream_writer than can seek
// this should be the last operation since the output position is moved
template <typename stream_writer>
inline void write_qx_hash(stream_writer & writer, const uint64_t value) {
    if(value != 0) {
        writer.seekp(HEADER_HASH_POSITION);
        writer.writeInteger(value);
    }
}


template <typename stream_reader>
inline void read_qdata_header(stream_reader & reader, bool & shuffle, uint64_t & hash) {
    std::array<uint8_t, 24> bits = {};
    reader.read(reinterpret_cast<char*>(bits.data()), bits.size());
    if(! checkMagicNumber(bits.data(), QDATA_MAGIC_BITS.data())) {
        // check for qs2 or qs-legacy
        if(checkMagicNumber(bits.data(), QS2_MAGIC_BITS.data())) {
            throw std::runtime_error("qs2 format detected, use qs2::qs_read");
        } else if(checkMagicNumber(bits.data(), QS_LEGACY_MAGIC_BITS.data())) {
            throw std::runtime_error("qs-legacy format detected, use qs::qread");
        }
        throw std::runtime_error("Unknown file format detected");
    }
    uint8_t format_ver = bits[4];
    if(format_ver > QDATA_CURRENT_FORMAT_VER) {
        throw std::runtime_error("qdata format may be newer; please update qdata to latest version");
    }
    uint8_t compress_alg = bits[5];
    if(compress_alg != ZSTD_COMPRESSION_FLAG) {
        throw std::runtime_error("Unknown compression algorithm detected in qdata format");
    }
    uint8_t file_endian = bits[6];
    uint8_t system_endian = is_big_endian() ? BIG_ENDIAN_FLAG : LITTLE_ENDIAN_FLAG;
    if(file_endian != system_endian) {
        throw std::runtime_error("File and system endian mismatch");
    }
    uint8_t shuffle_bit = bits[7];
    shuffle = shuffle_bit != NO_SHUFFLE_FLAG;

    // stored hash
    std::memcpy(&hash, bits.data() + HEADER_HASH_POSITION, 8);
}


// the following code is for qs_dump, in order to output information of a file back to R
struct qxHeaderInfo {
    std::string format;
    int format_version;
    std::string compression;
    int shuffle;
    std::string file_endian;
    std::string stored_hash;
};

template <typename stream_reader>
inline qxHeaderInfo read_qx_header(stream_reader & reader) {
    std::array<uint8_t, 24> bits = {};
    qxHeaderInfo output;
    reader.read(reinterpret_cast<char*>(bits.data()), bits.size());
    if(checkMagicNumber(bits.data(), QS2_MAGIC_BITS.data()) ) {
        output.format = "qs2";
    } else if(checkMagicNumber(bits.data(), QDATA_MAGIC_BITS.data())) {
        output.format = "qdata";
    } else {
        output.format = "unknown";
        output.format_version = -1;
        output.compression = "unknown";
        output.shuffle = -1;
        output.file_endian = "unknown";
        return output;
    }
    output.format_version = bits[4];
    uint8_t compression_bit = bits[5];
    if(compression_bit == ZSTD_COMPRESSION_FLAG) {
        output.compression = "zstd";
    } else {
        output.compression = "unknown";
    }
    uint8_t file_endian_bit = bits[6];
    if(file_endian_bit == BIG_ENDIAN_FLAG) {
        output.file_endian = "big";
    } else if(file_endian_bit == LITTLE_ENDIAN_FLAG) {
        output.file_endian = "little";
    } else {
        output.file_endian = "unknown";
    }
    output.shuffle = bits[7];

    // stored hash
    uint64_t stored_hash;
    std::memcpy(&stored_hash, bits.data() + HEADER_HASH_POSITION, 8);
    output.stored_hash = std::to_string(stored_hash);
    return output;
}


// get file hash from remaining bytes from the current position
// reset seek position to current position
template <class stream_reader>
uint64_t read_qx_hash(stream_reader & reader) {
    auto current_position = reader.tellg();
    xxHashEnv env;
    std::unique_ptr<char[]> zblock(MAKE_UNIQUE_BLOCK(MAX_ZBLOCKSIZE));
    uint64_t bytes_read = 0;
    while( (bytes_read = reader.read(zblock.get(), MAX_ZBLOCKSIZE)) ) {
        env.update(zblock.get(), bytes_read);
    }
    reader.seekg(current_position);
    return env.digest();
}

#endif

