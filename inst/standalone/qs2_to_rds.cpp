// A minimal standalone Rcpp converter for qs2 -> RDS without package dependency
// Requires linkage to zstd
//
// This file lives in inst/standalone so it can be sourced or reused
// without pulling in the rest of the qs2 sources.

#include <Rcpp.h>
#include <array>
#include <cstdint>
#include <fstream>
#include <string>
#include <vector>
#include <zstd.h>

using Rcpp::stop;

namespace {

constexpr uint32_t HEADER_SIZE = 24;
constexpr uint32_t MAX_BLOCKSIZE = 1048576u;            // 1 MiB
constexpr uint32_t BLOCK_METADATA_MASK = 0x80000000u;   // top bit carries shuffle metadata
constexpr uint32_t SHUFFLE_MASK = BLOCK_METADATA_MASK;
constexpr uint64_t SHUFFLE_ELEMSIZE = 8;                // matches qs2 shuffle element size

// zstd bound for a single block
const size_t MAX_ZBLOCKSIZE = ZSTD_compressBound(MAX_BLOCKSIZE);

const std::array<uint8_t, 4> QS2_MAGIC = {0x0B, 0x0E, 0x0A, 0xC1};

bool is_big_endian() {
    union {
        uint32_t i;
        char c[4];
    } bint = {0x01020304};
    return bint.c[0] == 1;
}

bool check_magic(const uint8_t* data, const std::array<uint8_t, 4>& magic) {
    return data[0] == magic[0] && data[1] == magic[1] && data[2] == magic[2] && data[3] == magic[3];
}

// Non-SIMD BLOSC unshuffle (adapted from Blosc unshuffle_routines.h)
void blosc_unshuffle_basic(const uint8_t* src, uint8_t* dest, uint64_t blocksize, uint64_t bytesoftype) {
    if (bytesoftype == 0) return;
    const uint64_t neblock_quot = blocksize / bytesoftype;
    for (uint64_t i = 0; i < neblock_quot; ++i) {
        for (uint64_t j = 0; j < bytesoftype; ++j) {
            dest[i * bytesoftype + j] = src[j * neblock_quot + i];
        }
    }
}

// Parse the qs2 header and return whether shuffle was enabled.
bool read_qs2_header(std::istream& in) {
    std::array<uint8_t, HEADER_SIZE> header = {};
    in.read(reinterpret_cast<char*>(header.data()), header.size());
    if (in.gcount() != static_cast<std::streamsize>(header.size())) {
        stop("Unable to read qs2 header");
    }

    if (!check_magic(header.data(), QS2_MAGIC)) {
        stop("qs2 magic bytes not found");
    }

    const uint8_t format_version = header[4];
    if (format_version > 1) {
        stop("qs2 format version is newer than this helper understands");
    }

    const uint8_t compression_flag = header[5];
    if (compression_flag != 1) {
        stop("Unsupported compression flag in qs2 header");
    }

    const uint8_t file_endian = header[6];
    const uint8_t system_endian = is_big_endian() ? 1 : 2;
    if (file_endian != system_endian) {
        stop("Endianness mismatch between file and host");
    }

    const uint8_t shuffle_flag = header[7];
    return shuffle_flag != 0;
}

void write_block(std::ofstream& out, const std::vector<char>& buffer, size_t len) {
    out.write(buffer.data(), static_cast<std::streamsize>(len));
    if (!out) {
        stop("Failed while writing to output file");
    }
}

}  // namespace

// [[Rcpp::export(rng=false)]]
void standalone_qs2_to_rds(const std::string& input_path, const std::string& output_path) {
    std::ifstream in(input_path, std::ios::binary);
    if (!in) {
        stop("Unable to open input file");
    }

    std::ofstream out(output_path, std::ios::binary);
    if (!out) {
        stop("Unable to open output file");
    }

    const bool shuffle_enabled = read_qs2_header(in);

    std::vector<char> zblock(MAX_ZBLOCKSIZE);
    std::vector<char> block(MAX_BLOCKSIZE);
    std::vector<char> unshuffled(MAX_BLOCKSIZE);

    std::unique_ptr<ZSTD_DCtx, decltype(&ZSTD_freeDCtx)> dctx(ZSTD_createDCtx(), &ZSTD_freeDCtx);
    if (!dctx) {
        stop("Failed to allocate zstd decompression context");
    }

    uint32_t zsize = 0;
    while (in.read(reinterpret_cast<char*>(&zsize), sizeof(uint32_t))) {
        const bool block_shuffled = shuffle_enabled && ((zsize & SHUFFLE_MASK) != 0);
        const uint32_t encoded_size = zsize & (~SHUFFLE_MASK);
        if (encoded_size == 0 || encoded_size > MAX_ZBLOCKSIZE) {
            stop("Invalid block size found in qs2 stream");
        }

        in.read(zblock.data(), encoded_size);
        if (in.gcount() != static_cast<std::streamsize>(encoded_size)) {
            stop("Unexpected end of file while reading block data");
        }

        const size_t decoded_size =
            ZSTD_decompressDCtx(dctx.get(), block.data(), block.size(), zblock.data(), encoded_size);
        if (ZSTD_isError(decoded_size)) {
            stop("zstd decompression failed");
        }
        if (decoded_size > block.size()) {
            stop("Decoded block exceeds expected size");
        }

        if (block_shuffled) {
            const uint64_t remainder = decoded_size % SHUFFLE_ELEMSIZE;
            const uint64_t shuf_length = decoded_size - remainder;
            blosc_unshuffle_basic(reinterpret_cast<uint8_t*>(block.data()),
                                  reinterpret_cast<uint8_t*>(unshuffled.data()),
                                  shuf_length,
                                  SHUFFLE_ELEMSIZE);
            std::memcpy(unshuffled.data() + shuf_length, block.data() + shuf_length, remainder);
            write_block(out, unshuffled, decoded_size);
        } else {
            write_block(out, block, decoded_size);
        }
    }

    if (!in.eof()) {
        stop("Failed while reading input stream");
    }
}
