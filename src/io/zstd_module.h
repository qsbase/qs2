#ifndef _QS2_ZSTD_MODULE_H
#define _QS2_ZSTD_MODULE_H

#include "io/io_common.h"

struct ZstdCompressor {
    ZSTD_CCtx * cctx;
    ZstdCompressor() : cctx(ZSTD_createCCtx()) {}
    ~ZstdCompressor() {
        ZSTD_freeCCtx(cctx);
    }
    uint64_t compress(char * const dst, const uint64_t dstCapacity,
                      const char * const src, const uint64_t srcSize,
                      int compress_level) {
        return ZSTD_compressCCtx(cctx, dst, dstCapacity, src, srcSize, compress_level);
    }
};

// to do: investigate whether 16/32 byte alignment improves shuffle performance
// likely not a big deal as shuffle is already really fast
// NOT necessary since _mm_loadu_si128 does not require alignment

struct ZstdShuffleCompressor {

    static constexpr int SHUFFLE_HEURISTIC_CLEVEL = -1; // compress with fast clevel to test whether shuffle is better
    static constexpr uint64_t SHUFFLE_HEURISTIC_BLOCKSIZE = 16384ULL; // 524288 / 8 / 4
    static constexpr float SHUFFLE_MIN_IMPROVEMENT_THRESHOLD = 1.07f; // shuffle must be at least 7% better to use

    ZSTD_CCtx * cctx;
    std::unique_ptr<char[]> shuffleblock;
    ZstdShuffleCompressor() : cctx(ZSTD_createCCtx()), shuffleblock(MAKE_UNIQUE_BLOCK(MAX_BLOCKSIZE)) {}
    ~ZstdShuffleCompressor() {
        ZSTD_freeCCtx(cctx);
    }

    // heuristic whether to use shuffle or not
    // using min compression level, compare compression of the first N bytes with or without shuffle using fast clevel
    // if shuffle improves compression by 7% or more, use shuffle
    bool use_shuffle_heuristic(char * const dst, const uint64_t dstCapacity, const char * const src, const uint64_t srcSize) {
        if(srcSize < SHUFFLE_HEURISTIC_BLOCKSIZE) {
            return false;
        };
        // copy from start of block
        blosc_shuffle(reinterpret_cast<const uint8_t*>(src), reinterpret_cast<uint8_t*>(shuffleblock.get()), SHUFFLE_HEURISTIC_BLOCKSIZE, SHUFFLE_ELEMSIZE);
        uint32_t shuffle_approx    = ZSTD_compressCCtx(cctx, dst, dstCapacity, shuffleblock.get(),
                                                       SHUFFLE_HEURISTIC_BLOCKSIZE, 
                                                       SHUFFLE_HEURISTIC_CLEVEL);
        uint32_t no_shuffle_approx = ZSTD_compressCCtx(cctx, dst, dstCapacity, src,
                                                       SHUFFLE_HEURISTIC_BLOCKSIZE,
                                                       SHUFFLE_HEURISTIC_CLEVEL);

        // copy from half way of block and add to estimate
        if(srcSize >= MAX_BLOCKSIZE/2 + SHUFFLE_HEURISTIC_BLOCKSIZE) {
            blosc_shuffle(reinterpret_cast<const uint8_t*>(src + MAX_BLOCKSIZE/2),
                          reinterpret_cast<uint8_t*>(shuffleblock.get()), SHUFFLE_HEURISTIC_BLOCKSIZE, SHUFFLE_ELEMSIZE);
        shuffle_approx += ZSTD_compressCCtx(cctx, dst, dstCapacity, shuffleblock.get(),
                                            SHUFFLE_HEURISTIC_BLOCKSIZE,
                                            SHUFFLE_HEURISTIC_CLEVEL);
        no_shuffle_approx += ZSTD_compressCCtx(cctx, dst, dstCapacity, src + MAX_BLOCKSIZE/2,
                                               SHUFFLE_HEURISTIC_BLOCKSIZE,
                                               SHUFFLE_HEURISTIC_CLEVEL);
        }
        return (static_cast<float>(no_shuffle_approx) / static_cast<float>(shuffle_approx)) > SHUFFLE_MIN_IMPROVEMENT_THRESHOLD;
    }
    uint64_t compress(char * const dst, const uint64_t dstCapacity,
                      const char * const src, const uint64_t srcSize,
                      int compress_level) {

        if(use_shuffle_heuristic(dst, dstCapacity, src, srcSize)) {
            uint64_t remainder = srcSize % SHUFFLE_ELEMSIZE;
            blosc_shuffle(reinterpret_cast<const uint8_t*>(src), reinterpret_cast<uint8_t*>(shuffleblock.get()), srcSize - remainder, SHUFFLE_ELEMSIZE);
            std::memcpy(shuffleblock.get() + srcSize - remainder, src + srcSize - remainder, remainder);
            uint32_t output_size = ZSTD_compressCCtx(cctx, dst, dstCapacity, shuffleblock.get(), srcSize, compress_level);
            output_size = output_size | SHUFFLE_MASK;
            return output_size;
        } else {
            uint32_t output_size = ZSTD_compressCCtx(cctx, dst, dstCapacity, src, srcSize, compress_level);
            return output_size;
        }
    }
};

struct ZstdDecompressor {
    ZSTD_DCtx * dctx;
    ZstdDecompressor() : dctx(ZSTD_createDCtx()) {}
    ~ZstdDecompressor() {
        ZSTD_freeDCtx(dctx);
    }
    uint64_t decompress(char * const dst, const uint64_t dstCapacity,
                        const char * const src, const uint64_t srcSize) {
        if(srcSize > MAX_ZBLOCKSIZE) {
            return 0; // 0 indicates an error
        }
        size_t output_blocksize = ZSTD_decompressDCtx(dctx, dst, dstCapacity, src, srcSize);
        if(ZSTD_isError(output_blocksize)) {
            return 0; // 0 indicates an error
        }
        return output_blocksize;
    }
    static bool is_error(const uint64_t blocksize) { return blocksize == 0; }
};

struct ZstdShuffleDecompressor {
    ZSTD_DCtx * dctx;
    std::unique_ptr<char[]> shuffleblock;
    ZstdShuffleDecompressor() : dctx(ZSTD_createDCtx()), shuffleblock(MAKE_UNIQUE_BLOCK(MAX_BLOCKSIZE)) {}
    ~ZstdShuffleDecompressor() {
        ZSTD_freeDCtx(dctx);
    }
    uint64_t decompress(char * const dst, const uint64_t dstCapacity,
                        const char * const src, uint64_t srcSize) { // srcSize modified by shuffle mask, so not const
        bool is_shuffled = srcSize & SHUFFLE_MASK;
        if(is_shuffled) {
            // set shuffle bit to zero, negate shuffle mask
            srcSize = srcSize & (~SHUFFLE_MASK);
            if(srcSize > MAX_ZBLOCKSIZE) {
                return 0; // 0 indicates an error
            }
            size_t output_blocksize = ZSTD_decompressDCtx(dctx, shuffleblock.get(), dstCapacity, src, srcSize);
            if(ZSTD_isError(output_blocksize)) {
                return 0; // 0 indicates an error
            }
            uint64_t remainder = output_blocksize % SHUFFLE_ELEMSIZE;
            blosc_unshuffle(reinterpret_cast<uint8_t*>(shuffleblock.get()), reinterpret_cast<uint8_t*>(dst), output_blocksize - remainder, SHUFFLE_ELEMSIZE);
            std::memcpy(dst + output_blocksize - remainder, shuffleblock.get() + output_blocksize - remainder, remainder);
            return output_blocksize;
        } else {
            if(srcSize > MAX_BLOCKSIZE) {
                return 0; // 0 indicates an error
            }
            size_t output_blocksize = ZSTD_decompressDCtx(dctx, dst, dstCapacity, src, srcSize);
            if(ZSTD_isError(output_blocksize)) {
                return 0; // 0 indicates an error
            }
            return output_blocksize;
        }
    }
    static bool is_error(const uint64_t blocksize) { return blocksize == 0; }
};

#endif
