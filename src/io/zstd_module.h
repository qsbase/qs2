#ifndef _QS2_ZSTD_MODULE_H
#define _QS2_ZSTD_MODULE_H

#include "io/io_common.h"
#include "io/xgboost_blockshuffle_model.h"


static constexpr uint32_t COMPRESSION_ERROR = 0;
static constexpr uint32_t SHUFFLE_HEURISTIC_BLOCKSIZE = 32768;
static constexpr uint32_t SHUFFLE_ELEMSIZE = 8;

static constexpr uint32_t SHUFFLE_HEURISTIC_CL = -1;
static constexpr uint32_t USE_HEURISTIC = 1;
static constexpr uint32_t DONT_USE_HEURISTIC = 2;

static constexpr int HIGH_COMPRESS_LEVEL_THRESHOLD = 14;
static constexpr double HIGH_HEURISTIC_THRESHOLD = -0.25;
static constexpr double LOW_HEURISTIC_THRESHOLD = 0;

struct ZstdCompressor {
    ZSTD_CCtx * cctx;
    ZstdCompressor() : cctx(ZSTD_createCCtx()) {}
    ~ZstdCompressor() {
        ZSTD_freeCCtx(cctx);
    }
    uint32_t compress(char * const dst, const uint32_t dstCapacity,
                      const char * const src, const uint32_t srcSize,
                      int compress_level) {
        auto output = ZSTD_compressCCtx(cctx, dst, dstCapacity, src, srcSize, compress_level);
        if(ZSTD_isError(output)) {
            return COMPRESSION_ERROR;
        } else {
            return output;
        }
    }
};

struct ZstdShuffleCompressor {

    ZSTD_CCtx * cctx;
    std::unique_ptr<char[]> shuffleblock;
    ZstdShuffleCompressor() : cctx(ZSTD_createCCtx()), shuffleblock(MAKE_UNIQUE_BLOCK(MAX_BLOCKSIZE)) {}
    ~ZstdShuffleCompressor() {
        ZSTD_freeCCtx(cctx);
    }

    static bool is_error(const uint32_t blocksize) { return blocksize == COMPRESSION_ERROR; }

    uint32_t use_shuffle_heuristic(char * const dst, const uint32_t dstCapacity, 
                               const char * const src, const uint32_t srcSize,
                               const int compress_level,
                               const double threshold) {
        if(srcSize < 8*SHUFFLE_HEURISTIC_BLOCKSIZE) {
            return DONT_USE_HEURISTIC;
        } else {
            std::array<double, 9> features;
            features[8] = compress_level;

            for(int i = 0; i < 4; ++i) {
                uint32_t block_offset = (srcSize / 4ULL) * i; // integer division, rounds down
                blosc_shuffle(reinterpret_cast<const uint8_t*>(src + block_offset), reinterpret_cast<uint8_t*>(shuffleblock.get()), SHUFFLE_HEURISTIC_BLOCKSIZE, SHUFFLE_ELEMSIZE);
                auto shuf = ZSTD_compressCCtx(cctx, dst, dstCapacity, shuffleblock.get(),
                                                SHUFFLE_HEURISTIC_BLOCKSIZE, 
                                                SHUFFLE_HEURISTIC_CL);
                if(ZSTD_isError(shuf)) { return COMPRESSION_ERROR; }
                
                auto noshuf = ZSTD_compressCCtx(cctx, dst, dstCapacity, src + block_offset,
                                                  SHUFFLE_HEURISTIC_BLOCKSIZE,
                                                  SHUFFLE_HEURISTIC_CL);
                if(ZSTD_isError(noshuf)) { return COMPRESSION_ERROR; }

                features[i*2] = shuf;
                features[i*2+1] = noshuf;
            }

            if( XgboostBlockshuffleModel::predict_xgboost_impl(features) > threshold ) {
                return USE_HEURISTIC;
            } else {
                return DONT_USE_HEURISTIC;
            }
        }
    }

    uint32_t compress(char * const dst, const uint32_t dstCapacity,
                      const char * const src, const uint32_t srcSize,
                      const int compress_level) {

        uint32_t heuristic;
        if(compress_level >= HIGH_COMPRESS_LEVEL_THRESHOLD) {
            heuristic = use_shuffle_heuristic(dst, dstCapacity, src, srcSize, compress_level, HIGH_HEURISTIC_THRESHOLD);
        } else {
            heuristic = use_shuffle_heuristic(dst, dstCapacity, src, srcSize, compress_level, LOW_HEURISTIC_THRESHOLD);
        }

        if(heuristic == COMPRESSION_ERROR) {
            return COMPRESSION_ERROR;
        } else if(heuristic == USE_HEURISTIC) {
            if(compress_level >= HIGH_COMPRESS_LEVEL_THRESHOLD) { // test both ways
                // shuffle compress into new shuffleblock
                std::unique_ptr<char[]> shuffle_zblock(MAKE_UNIQUE_BLOCK(MAX_ZBLOCKSIZE));
                uint32_t remainder = srcSize % SHUFFLE_ELEMSIZE;
                blosc_shuffle(reinterpret_cast<const uint8_t*>(src), reinterpret_cast<uint8_t*>(shuffleblock.get()), srcSize - remainder, SHUFFLE_ELEMSIZE);
                std::memcpy(shuffleblock.get() + srcSize - remainder, src + srcSize - remainder, remainder);
                auto output_size_with_shuffle = ZSTD_compressCCtx(cctx, shuffle_zblock.get(), MAX_ZBLOCKSIZE, shuffleblock.get(), srcSize, compress_level);
                // compress without shuffle into dst
                auto output_size_no_shuffle = ZSTD_compressCCtx(cctx, dst, dstCapacity, src, srcSize, compress_level);
                // check for any error and propagate
                if(ZSTD_isError(output_size_with_shuffle) || ZSTD_isError(output_size_no_shuffle)) {
                    return COMPRESSION_ERROR;
                }
                if(output_size_with_shuffle > output_size_no_shuffle) {
                    // replace output in dst
                    std::memcpy(dst, shuffle_zblock.get(), output_size_with_shuffle);
                    return output_size_with_shuffle | SHUFFLE_MASK;
                } else {
                    return output_size_no_shuffle;
                }
            } else {
                uint32_t remainder = srcSize % SHUFFLE_ELEMSIZE;
                blosc_shuffle(reinterpret_cast<const uint8_t*>(src), reinterpret_cast<uint8_t*>(shuffleblock.get()), srcSize - remainder, SHUFFLE_ELEMSIZE);
                std::memcpy(shuffleblock.get() + srcSize - remainder, src + srcSize - remainder, remainder);
                auto output_size = ZSTD_compressCCtx(cctx, dst, dstCapacity, shuffleblock.get(), srcSize, compress_level);
                if(ZSTD_isError(output_size)) {
                    return COMPRESSION_ERROR;
                } else {
                return output_size | SHUFFLE_MASK;
                }

            }
        } else { // heuristic == DONT_USE_HEURISTIC
            auto output_size = ZSTD_compressCCtx(cctx, dst, dstCapacity, src, srcSize, compress_level);
            if(ZSTD_isError(output_size)) {
                return COMPRESSION_ERROR;
            } else {
                return output_size;
            }
        }
    }
};

struct ZstdDecompressor {
    ZSTD_DCtx * dctx;
    ZstdDecompressor() : dctx(ZSTD_createDCtx()) {}
    ~ZstdDecompressor() {
        ZSTD_freeDCtx(dctx);
    }
    static bool is_error(const uint32_t blocksize) { return blocksize == COMPRESSION_ERROR; }
    uint32_t decompress(char * const dst, const uint32_t dstCapacity,
                        const char * const src, const uint32_t srcSize) {
        if(srcSize > MAX_ZBLOCKSIZE) {
            return COMPRESSION_ERROR;
        }
        auto output_blocksize = ZSTD_decompressDCtx(dctx, dst, dstCapacity, src, srcSize);
        if(ZSTD_isError(output_blocksize)) {
            return COMPRESSION_ERROR;
        }
        return output_blocksize;
    }
};

struct ZstdShuffleDecompressor {
    ZSTD_DCtx * dctx;
    std::unique_ptr<char[]> shuffleblock;
    ZstdShuffleDecompressor() : dctx(ZSTD_createDCtx()), shuffleblock(MAKE_UNIQUE_BLOCK(MAX_BLOCKSIZE)) {}
    ~ZstdShuffleDecompressor() {
        ZSTD_freeDCtx(dctx);
    }
    static bool is_error(const uint32_t blocksize) { return blocksize == COMPRESSION_ERROR; }
    uint32_t decompress(char * const dst, const uint32_t dstCapacity,
                        const char * const src, uint32_t srcSize) { // srcSize modified by shuffle mask, so not const
        bool is_shuffled = srcSize & SHUFFLE_MASK;
        if(is_shuffled) {
            // set shuffle bit to zero, negate shuffle mask
            srcSize = srcSize & (~SHUFFLE_MASK);
            if(srcSize > MAX_ZBLOCKSIZE) {
                return COMPRESSION_ERROR; // 0 indicates an error
            }
            auto output_blocksize = ZSTD_decompressDCtx(dctx, shuffleblock.get(), dstCapacity, src, srcSize);
            if(ZSTD_isError(output_blocksize)) {
                return COMPRESSION_ERROR; // 0 indicates an error
            }
            uint32_t remainder = output_blocksize % SHUFFLE_ELEMSIZE;
            blosc_unshuffle(reinterpret_cast<uint8_t*>(shuffleblock.get()), reinterpret_cast<uint8_t*>(dst), output_blocksize - remainder, SHUFFLE_ELEMSIZE);
            std::memcpy(dst + output_blocksize - remainder, shuffleblock.get() + output_blocksize - remainder, remainder);
            return output_blocksize;
        } else {
            if(srcSize > MAX_ZBLOCKSIZE) {
                return COMPRESSION_ERROR; // 0 indicates an error
            }
            auto output_blocksize = ZSTD_decompressDCtx(dctx, dst, dstCapacity, src, srcSize);
            if(ZSTD_isError(output_blocksize)) {

                return COMPRESSION_ERROR; // 0 indicates an error
            }
            return output_blocksize;
        }
    }
};

#endif
