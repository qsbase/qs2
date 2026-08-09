#ifndef _QS2_IO_COMMON_H
#define _QS2_IO_COMMON_H

#include <fstream>
#include <cstdint>
#include <memory>
#include <cstring>
#include <algorithm>

#include "error_policy.h"

#include "zstd.h"
#define XXH_INLINE_ALL
#include "../xxhash/xxhash.h"
#undef XXH_INLINE_ALL

#include "../blosc/shuffle_routines.h"
#include "../blosc/unshuffle_routines.h"

static constexpr uint32_t MAX_BLOCKSIZE = 1048576UL;
static constexpr uint32_t BLOCK_RESERVE = 64UL;
static constexpr uint32_t MIN_BLOCKSIZE = MAX_BLOCKSIZE - BLOCK_RESERVE; // smallest allowable block size, except for last block
static constexpr uint32_t MAX_ZBLOCKSIZE = static_cast<uint32_t>(ZSTD_COMPRESSBOUND(MAX_BLOCKSIZE));
// 2^20 ... we save blocksize as uint32_t, so the last 12 MSBs can be used to store metadata
// This blocksize is 2x larger than `qs` and seems to be a better tradeoff overall in benchmarks

// 11111111 11110000 00000000 00000000 in binary, First 12 MSBs can be used for metadata in either zblock or block
// currently only using the first bit for metadata
static constexpr uint32_t BLOCK_METADATA = 0x80000000; // 10000000 00000000 00000000 00000000
static constexpr uint32_t SHUFFLE_MASK = (1ULL << 31);

struct QioByteCopier {
    static void copy(void * const destination, const void * const source, const std::size_t size) {
        std::memcpy(destination, source, size);
    }
};

inline constexpr uint32_t compressed_block_size(const uint32_t zsize) noexcept {
    return zsize & (~BLOCK_METADATA);
}

inline constexpr bool compressed_block_size_fits_buffer(const uint32_t zsize) noexcept {
    return static_cast<uint64_t>(compressed_block_size(zsize)) <= MAX_ZBLOCKSIZE;
}

// MAKE_UNIQUE_BLOCK and MAKE_SHARED_BLOCK macros should be used ONLY in initializer lists
#define MAKE_UNIQUE_BLOCK(SIZE) std::unique_ptr<char[]>(new char[SIZE])

#if __cplusplus >= 201402L // Check for C++14 or above
    #define MAKE_UNIQUE_BLOCK_CUSTOM(_TYPE_, SIZE) std::make_unique<_TYPE_[]>(SIZE)
#else
    #define MAKE_UNIQUE_BLOCK_CUSTOM(_TYPE_, SIZE) new _TYPE_[SIZE]
#endif

// Shared char buffer for the multithreaded pipeline (blocks are passed by
// shared_ptr between TBB flow-graph nodes). qio_make_block<N>() does a single
// allocation, leaves the bytes uninitialized, and hands back a shared_ptr<char[]>.
template <std::size_t N>
struct qio_uninit_block {
    alignas(16) char data[N];
    qio_uninit_block() {}  // user-provided + empty => 'data' left uninitialized
};

template <std::size_t N>
inline std::shared_ptr<char[]> qio_make_block() {
    std::shared_ptr<qio_uninit_block<N>> holder = std::make_shared<qio_uninit_block<N>>();
    char* data = holder->data;  // read before the move below
    // aliasing ctor: no allocation; move form (C++20) hands off without a refcount op
    return std::shared_ptr<char[]>(std::move(holder), data);
}

#define MAKE_SHARED_BLOCK(SIZE)            qio_make_block<SIZE>()
#define MAKE_SHARED_BLOCK_ASSIGNMENT(SIZE) qio_make_block<SIZE>()

// https://stackoverflow.com/a/36835959/2723734
#ifndef QDATA_U8_LITERAL_DEFINED
#define QDATA_U8_LITERAL_DEFINED
inline constexpr unsigned char operator ""_u8(unsigned long long arg) noexcept {
    return static_cast<uint8_t>(arg);
}
#endif

// #define QS_MT_SERIALIZATION_DEBUG
#if defined(QS_MT_SERIALIZATION_DEBUG)
    #include <iostream>
    #include <sstream>
    #include <mutex>
    // multithreaded print statements for debugging
    // https://stackoverflow.com/a/53288135/2723734
    // Thread-safe std::ostream class.
    #define tout ThreadStream(std::cout)
    class ThreadStream : public std::ostringstream {
    public:
    ThreadStream(std::ostream& os) : os_(os)
    {
        imbue(os.getloc());
        precision(os.precision());
        width(os.width());
        setf(std::ios::fixed, std::ios::floatfield);
    }
    ~ThreadStream() {
        std::lock_guard<std::mutex> guard(_mutex_threadstream);
        os_ << this->str();
    }
    private:
    static std::mutex _mutex_threadstream;
    std::ostream& os_;
    };
    inline std::mutex ThreadStream::_mutex_threadstream{};
    template<typename ...Args>
    inline void TOUT(Args && ...args) {
        (tout << ... << args) << std::endl;
    }
#else
    #define TOUT(...)
#endif

#endif
