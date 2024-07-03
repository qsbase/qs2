#ifndef _QS2_IO_COMMON_H
#define _QS2_IO_COMMON_H

#include <Rcpp.h> // only needed for Rf_error
#include <fstream>
#include <cstdint>
#include <memory>
#include <cstring>
#include <algorithm>

#include "zstd.h"
#include "xxhash/xxhash.c"

#include "BLOSC/shuffle_routines.h"
#include "BLOSC/unshuffle_routines.h"

#ifndef QS2_BLOCKSIZE_TESTING_ONLY_DO_NOT_USE
static constexpr uint64_t MAX_BLOCKSIZE = 786432ULL;
// 2^19 * 1.5 ... we save blocksize as uint32_t, so the last 12 MSBs can be used to store metadata
// This blocksize is 50% larger than `qs` and seems to be a better tradeoff overall in benchmarks
#else
static constexpr uint64_t MAX_BLOCKSIZE = QS2_BLOCKSIZE_TESTING_ONLY_DO_NOT_USE;
#endif

static constexpr uint64_t BLOCK_RESERVE = 64ULL;
static constexpr uint64_t MIN_BLOCKSIZE = MAX_BLOCKSIZE - BLOCK_RESERVE; // smallest allowable block size, except for last block
static const uint64_t MAX_ZBLOCKSIZE = ZSTD_compressBound(MAX_BLOCKSIZE);
// 11111111 11110000 00000000 00000000 in binary, First 12 MSBs can be used for metadata in either zblock or block
// currently only using the first bit for metadata
static constexpr uint32_t BLOCK_METADATA = 0x80000000; // 10000000 00000000 00000000 00000000

static constexpr uint64_t SHUFFLE_ELEMSIZE = 8ULL;
static constexpr uint32_t SHUFFLE_MASK = (1ULL << 31);
static const int SHUFFLE_HEURISTIC_CLEVEL = -1; // compress with fast clevel to test whether shuffle is better
static constexpr uint64_t SHUFFLE_HEURISTIC_BLOCKSIZE = 16384ULL; // 524288 / 8 / 4
static constexpr float SHUFFLE_MIN_IMPROVEMENT_THRESHOLD = 1.07f; // shuffle must be at least 7% better to use

// MAKE_UNIQUE_BLOCK and MAKE_SHARED_BLOCK macros should be used ONLY in initializer lists
#if __cplusplus >= 201402L // Check for C++14 or above
    #define MAKE_UNIQUE_BLOCK(SIZE) std::make_unique<char[]>(SIZE)
#else
    #define MAKE_UNIQUE_BLOCK(SIZE) new char[SIZE]
#endif

// std::make_shared on an array requires c++20 (yes it is true)
// If you try to use make_shared on c++17 or lower it won't compile or will segfault.
#if __cplusplus >= 202002L  // Check for C++20 or above
    #define MAKE_SHARED_BLOCK(SIZE) std::make_shared<char[]>(SIZE)
#else
    #define MAKE_SHARED_BLOCK(SIZE) new char[SIZE]
#endif

#if __cplusplus >= 202002L  // Check for C++20 or above
    #define MAKE_SHARED_BLOCK_ASSIGNMENT(SIZE) std::make_shared<char[]>(SIZE)
#else
    #define MAKE_SHARED_BLOCK_ASSIGNMENT(SIZE) std::shared_ptr<char[]>(new char[SIZE])
#endif

enum class ErrorType { r_error, cpp_error };

// default including cpp_error
template <ErrorType E>
inline void throw_error(const char * const msg) {
    throw std::runtime_error(msg);
}

template<>
inline void throw_error<ErrorType::r_error>(const char * const msg) {
    Rf_error(msg, "%s");
}

template <ErrorType E>
inline void throw_error(const std::string msg) {
    throw std::runtime_error(msg.c_str());
}

template<>
inline void throw_error<ErrorType::r_error>(const std::string msg) {
    Rf_error(msg.c_str(), "%s");
}

// https://stackoverflow.com/a/36835959/2723734
inline constexpr unsigned char operator "" _u8(unsigned long long arg) noexcept {
    return static_cast<uint8_t>(arg);
}

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