#ifndef _QS2_BYTE_SHUFFLE_H
#define _QS2_BYTE_SHUFFLE_H

/* qdata-cpp
 *
 * Byte shuffle (transpose) of fixed-size elements: byte k of element i moves to k * n + i, where n is the number of
 * whole elements in the block. Bytes past the last whole element are left untouched. The layout is the one used by
 * the Blosc shuffle filter, which qdata originally used.
 *
 * GCC and clang get a 16-byte vector kernel for 4- and 8-byte elements (SSE2 on x86-64, NEON on arm64); other
 * compilers and element sizes use plain loops.
 */

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <utility>

namespace byte_shuffle_detail {

template <std::size_t K>
inline void shuffle_plain(const uint8_t* const src, uint8_t* const dest, const std::size_t n, std::size_t i,
                          const std::size_t end) {
    for (; i < end; ++i)
        for (std::size_t k = 0; k < K; ++k) dest[k * n + i] = src[i * K + k];
}

template <std::size_t K>
inline void unshuffle_plain(const uint8_t* const src, uint8_t* const dest, const std::size_t n, std::size_t i,
                            const std::size_t end) {
    for (; i < end; ++i)
        for (std::size_t k = 0; k < K; ++k) dest[i * K + k] = src[k * n + i];
}

#if defined(__GNUC__) || defined(__clang__)
#define QS2_SHUFFLE_INLINE inline __attribute__((always_inline))

typedef uint8_t bv16 __attribute__((vector_size(16)));

QS2_SHUFFLE_INLINE bv16 load16(const uint8_t* const p) {
    bv16 v;
    std::memcpy(&v, p, 16);
    return v;
}

// Interleave the low (high) 8 bytes of a and b. GCC added __builtin_shufflevector in version 12; __builtin_shuffle
// compiles to the same instruction.
QS2_SHUFFLE_INLINE bv16 zip_lo(const bv16 a, const bv16 b) {
#if defined(__clang__) || __GNUC__ >= 12
    return __builtin_shufflevector(a, b, 0, 16, 1, 17, 2, 18, 3, 19, 4, 20, 5, 21, 6, 22, 7, 23);
#else
    const bv16 mask = {0, 16, 1, 17, 2, 18, 3, 19, 4, 20, 5, 21, 6, 22, 7, 23};
    return __builtin_shuffle(a, b, mask);
#endif
}
QS2_SHUFFLE_INLINE bv16 zip_hi(const bv16 a, const bv16 b) {
#if defined(__clang__) || __GNUC__ >= 12
    return __builtin_shufflevector(a, b, 8, 24, 9, 25, 10, 26, 11, 27, 12, 28, 13, 29, 14, 30, 15, 31);
#else
    const bv16 mask = {8, 24, 9, 25, 10, 26, 11, 27, 12, 28, 13, 29, 14, 30, 15, 31};
    return __builtin_shuffle(a, b, mask);
#endif
}

// One stage rotates the byte index across the K registers left by one bit. With 16 elements of K bytes, the
// transpose is a rotation by 4 bits (shuffle) or by log2(K) bits (unshuffle). The register loops are pack
// expansions so that -O2 builds unroll them too.
template <std::size_t K, std::size_t... J>
QS2_SHUFFLE_INLINE void zip_stage(bv16 (&r)[K], std::index_sequence<J...>) {
    const bv16 t[K] = {(J % 2 ? zip_hi(r[J / 2], r[J / 2 + K / 2]) : zip_lo(r[J / 2], r[J / 2 + K / 2]))...};
    ((r[J] = t[J]), ...);
}
template <std::size_t K, int S>
QS2_SHUFFLE_INLINE void zip_stages(bv16 (&r)[K]) {
    if constexpr (S > 0) {
        zip_stage<K>(r, std::make_index_sequence<K>());
        zip_stages<K, S - 1>(r);
    }
}

// elements i to i + 15 into the K streams
template <std::size_t K, std::size_t... J>
QS2_SHUFFLE_INLINE void shuffle16(const uint8_t* const src, uint8_t* const dest, const std::size_t n,
                                  const std::size_t i, std::index_sequence<J...>) {
    bv16 r[K] = {load16(src + i * K + 16 * J)...};
    zip_stages<K, 4>(r);
    (std::memcpy(dest + J * n + i, &r[J], 16), ...);
}
// the K streams at elements i to i + 15 into elements
template <std::size_t K, std::size_t... J>
QS2_SHUFFLE_INLINE void unshuffle16(const uint8_t* const src, uint8_t* const dest, const std::size_t n,
                                    const std::size_t i, std::index_sequence<J...>) {
    bv16 r[K] = {load16(src + J * n + i)...};
    zip_stages<K, K == 8 ? 3 : 2>(r);
    (std::memcpy(dest + i * K + 16 * J, &r[J], 16), ...);
}

// The plain head aligns the 16-byte stores: stream 0 on shuffle, the elements on unshuffle when that is possible.
// Callers pass buffers at any offset (R vectors on decode), and unaligned stores cost 20-30%.
template <std::size_t K>
inline void shuffle(const uint8_t* const src, uint8_t* const dest, const std::size_t n) {
    static_assert(K == 4 || K == 8, "vector shuffle supports 4- and 8-byte elements");
    std::size_t i = std::min(n, (16 - reinterpret_cast<std::uintptr_t>(dest) % 16) % 16);
    shuffle_plain<K>(src, dest, n, 0, i);
    for (; i + 16 <= n; i += 16) shuffle16<K>(src, dest, n, i, std::make_index_sequence<K>());
    shuffle_plain<K>(src, dest, n, i, n);
}
template <std::size_t K>
inline void unshuffle(const uint8_t* const src, uint8_t* const dest, const std::size_t n) {
    static_assert(K == 4 || K == 8, "vector shuffle supports 4- and 8-byte elements");
    const std::uintptr_t m = reinterpret_cast<std::uintptr_t>(dest) % 16;
    std::size_t i = m % K ? 0 : std::min(n, (16 - m) % 16 / K);
    unshuffle_plain<K>(src, dest, n, 0, i);
    for (; i + 16 <= n; i += 16) unshuffle16<K>(src, dest, n, i, std::make_index_sequence<K>());
    unshuffle_plain<K>(src, dest, n, i, n);
}

#undef QS2_SHUFFLE_INLINE
#else
template <std::size_t K>
inline void shuffle(const uint8_t* const src, uint8_t* const dest, const std::size_t n) {
    shuffle_plain<K>(src, dest, n, 0, n);
}
template <std::size_t K>
inline void unshuffle(const uint8_t* const src, uint8_t* const dest, const std::size_t n) {
    unshuffle_plain<K>(src, dest, n, 0, n);
}
#endif

}  // namespace byte_shuffle_detail

// The kernel this build uses, for diagnostics. With AVX2 enabled the vector kernel
// still works on 16-byte vectors; the compiler only encodes them as AVX instructions.
inline const char* byte_shuffle_kernel() {
#if defined(__GNUC__) || defined(__clang__)
#if defined(__AVX2__)
    return "vector (AVX2)";
#elif defined(__SSE2__)
    return "vector (SSE2)";
#elif defined(__ARM_NEON)
    return "vector (NEON)";
#else
    return "vector";
#endif
#else
    return "plain";
#endif
}

// Shuffles the whole elements of a block of blocksize bytes; trailing bytes are not written.
inline void byte_shuffle(const uint8_t* const src, uint8_t* const dest, const uint64_t blocksize,
                         const uint64_t bytesoftype) {
    const std::size_t n = blocksize / bytesoftype;
    switch (bytesoftype) {
    case 4:
        byte_shuffle_detail::shuffle<4>(src, dest, n);
        break;
    case 8:
        byte_shuffle_detail::shuffle<8>(src, dest, n);
        break;
    default:
        for (std::size_t k = 0; k < bytesoftype; ++k)
            for (std::size_t i = 0; i < n; ++i) dest[k * n + i] = src[i * bytesoftype + k];
    }
}

inline void byte_unshuffle(const uint8_t* const src, uint8_t* const dest, const uint64_t blocksize,
                           const uint64_t bytesoftype) {
    const std::size_t n = blocksize / bytesoftype;
    switch (bytesoftype) {
    case 4:
        byte_shuffle_detail::unshuffle<4>(src, dest, n);
        break;
    case 8:
        byte_shuffle_detail::unshuffle<8>(src, dest, n);
        break;
    default:
        for (std::size_t i = 0; i < n; ++i)
            for (std::size_t k = 0; k < bytesoftype; ++k) dest[i * bytesoftype + k] = src[k * n + i];
    }
}

#endif
