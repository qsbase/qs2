#include <stdint.h>
#include <stdio.h>

#include <chrono>

#include "zstd.h"

#include <Rcpp.h>
using namespace Rcpp;

#include "immintrin.h"
#include <emmintrin.h>
#include <tmmintrin.h>

namespace shuffle {
static inline void shuffle_generic_inline(const uint64_t type_size,
                                          const uint64_t vectorizable_elements, const uint64_t blocksize,
                                          const uint8_t* const _src, uint8_t* const _dest) {
  uint64_t i, j;
  const uint64_t neblock_quot = blocksize / type_size;
  
  /* Non-optimized shuffle */
  for (j = 0; j < type_size; j++) {
    for (i = vectorizable_elements; i < neblock_quot; i++) {
      _dest[j*neblock_quot+i] = _src[i*type_size+j];
    }
  }
}

#if defined (__AVX2__)

static void shuffle8_avx2(uint8_t* const dest, const uint8_t* const src,
                          const uint64_t vectorizable_elements, const uint64_t total_elements) {
  static const uint64_t bytesoftype = 8;
  uint64_t j;
  int k, l;
  __m256i ymm0[8], ymm1[8];
  
  for (j = 0; j < vectorizable_elements; j += sizeof(__m256i)) {
    /* Fetch 32 elements (256 bytes) then transpose bytes. */
    for (k = 0; k < 8; k++) {
      ymm0[k] = _mm256_loadu_si256((__m256i*)(src + (j * bytesoftype) + (k * sizeof(__m256i))));
      ymm1[k] = _mm256_shuffle_epi32(ymm0[k], 0x4e);
      ymm1[k] = _mm256_unpacklo_epi8(ymm0[k], ymm1[k]);
    }
    /* Transpose words */
    for (k = 0, l = 0; k < 4; k++, l +=2) {
      ymm0[k*2] = _mm256_unpacklo_epi16(ymm1[l], ymm1[l+1]);
      ymm0[k*2+1] = _mm256_unpackhi_epi16(ymm1[l], ymm1[l+1]);
    }
    /* Transpose double words */
    for (k = 0, l = 0; k < 4; k++, l++) {
      if (k == 2) l += 2;
      ymm1[k*2] = _mm256_unpacklo_epi32(ymm0[l], ymm0[l+2]);
      ymm1[k*2+1] = _mm256_unpackhi_epi32(ymm0[l], ymm0[l+2]);
    }
    /* Transpose quad words */
    for (k = 0; k < 4; k++) {
      ymm0[k*2] = _mm256_unpacklo_epi64(ymm1[k], ymm1[k+4]);
      ymm0[k*2+1] = _mm256_unpackhi_epi64(ymm1[k], ymm1[k+4]);
    }
    for(k = 0; k < 8; k++) {
      ymm1[k] = _mm256_permute4x64_epi64(ymm0[k], 0x72);
      ymm0[k] = _mm256_permute4x64_epi64(ymm0[k], 0xD8);
      ymm0[k] = _mm256_unpacklo_epi16(ymm0[k], ymm1[k]);
    }
    /* Store the result vectors */
    uint8_t* const dest_for_jth_element = dest + j;
    for (k = 0; k < 8; k++) {
      _mm256_storeu_si256((__m256i*)(dest_for_jth_element + (k * total_elements)), ymm0[k]);
    }
  }
}

static void shuffle4_avx2(uint8_t* const dest, const uint8_t* const src,
                          const uint64_t vectorizable_elements, const uint64_t total_elements) {
  static const uint64_t bytesoftype = 4;
  uint64_t i;
  int j;
  __m256i ymm0[4], ymm1[4];
  
  /* Create the shuffle mask.
   NOTE: The XMM/YMM 'set' intrinsics require the arguments to be ordered from
   most to least significant (i.e., their order is reversed when compared to
   loading the mask from an array). */
  const __m256i mask = _mm256_set_epi32(
    0x07, 0x03, 0x06, 0x02, 0x05, 0x01, 0x04, 0x00);
  
  for (i = 0; i < vectorizable_elements; i += sizeof(__m256i)) {
    /* Fetch 32 elements (128 bytes) then transpose bytes and words. */
    for (j = 0; j < 4; j++) {
      ymm0[j] = _mm256_loadu_si256((__m256i*)(src + (i * bytesoftype) + (j * sizeof(__m256i))));
      ymm1[j] = _mm256_shuffle_epi32(ymm0[j], 0xd8);
      ymm0[j] = _mm256_shuffle_epi32(ymm0[j], 0x8d);
      ymm0[j] = _mm256_unpacklo_epi8(ymm1[j], ymm0[j]);
      ymm1[j] = _mm256_shuffle_epi32(ymm0[j], 0x04e);
      ymm0[j] = _mm256_unpacklo_epi16(ymm0[j], ymm1[j]);
    }
    /* Transpose double words */
    for (j = 0; j < 2; j++) {
      ymm1[j*2] = _mm256_unpacklo_epi32(ymm0[j*2], ymm0[j*2+1]);
      ymm1[j*2+1] = _mm256_unpackhi_epi32(ymm0[j*2], ymm0[j*2+1]);
    }
    /* Transpose quad words */
    for (j = 0; j < 2; j++) {
      ymm0[j*2] = _mm256_unpacklo_epi64(ymm1[j], ymm1[j+2]);
      ymm0[j*2+1] = _mm256_unpackhi_epi64(ymm1[j], ymm1[j+2]);
    }
    for (j = 0; j < 4; j++) {
      ymm0[j] = _mm256_permutevar8x32_epi32(ymm0[j], mask);
    }
    /* Store the result vectors */
    uint8_t* const dest_for_ith_element = dest + i;
    for (j = 0; j < 4; j++) {
      _mm256_storeu_si256((__m256i*)(dest_for_ith_element + (j * total_elements)), ymm0[j]);
    }
  }
}

#elif defined(__SSE2__)

static void
shuffle8_sse2(uint8_t* const dest, const uint8_t* const src,
              const uint64_t vectorizable_elements, const uint64_t total_elements) {
  static const uint64_t bytesoftype = 8;
  uint64_t j;
  int k, l;
  uint8_t* dest_for_jth_element;
  __m128i xmm0[8], xmm1[8];
  
  for (j = 0; j < vectorizable_elements; j += sizeof(__m128i)) {
    /* Fetch 16 elements (128 bytes) then transpose bytes. */
    for (k = 0; k < 8; k++) {
      xmm0[k] = _mm_loadu_si128((__m128i*)(src + (j * bytesoftype) + (k * sizeof(__m128i))));
      xmm1[k] = _mm_shuffle_epi32(xmm0[k], 0x4e);
      xmm1[k] = _mm_unpacklo_epi8(xmm0[k], xmm1[k]);
    }
    /* Transpose words */
    for (k = 0, l = 0; k < 4; k++, l +=2) {
      xmm0[k*2] = _mm_unpacklo_epi16(xmm1[l], xmm1[l+1]);
      xmm0[k*2+1] = _mm_unpackhi_epi16(xmm1[l], xmm1[l+1]);
    }
    /* Transpose double words */
    for (k = 0, l = 0; k < 4; k++, l++) {
      if (k == 2) l += 2;
      xmm1[k*2] = _mm_unpacklo_epi32(xmm0[l], xmm0[l+2]);
      xmm1[k*2+1] = _mm_unpackhi_epi32(xmm0[l], xmm0[l+2]);
    }
    /* Transpose quad words */
    for (k = 0; k < 4; k++) {
      xmm0[k*2] = _mm_unpacklo_epi64(xmm1[k], xmm1[k+4]);
      xmm0[k*2+1] = _mm_unpackhi_epi64(xmm1[k], xmm1[k+4]);
    }
    /* Store the result vectors */
    dest_for_jth_element = dest + j;
    for (k = 0; k < 8; k++) {
      _mm_storeu_si128((__m128i*)(dest_for_jth_element + (k * total_elements)), xmm0[k]);
    }
  }
}

static void shuffle4_sse2(uint8_t* const dest, const uint8_t* const src,
                          const uint64_t vectorizable_elements, const uint64_t total_elements) {
  static const uint64_t bytesoftype = 4;
  uint64_t i;
  int j;
  uint8_t* dest_for_ith_element;
  __m128i xmm0[4], xmm1[4];
  
  for (i = 0; i < vectorizable_elements; i += sizeof(__m128i)) {
    /* Fetch 16 elements (64 bytes) then transpose bytes and words. */
    for (j = 0; j < 4; j++) {
      xmm0[j] = _mm_loadu_si128((__m128i*)(src + (i * bytesoftype) + (j * sizeof(__m128i))));
      xmm1[j] = _mm_shuffle_epi32(xmm0[j], 0xd8);
      xmm0[j] = _mm_shuffle_epi32(xmm0[j], 0x8d);
      xmm0[j] = _mm_unpacklo_epi8(xmm1[j], xmm0[j]);
      xmm1[j] = _mm_shuffle_epi32(xmm0[j], 0x04e);
      xmm0[j] = _mm_unpacklo_epi16(xmm0[j], xmm1[j]);
    }
    /* Transpose double words */
    for (j = 0; j < 2; j++) {
      xmm1[j*2] = _mm_unpacklo_epi32(xmm0[j*2], xmm0[j*2+1]);
      xmm1[j*2+1] = _mm_unpackhi_epi32(xmm0[j*2], xmm0[j*2+1]);
    }
    /* Transpose quad words */
    for (j = 0; j < 2; j++) {
      xmm0[j*2] = _mm_unpacklo_epi64(xmm1[j], xmm1[j+2]);
      xmm0[j*2+1] = _mm_unpackhi_epi64(xmm1[j], xmm1[j+2]);
    }
    /* Store the result vectors */
    dest_for_ith_element = dest + i;
    for (j = 0; j < 4; j++) {
      _mm_storeu_si128((__m128i*)(dest_for_ith_element + (j * total_elements)), xmm0[j]);
    }
  }
}

#else
// if neither supported, no other functions needed
#endif

// shuffle dispatcher
static void blosc_shuffle(const uint8_t * const src, uint8_t * const dest, const uint64_t blocksize, const uint64_t bytesoftype) {
#if defined (__AVX2__)
  uint64_t total_elements = blocksize / bytesoftype;
  uint64_t vectorized_chunk_size = bytesoftype * sizeof(__m256i);
  uint64_t vectorizable_bytes = blocksize - (blocksize % vectorized_chunk_size);
  uint64_t vectorizable_elements = vectorizable_bytes / bytesoftype;
  switch(bytesoftype) {
  case 4:
    shuffle4_avx2(dest, src, vectorizable_elements, total_elements);
    break;
  case 8:
    shuffle8_avx2(dest, src, vectorizable_elements, total_elements);
    break;
  }
  if(blocksize != vectorizable_bytes) shuffle_generic_inline(bytesoftype, vectorizable_elements, blocksize, src, dest);
#elif defined(__SSE2__)
  uint64_t total_elements = blocksize / bytesoftype;
  uint64_t vectorized_chunk_size = bytesoftype * sizeof(__m128i);
  uint64_t vectorizable_bytes = blocksize - (blocksize % vectorized_chunk_size);
  uint64_t vectorizable_elements = vectorizable_bytes / bytesoftype;
  switch(bytesoftype) {
  case 4:
    shuffle4_sse2(dest, src, vectorizable_elements, total_elements);
    break;
  case 8:
    shuffle8_sse2(dest, src, vectorizable_elements, total_elements);
    break;
  }
  if(blocksize != vectorizable_bytes) shuffle_generic_inline(bytesoftype, vectorizable_elements, blocksize, src, dest);
#else
  shuffle_generic_inline(bytesoftype, 0, blocksize, src, dest);
#endif
}


static inline void unshuffle_generic_inline(const uint64_t type_size,
                                            const uint64_t vectorizable_elements, const uint64_t blocksize,
                                            const uint8_t* const _src, uint8_t* const _dest) {
  uint64_t i, j;
  
  /* Calculate the number of elements in the block. */
  const uint64_t neblock_quot = blocksize / type_size;
  
  /* Non-optimized unshuffle */
  for (i = vectorizable_elements; i < neblock_quot; i++) {
    for (j = 0; j < type_size; j++) {
      _dest[i*type_size+j] = _src[j*neblock_quot+i];
    }
  }
}

#if defined (__AVX2__)

static void unshuffle4_avx2(uint8_t* const dest, const uint8_t* const src,
                            const uint64_t vectorizable_elements, const uint64_t total_elements) {
  static const uint64_t bytesoftype = 4;
  uint64_t i;
  int j;
  __m256i ymm0[4], ymm1[4];
  
  for (i = 0; i < vectorizable_elements; i += sizeof(__m256i)) {
    /* Load 32 elements (128 bytes) into 4 YMM registers. */
    const uint8_t* const src_for_ith_element = src + i;
    for (j = 0; j < 4; j++) {
      ymm0[j] = _mm256_loadu_si256((__m256i*)(src_for_ith_element + (j * total_elements)));
    }
    /* Shuffle bytes */
    for (j = 0; j < 2; j++) {
      /* Compute the low 64 bytes */
      ymm1[j] = _mm256_unpacklo_epi8(ymm0[j*2], ymm0[j*2+1]);
      /* Compute the hi 64 bytes */
      ymm1[2+j] = _mm256_unpackhi_epi8(ymm0[j*2], ymm0[j*2+1]);
    }
    /* Shuffle 2-byte words */
    for (j = 0; j < 2; j++) {
      /* Compute the low 64 bytes */
      ymm0[j] = _mm256_unpacklo_epi16(ymm1[j*2], ymm1[j*2+1]);
      /* Compute the hi 64 bytes */
      ymm0[2+j] = _mm256_unpackhi_epi16(ymm1[j*2], ymm1[j*2+1]);
    }
    ymm1[0] = _mm256_permute2x128_si256(ymm0[0], ymm0[2], 0x20);
    ymm1[1] = _mm256_permute2x128_si256(ymm0[1], ymm0[3], 0x20);
    ymm1[2] = _mm256_permute2x128_si256(ymm0[0], ymm0[2], 0x31);
    ymm1[3] = _mm256_permute2x128_si256(ymm0[1], ymm0[3], 0x31);
    
    /* Store the result vectors in proper order */
    for (j = 0; j < 4; j++) {
      _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (j * sizeof(__m256i))), ymm1[j]);
    }
  }
}

static void unshuffle8_avx2(uint8_t* const dest, const uint8_t* const src,
                            const uint64_t vectorizable_elements, const uint64_t total_elements) {
  static const uint64_t bytesoftype = 8;
  uint64_t i;
  int j;
  __m256i ymm0[8], ymm1[8];
  
  for (i = 0; i < vectorizable_elements; i += sizeof(__m256i)) {
    /* Fetch 32 elements (256 bytes) into 8 YMM registers. */
    const uint8_t* const src_for_ith_element = src + i;
    for (j = 0; j < 8; j++) {
      ymm0[j] = _mm256_loadu_si256((__m256i*)(src_for_ith_element + (j * total_elements)));
    }
    /* Shuffle bytes */
    for (j = 0; j < 4; j++) {
      /* Compute the low 32 bytes */
      ymm1[j] = _mm256_unpacklo_epi8(ymm0[j*2], ymm0[j*2+1]);
      /* Compute the hi 32 bytes */
      ymm1[4+j] = _mm256_unpackhi_epi8(ymm0[j*2], ymm0[j*2+1]);
    }
    /* Shuffle words */
    for (j = 0; j < 4; j++) {
      /* Compute the low 32 bytes */
      ymm0[j] = _mm256_unpacklo_epi16(ymm1[j*2], ymm1[j*2+1]);
      /* Compute the hi 32 bytes */
      ymm0[4+j] = _mm256_unpackhi_epi16(ymm1[j*2], ymm1[j*2+1]);
    }
    for (j = 0; j < 8; j++) {
      ymm0[j] = _mm256_permute4x64_epi64(ymm0[j], 0xd8);
    }
    
    /* Shuffle 4-byte dwords */
    for (j = 0; j < 4; j++) {
      /* Compute the low 32 bytes */
      ymm1[j] = _mm256_unpacklo_epi32(ymm0[j*2], ymm0[j*2+1]);
      /* Compute the hi 32 bytes */
      ymm1[4+j] = _mm256_unpackhi_epi32(ymm0[j*2], ymm0[j*2+1]);
    }
    
    /* Store the result vectors in proper order */
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (0 * sizeof(__m256i))), ymm1[0]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (1 * sizeof(__m256i))), ymm1[2]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (2 * sizeof(__m256i))), ymm1[1]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (3 * sizeof(__m256i))), ymm1[3]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (4 * sizeof(__m256i))), ymm1[4]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (5 * sizeof(__m256i))), ymm1[6]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (6 * sizeof(__m256i))), ymm1[5]);
    _mm256_storeu_si256((__m256i*)(dest + (i * bytesoftype) + (7 * sizeof(__m256i))), ymm1[7]);
  }
}



#elif defined(__SSE2__)

static void unshuffle4_sse2(uint8_t* const dest, const uint8_t* const src,
                            const uint64_t vectorizable_elements, const uint64_t total_elements) {
  static const uint64_t bytesoftype = 4;
  uint64_t i;
  int j;
  __m128i xmm0[4], xmm1[4];
  
  for (i = 0; i < vectorizable_elements; i += sizeof(__m128i)) {
    /* Load 16 elements (64 bytes) into 4 XMM registers. */
    const uint8_t* const src_for_ith_element = src + i;
    for (j = 0; j < 4; j++) {
      xmm0[j] = _mm_loadu_si128((__m128i*)(src_for_ith_element + (j * total_elements)));
    }
    /* Shuffle bytes */
    for (j = 0; j < 2; j++) {
      /* Compute the low 32 bytes */
      xmm1[j] = _mm_unpacklo_epi8(xmm0[j*2], xmm0[j*2+1]);
      /* Compute the hi 32 bytes */
      xmm1[2+j] = _mm_unpackhi_epi8(xmm0[j*2], xmm0[j*2+1]);
    }
    /* Shuffle 2-byte words */
    for (j = 0; j < 2; j++) {
      /* Compute the low 32 bytes */
      xmm0[j] = _mm_unpacklo_epi16(xmm1[j*2], xmm1[j*2+1]);
      /* Compute the hi 32 bytes */
      xmm0[2+j] = _mm_unpackhi_epi16(xmm1[j*2], xmm1[j*2+1]);
    }
    /* Store the result vectors in proper order */
    _mm_storeu_si128((__m128i*)(dest + (i * bytesoftype) + (0 * sizeof(__m128i))), xmm0[0]);
    _mm_storeu_si128((__m128i*)(dest + (i * bytesoftype) + (1 * sizeof(__m128i))), xmm0[2]);
    _mm_storeu_si128((__m128i*)(dest + (i * bytesoftype) + (2 * sizeof(__m128i))), xmm0[1]);
    _mm_storeu_si128((__m128i*)(dest + (i * bytesoftype) + (3 * sizeof(__m128i))), xmm0[3]);
  }
}

/* Routine optimized for unshuffling a buffer for a type size of 8 bytes. */
static void unshuffle8_sse2(uint8_t* const dest, const uint8_t* const src,
                            const uint64_t vectorizable_elements, const uint64_t total_elements) {
  static const uint64_t bytesoftype = 8;
  uint64_t i;
  int j;
  __m128i xmm0[8], xmm1[8];
  
  for (i = 0; i < vectorizable_elements; i += sizeof(__m128i)) {
    /* Load 16 elements (128 bytes) into 8 XMM registers. */
    const uint8_t* const src_for_ith_element = src + i;
    for (j = 0; j < 8; j++) {
      xmm0[j] = _mm_loadu_si128((__m128i*)(src_for_ith_element + (j * total_elements)));
    }
    /* Shuffle bytes */
    for (j = 0; j < 4; j++) {
      /* Compute the low 32 bytes */
      xmm1[j] = _mm_unpacklo_epi8(xmm0[j*2], xmm0[j*2+1]);
      /* Compute the hi 32 bytes */
      xmm1[4+j] = _mm_unpackhi_epi8(xmm0[j*2], xmm0[j*2+1]);
    }
    /* Shuffle 2-byte words */
    for (j = 0; j < 4; j++) {
      /* Compute the low 32 bytes */
      xmm0[j] = _mm_unpacklo_epi16(xmm1[j*2], xmm1[j*2+1]);
      /* Compute the hi 32 bytes */
      xmm0[4+j] = _mm_unpackhi_epi16(xmm1[j*2], xmm1[j*2+1]);
    }
    /* Shuffle 4-byte dwords */
    for (j = 0; j < 4; j++) {
      /* Compute the low 32 bytes */
      xmm1[j] = _mm_unpacklo_epi32(xmm0[j*2], xmm0[j*2+1]);
      /* Compute the hi 32 bytes */
      xmm1[4+j] = _mm_unpackhi_epi32(xmm0[j*2], xmm0[j*2+1]);
    }
    /* Store the result vectors in proper order */
    _mm_storeu_si128((__m128i*)(dest + (i * bytesoftype) + (0 * sizeof(__m128i))), xmm1[0]);
    _mm_storeu_si128((__m128i*)(dest + (i * bytesoftype) + (1 * sizeof(__m128i))), xmm1[4]);
    _mm_storeu_si128((__m128i*)(dest + (i * bytesoftype) + (2 * sizeof(__m128i))), xmm1[2]);
    _mm_storeu_si128((__m128i*)(dest + (i * bytesoftype) + (3 * sizeof(__m128i))), xmm1[6]);
    _mm_storeu_si128((__m128i*)(dest + (i * bytesoftype) + (4 * sizeof(__m128i))), xmm1[1]);
    _mm_storeu_si128((__m128i*)(dest + (i * bytesoftype) + (5 * sizeof(__m128i))), xmm1[5]);
    _mm_storeu_si128((__m128i*)(dest + (i * bytesoftype) + (6 * sizeof(__m128i))), xmm1[3]);
    _mm_storeu_si128((__m128i*)(dest + (i * bytesoftype) + (7 * sizeof(__m128i))), xmm1[7]);
  }
}

#else
// if neither supported, no other functions needed
#endif

// shuffle dispatcher
static void blosc_unshuffle(const uint8_t * const src, uint8_t * const dest, const uint64_t blocksize, const uint64_t bytesoftype) {
#if defined (__AVX2__)
  uint64_t total_elements = blocksize / bytesoftype;
  uint64_t vectorized_chunk_size = bytesoftype * sizeof(__m256i);
  uint64_t vectorizable_bytes = blocksize - (blocksize % vectorized_chunk_size);
  uint64_t vectorizable_elements = vectorizable_bytes / bytesoftype;
  switch(bytesoftype) {
  case 4:
    unshuffle4_avx2(dest, src, vectorizable_elements, total_elements);
    break;
  case 8:
    unshuffle8_avx2(dest, src, vectorizable_elements, total_elements);
    break;
  }
  unshuffle_generic_inline(bytesoftype, vectorizable_elements, blocksize, src, dest);
#elif defined(__SSE2__)
  uint64_t total_elements = blocksize / bytesoftype;
  uint64_t vectorized_chunk_size = bytesoftype * sizeof(__m128i);
  uint64_t vectorizable_bytes = blocksize - (blocksize % vectorized_chunk_size);
  uint64_t vectorizable_elements = vectorizable_bytes / bytesoftype;
  switch(bytesoftype) {
  case 4:
    unshuffle4_sse2(dest, src, vectorizable_elements, total_elements);
    break;
  case 8:
    unshuffle8_sse2(dest, src, vectorizable_elements, total_elements);
    break;
  }
  unshuffle_generic_inline(bytesoftype, vectorizable_elements, blocksize, src, dest);
#else
  unshuffle_generic_inline(bytesoftype, 0, blocksize, src, dest);
#endif
}
}

// [[Rcpp::export(rng=false)]]
std::string SIMD_test() {
#if defined(__AVX2__)
  return "AVX2";
#elif defined(__SSSE3__)
  return "SSSE3";
#else
  return "none";
#endif
}


// [[Rcpp::export(rng=false)]]
DataFrame shuffle_compress(List blocks, int elementsize, int compress_level) {
  size_t nblocks = Rf_xlength(blocks);
  NumericVector outtime(nblocks);
  NumericVector outsize(nblocks);
  
  std::vector<uint8_t> shuffleblock;
  std::vector<uint8_t> zblock;
  for (size_t i = 0; i < nblocks; ++i) {
    auto start = std::chrono::high_resolution_clock::now();
    RawVector block = blocks[i];
    uint64_t blocksize = Rf_xlength(block);
    
    uint64_t compress_bound = ZSTD_compressBound(blocksize);
    if(shuffleblock.size() < blocksize) {
      shuffleblock.resize(blocksize);
    }
    if(zblock.size() < compress_bound) {
      zblock.resize(compress_bound);
    }

    uint64_t remainder = blocksize % elementsize;
    shuffle::blosc_shuffle(reinterpret_cast<uint8_t*>(RAW(block)), shuffleblock.data(), blocksize-remainder, elementsize);
    std::memcpy(shuffleblock.data() + blocksize - remainder, RAW(block) + blocksize - remainder, remainder);
    uint32_t zblocksize = ZSTD_compress(zblock.data(), compress_bound, shuffleblock.data(), blocksize, compress_level);
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end - start;
    outtime[i] = duration.count();
    outsize[i] = zblocksize;
  }
  return DataFrame::create(_["size"] = outsize, _["time"] = outtime);
}


// [[Rcpp::export(rng=false)]]
DataFrame og_compress(List blocks, int compress_level) {
  size_t nblocks = Rf_xlength(blocks);
  NumericVector outtime(nblocks);
  NumericVector outsize(nblocks);
  
  std::vector<uint8_t> zblock;
  for (size_t i = 0; i < nblocks; ++i) {
    auto start = std::chrono::high_resolution_clock::now();
    RawVector block = blocks[i];
    uint64_t blocksize = Rf_xlength(block);
    
    uint64_t compress_bound = ZSTD_compressBound(blocksize);
    if(zblock.size() < compress_bound) {
      zblock.resize(compress_bound);
    }

    uint32_t zblocksize = ZSTD_compress(zblock.data(), compress_bound, RAW(block), blocksize, compress_level);
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end - start;
    outtime[i] = duration.count();
    outsize[i] = zblocksize;
  }
  return DataFrame::create(_["size"] = outsize, _["time"] = outtime);
}

// like above but sample from 4 points in the block
constexpr uint64_t SHUFFLE_HEURISTIC_BLOCKSIZE = 32768;
constexpr uint32_t SHUFFLE_ELEMSIZE = 8;
constexpr uint32_t SHUFFLE_HEURISTIC_CL = -1;
std::array<uint32_t, 8> shuffle_heuristic_impl(char * const dst, const uint64_t dstCapacity, 
                                               char * const shuffleblock,
                                               const char * const src, const uint64_t srcSize) {
  if(srcSize < 8*SHUFFLE_HEURISTIC_BLOCKSIZE) {
    return {0,0,0,0,0,0,0,0};
  } else {
    std::array<uint32_t, 8> output;
    for(int i = 0; i < 4; ++i) {
      uint64_t block_offset = (srcSize / 4ULL) * i; // integer division, rounds down
      shuffle::blosc_shuffle(reinterpret_cast<const uint8_t*>(src + block_offset), reinterpret_cast<uint8_t*>(shuffleblock), SHUFFLE_HEURISTIC_BLOCKSIZE, SHUFFLE_ELEMSIZE);
      output[i*2] = ZSTD_compress(dst, dstCapacity, shuffleblock,
                                      SHUFFLE_HEURISTIC_BLOCKSIZE, 
                                      SHUFFLE_HEURISTIC_CL);
      output[i*2+1] = ZSTD_compress(dst, dstCapacity, src + block_offset,
                                         SHUFFLE_HEURISTIC_BLOCKSIZE,
                                         SHUFFLE_HEURISTIC_CL);
    }
    return output;
  }
}


// [[Rcpp::export(rng=false)]]
DataFrame shuffle_heuristic(List blocks) {
  int nblocks = Rf_xlength(blocks);
  NumericVector outtime(nblocks);
  NumericVector h1(nblocks);
  NumericVector h2(nblocks);
  NumericVector h3(nblocks);
  NumericVector h4(nblocks);
  NumericVector h5(nblocks);
  NumericVector h6(nblocks);
  NumericVector h7(nblocks);
  NumericVector h8(nblocks);
  NumericVector cl(nblocks);
  
  std::vector<uint8_t> shuffleblock(SHUFFLE_HEURISTIC_BLOCKSIZE);
  std::vector<uint8_t> zblock(ZSTD_compressBound(SHUFFLE_HEURISTIC_BLOCKSIZE));
  for (int i = 0; i < nblocks; ++i) {
    auto start = std::chrono::high_resolution_clock::now();
    RawVector block = blocks[i];
    uint64_t blocksize = Rf_xlength(block);
    auto results = shuffle_heuristic_impl(reinterpret_cast<char*>(zblock.data()), 
                                          zblock.size(),
                                          reinterpret_cast<char*>(shuffleblock.data()), 
                                          reinterpret_cast<char*>(RAW(block)), blocksize);

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end - start;
    outtime[i] = duration.count();
    h1[i] = results[0];
    h2[i] = results[1];
    h3[i] = results[2];
    h4[i] = results[3];
    h5[i] = results[4];
    h6[i] = results[5];
    h7[i] = results[6];
    h8[i] = results[7];
  }
  return DataFrame::create(_["h1"] = h1, _["h2"] = h2, _["h3"] = h3, _["h4"] = h4,
                           _["h5"] = h5, _["h6"] = h6, _["h7"] = h7, _["h8"] = h8,
                            _["time"] = outtime);
}

