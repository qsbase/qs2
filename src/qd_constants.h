#ifndef _QS_QD_CONSTANTS_H_
#define _QS_QD_CONSTANTS_H_

#include <cstdint>
#include "u8_literal.h"

static constexpr uint32_t NA_STRING_LENGTH = 4294967295UL; // 2^32-1 -- length used to signify NA value; note maximum string size is defined by `int` in mkCharLen, so this value is safe
static constexpr uint64_t MAX_SAFE_INTEGER = 9007199254740991ULL; // 2^53-1 -- the largest integer that can be "safely" represented as a double ~ (about 9000 terabytes)

static constexpr uint64_t MIN_SHUFFLE_ARRAYSIZE = 256ULL; // AVX2 vectorized size

static constexpr uint64_t MAX_5_BIT_LENGTH = 32; // exclusive of max value
static constexpr uint64_t MAX_8_BIT_LENGTH = 256; // exclusive of max value
static constexpr uint64_t MAX_16_BIT_LENGTH = 65536; // exclusive of max value
static constexpr uint64_t MAX_32_BIT_LENGTH = 4294967296; // exclusive of max value

static constexpr uint8_t list_header_5 = 0x20_u8;
static constexpr uint8_t list_header_8 = 0x01_u8;
static constexpr uint8_t list_header_16 = 0x02_u8;
static constexpr uint8_t list_header_32 = 0x03_u8;
static constexpr uint8_t list_header_64 = 0x04_u8;

static constexpr uint8_t numeric_header_5 = 0x40_u8;
static constexpr uint8_t numeric_header_8 = 0x05_u8;
static constexpr uint8_t numeric_header_16 = 0x06_u8;
static constexpr uint8_t numeric_header_32 = 0x07_u8;
static constexpr uint8_t numeric_header_64 = 0x08_u8;

static constexpr uint8_t integer_header_5 = 0x60_u8;
static constexpr uint8_t integer_header_8 = 0x09_u8;
static constexpr uint8_t integer_header_16 = 0x0A_u8;
static constexpr uint8_t integer_header_32 = 0x0B_u8;
static constexpr uint8_t integer_header_64 = 0x0C_u8;

static constexpr uint8_t logical_header_5 = 0x80_u8;
static constexpr uint8_t logical_header_8 = 0x0D_u8;
static constexpr uint8_t logical_header_16 = 0x0E_u8;
static constexpr uint8_t logical_header_32 = 0x0F_u8;
static constexpr uint8_t logical_header_64 = 0x10_u8;

static constexpr uint8_t raw_header_32 = 0x17_u8;
static constexpr uint8_t raw_header_64 = 0x18_u8;

static constexpr uint8_t nil_header = 0x00_u8;

static constexpr uint8_t character_header_5 = 0xA0_u8;
static constexpr uint8_t character_header_8 = 0x11_u8;
static constexpr uint8_t character_header_16 = 0x12_u8;
static constexpr uint8_t character_header_32 = 0x13_u8;
static constexpr uint8_t character_header_64 = 0x14_u8;


static constexpr uint8_t complex_header_32 = 0x15_u8;
static constexpr uint8_t complex_header_64 = 0x16_u8;

static constexpr uint8_t attribute_header_5 = 0xE0_u8;
static constexpr uint8_t attribute_header_8 = 0x1E_u8;
static constexpr uint8_t attribute_header_32 = 0x1F_u8;


// strings with encoding
static constexpr uint8_t string_header_NA = 0x0F_u8;
static constexpr uint8_t string_header_5 = 0x20_u8;
static constexpr uint8_t string_header_8 = 0x01_u8;
static constexpr uint8_t string_header_16 = 0x02_u8;
static constexpr uint8_t string_header_32 = 0x03_u8;

static constexpr uint8_t string_enc_native = 0x00_u8;
static constexpr uint8_t string_enc_utf8 = 0x40_u8;
static constexpr uint8_t string_enc_latin1 = 0x80_u8;
static constexpr uint8_t string_enc_bytes = 0xC0_u8;

// masks for 5-bit length headers
static constexpr uint8_t bitmask_type_5 = 0xE0_u8;
static constexpr uint8_t bitmask_length_5 = 0x1F_u8;

// string header masks
static constexpr uint8_t bitmask_string_encoding = 0xC0_u8;
static constexpr uint8_t bitmask_string_type_5 = 0x20_u8;
static constexpr uint8_t bitmask_string_length_type = 0x1F_u8;

// strings with assumed uniform encoding
// static constexpr uint8_t string_UF_max_8 = 254_u8; // exclusive of max string length using a uint8_t
// static constexpr uint8_t string_UF_header_NA = 254_u8; // NA header value
// static constexpr uint8_t string_UF_header_32 = 255_u8; // 32-bit header value

enum class qstype : uint8_t {
  NIL = 0,
  LOGICAL = 1,
  INTEGER = 2,
  REAL = 3,
  COMPLEX = 4,
  CHARACTER = 5,
  LIST = 6,
  RAW = 7,
  ATTRIBUTE = 255
};

#endif


