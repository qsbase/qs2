#ifndef QDATA_FORMAT_DETAIL_R_COMPAT_LIMITS_H
#define QDATA_FORMAT_DETAIL_R_COMPAT_LIMITS_H

#include <cstddef>
#include <cstdint>
#include <stdexcept>

namespace qdata {
namespace detail {

inline constexpr std::uint64_t max_r_compatible_vector_length = 4503599627370496ULL;
inline constexpr std::uint32_t max_r_compatible_attr_count = 2147483647U;
inline constexpr std::uint32_t max_r_compatible_string_length = 2147483647U;
inline constexpr std::size_t default_qdata_max_nesting_depth = 512;

inline std::size_t checked_max_nesting_depth(const std::size_t value) {
    if(value == 0) {
        throw std::runtime_error("max_depth must be at least 1");
    }
    return value;
}

} // namespace detail
} // namespace qdata

#endif
