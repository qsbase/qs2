#ifndef _QS_U8_LITERAL_H_
#define _QS_U8_LITERAL_H_

// https://stackoverflow.com/a/36835959/2723734
inline constexpr unsigned char operator "" _u8(unsigned long long arg) noexcept {
    return static_cast<uint8_t>(arg);
}

#endif