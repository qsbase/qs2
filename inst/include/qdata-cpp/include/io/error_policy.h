#ifndef _QS2_ERROR_POLICY_H_
#define _QS2_ERROR_POLICY_H_

#include <stdexcept>
#include <string>

struct StdErrorPolicy {
    [[noreturn]] static inline void raise(const char * const msg) {
        throw std::runtime_error(msg);
    }
    [[noreturn]] static inline void raise(const std::string & msg) {
        throw std::runtime_error(msg.c_str());
    }
};

template <class ErrorPolicy>
[[noreturn]] inline void throw_error(const char * const msg) {
    ErrorPolicy::raise(msg);
}

template <class ErrorPolicy>
[[noreturn]] inline void throw_error(const std::string & msg) {
    ErrorPolicy::raise(msg);
}

#endif
