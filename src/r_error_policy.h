#ifndef _QS2_R_ERROR_POLICY_H_
#define _QS2_R_ERROR_POLICY_H_

#include <R_ext/Error.h>
#include "io/error_policy.h"
#include <string>

struct RErrorPolicy {
    [[noreturn]] static inline void raise(const char * const msg) {
        (Rf_error)("%s", msg);
    }
    [[noreturn]] static inline void raise(const std::string & msg) {
        (Rf_error)("%s", msg.c_str());
    }
};

#endif
