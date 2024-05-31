#ifndef _QS_UNWIND_PROTECT_IMPLEMENTATION_H_
#define _QS_UNWIND_PROTECT_IMPLEMENTATION_H_

#include <Rcpp.h>

struct RerrorUnwind {
    SEXP cont;
};

inline void throw_unwind_error(void * cont_token, Rboolean jump) {
    if(jump) {
        throw RerrorUnwind{(SEXP)cont_token};
    }
}

#define UNWIND_PROTECT_BEGIN() \
    Rcpp::RObject cont_token = R_MakeUnwindCont(); \
    try {

#define UNWIND_PROTECT_END() \
    } catch(RerrorUnwind & cont) { R_ContinueUnwind(cont.cont); } \
    return R_NilValue; // unreachable

#define DO_UNWIND_PROTECT(_FUNCTION_, _ARGS_) \
    R_UnwindProtect(_FUNCTION_, _ARGS_, throw_unwind_error, (void*)(static_cast<SEXP>(cont_token)), static_cast<SEXP>(cont_token))

#endif