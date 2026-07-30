#ifndef _QS2_QX_NTHREADS_GUARD_H_
#define _QS2_QX_NTHREADS_GUARD_H_

// Centralized nthreads policy for all qs2 and qdata entry points
// We disallow multithreading in the following conditions:
// 1) if qs2 was built without TBB, nthreads > 1 is not available and is clamped with a warning
// 2) Inside a forked process (mclapply) nthreads > 1 is disabled
// TBB state inherited across fork() is not officially supported and can crash

#include <R_ext/Error.h>
#include <RcppParallel/Fork.h>

#ifndef RCPP_PARALLEL_USE_TBB
#define RCPP_PARALLEL_USE_TBB 0
#endif

static constexpr const char* nthreads_warning_msg_internal = "nthreads > 1 requested but TBB not available; using nthreads = 1";

inline int normalize_nthreads(int nthreads) {
#if RCPP_PARALLEL_USE_TBB == 0
    if (nthreads > 1) {
        Rf_warning("%s", nthreads_warning_msg_internal);
        return 1;
    }
    return nthreads;
#else
    if (nthreads > 1 && RcppParallel::isProcessForkedChild()) {
        return 1;
    }
    return nthreads;
#endif
}

#endif
