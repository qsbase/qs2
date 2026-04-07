#ifndef _QS2_QS_UNWIND_PROTECT_H_
#define _QS2_QS_UNWIND_PROTECT_H_

#include <Rcpp.h>
#include <Rcpp/unwindProtect.h>
#include <utility>

template <typename block_io_type, typename callback_type>
inline auto qs_with_unwind_cleanup(block_io_type& block_io, callback_type&& callback, const char* warning_msg = nullptr)
    -> decltype(std::forward<callback_type>(callback)()) {
    try {
        return std::forward<callback_type>(callback)();
    } catch (Rcpp::LongjumpException&) {
        block_io.cleanup();
        if (warning_msg != nullptr) {
            Rf_warning("%s", warning_msg);
        }
        throw;
    }
}

#endif

// Notes to future self on what this is doing now

// unwind signature
// SEXP R_UnwindProtect(SEXP (*fun)(void *data), void *data,
//                      void (*cleanfun)(void *data, Rboolean jump),
//                      void *cleandata, SEXP cont);

// R still handles many errors / interrupts by jumping, so plain RAII around
// R_Serialize / R_Unserialize is not enough. If R jumps out directly, C++
// destructors on the skipped path do not run.

// What Rcpp::unwindProtect is doing under the hood:
// - it creates a tiny helper frame that owns the continuation token and jmp_buf
// - it calls R_UnwindProtect around the callback we give it
// - if R errors or interrupts, R_UnwindProtect runs its clean function
// - that clean function longjmps back into the tiny helper frame, not into the
//   qs_read / qs_save frame that owns block_io or TBB state
// - the tiny helper frame preserves the token and throws Rcpp::LongjumpException
// - qs2 catches that exception in ordinary C++ code, calls block_io.cleanup(),
//   optionally warns if the operation leaves partial user-visible state, and
//   rethrows
// - the Rcpp-generated BEGIN_RCPP / END_RCPP wrapper finally resumes the
//   original R unwind

// The important bit is that the jumped-into frame stays tiny and boring,
// because longjmp is not normal C++ control flow. You do not want to land back
// in a frame full of non-trivial mutable C++ objects and then keep using those
// objects as if nothing happened. The tiny helper frame only holds unwind
// plumbing, so after the jump it can immediately throw a C++ exception and get
// out of the way. The real qs_* frame still owns block_io / tbb::global_control,
// but it only sees a normal C++ exception path, which is where qs2-specific
// cleanup belongs.

// So this header should stay boring. qs2-specific work here is just "if the R
// jump got converted into a C++ LongjumpException, run block_io.cleanup() before
// the exception keeps propagating."

// We don't need any of this for qdata paths that use ordinary C++ / Rcpp error
// handling and do not go through R_Serialize / R_Unserialize.

// References
// https://yutani.rbind.io/post/r-rust-protect-and-unwinding/
// https://github.com/r-lib/cpp11/blob/51f4cd5ad9425a491dedf951a3679346d416e51c/vignettes/FAQ.Rmd#L287
