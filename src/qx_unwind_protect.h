#ifndef _QS2_QX_UNWIND_PROTECT_H_
#define _QS2_QX_UNWIND_PROTECT_H_

#include <Rcpp.h>
#include <Rcpp/unwindProtect.h>
#include <exception>
#include <functional>
#include <utility>

template <typename io_owner_type, typename operation_type>
inline SEXP qx_with_unwind_cleanup(io_owner_type& io_owner, operation_type&& protected_operation, const char* cleanup_warning = nullptr) {
    std::exception_ptr deferred_cpp_exception;
    try {
        SEXP protected_result = Rcpp::unwindProtect(std::function<SEXP(void)>([&]() -> SEXP {
            try {
                return std::forward<operation_type>(protected_operation)();
            } catch (...) {
                deferred_cpp_exception = std::current_exception();
                return R_NilValue;
            }
        }));
        if (deferred_cpp_exception) {
            std::rethrow_exception(deferred_cpp_exception);
        }
        return protected_result;
    } catch (Rcpp::LongjumpException&) {
        io_owner.cleanup();
        if (cleanup_warning != nullptr) {
            REprintf("%s\n", cleanup_warning);
        }
        throw;
    } catch (...) {
        io_owner.cleanup();
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

// This helper is shared by both qs and qdata read/write paths.

// What Rcpp::unwindProtect is doing under the hood:
// - it creates a tiny helper frame that owns the continuation token and jmp_buf
// - it calls R_UnwindProtect around the callback we give it
// - if R errors or interrupts, R_UnwindProtect runs its clean function
// - that clean function longjmps back into the tiny helper frame, not into the
//   outer qs2/qdata frame that owns block I/O, reader/writer, or TBB state
// - ordinary C++ exceptions from the callback are caught inside the protected
//   callback and rethrown only after R_UnwindProtect returns, so they do not
//   cross the C boundary
// - the tiny helper frame preserves the token and throws Rcpp::LongjumpException
// - qs2 catches that exception in ordinary C++ code, calls cleanup() on the
//   owner object, optionally warns if the operation leaves partial user-visible
//   state, and rethrows
// - the Rcpp-generated BEGIN_RCPP / END_RCPP wrapper finally resumes the
//   original R unwind

// The important bit is that the jumped-into frame stays tiny and boring,
// because longjmp is not normal C++ control flow. You do not want to land back
// in a frame full of non-trivial mutable C++ objects and then keep using those
// objects as if nothing happened. The tiny helper frame only holds unwind
// plumbing, so after the jump it can immediately throw a C++ exception and get
// out of the way. The real qs2/qdata frame still owns block I/O and any TBB
// state, but it only sees a normal C++ exception path, which is where qs2-
// specific cleanup belongs.

// So this header should stay boring. qs2-specific work here is just "if the R
// jump got converted into a C++ LongjumpException, run cleanup() before the
// exception keeps propagating."

// References
// https://yutani.rbind.io/post/r-rust-protect-and-unwinding/
// https://github.com/r-lib/cpp11/blob/51f4cd5ad9425a491dedf951a3679346d416e51c/vignettes/FAQ.Rmd#L287
