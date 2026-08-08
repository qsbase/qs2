#ifndef _QS2_QX_UNWIND_PROTECT_H_
#define _QS2_QX_UNWIND_PROTECT_H_

#include <Rcpp.h>
#include <Rcpp/unwindProtect.h>
#include <exception>
#include <type_traits>
#include <utility>

// R still reports many errors and interrupts by jumping, so RAII alone is not
// enough. The jump lands in Rcpp::unwindProtect's frame and skips every frame
// in between, so those frames must be trivially destructible. All non-trivial
// owners (block io, serializers, TBB state, scratch buffers) live in the
// caller's frame and are destroyed normally once the jump has been converted to
// Rcpp::LongjumpException. Rcpp's END_RCPP is what finally resumes the R unwind,
// so that exception type must be the one that escapes.
//
// https://yutani.rbind.io/post/r-rust-protect-and-unwinding/
// https://github.com/r-lib/cpp11/blob/51f4cd5ad9425a491dedf951a3679346d416e51c/vignettes/FAQ.Rmd#L287

namespace qx_unwind_detail {

template <typename operation_type>
struct call_state {
    operation_type* operation;
    std::exception_ptr error;
};

template <typename operation_type>
inline SEXP call_body(void* data) {
    call_state<operation_type>* state = static_cast<call_state<operation_type>*>(data);
    try {
        return (*state->operation)();
    } catch (...) {
        state->error = std::current_exception();
        return R_NilValue;
    }
}

}  // namespace qx_unwind_detail

// C++ exceptions are held until after the boundary so they never cross R's C frames.
template <typename operation_type>
inline SEXP qx_unwind_protect(operation_type&& operation) {
    using op_type = typename std::remove_reference<operation_type>::type;
    static_assert(std::is_trivially_destructible<op_type>::value,
                  "unwind callback must be trivially destructible; an R jump skips its destructor");
    qx_unwind_detail::call_state<op_type> state{&operation, std::exception_ptr()};
    SEXP result = Rcpp::unwindProtect(&qx_unwind_detail::call_body<op_type>, &state);
    if (state.error) {
        std::rethrow_exception(state.error);
    }
    return result;
}

template <typename io_owner_type, typename operation_type>
inline SEXP qx_with_unwind_cleanup(io_owner_type& io_owner, operation_type&& protected_operation, const char* cleanup_warning = nullptr) {
    try {
        return qx_unwind_protect(std::forward<operation_type>(protected_operation));
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
