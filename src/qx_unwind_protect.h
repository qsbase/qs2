#ifndef _QS2_QX_UNWIND_PROTECT_H_
#define _QS2_QX_UNWIND_PROTECT_H_

#include <Rcpp.h>
#include <csetjmp>
#include <exception>
#include <stdexcept>
#include <type_traits>
#include <utility>

// R still reports many errors and interrupts by jumping, so RAII alone is not
// enough. The jump lands in qx_unwind_protect's frame and skips every frame in
// between, so those frames must be trivially destructible. All non-trivial
// owners (block io, serializers, TBB state, scratch buffers) live in the
// caller's frame and are destroyed normally once the jump has been converted to
// Rcpp::LongjumpException. Rcpp's END_RCPP is what finally resumes the R unwind,
// so that exception type must be the one that escapes.
//
// https://yutani.rbind.io/post/r-rust-protect-and-unwinding/
// https://github.com/r-lib/cpp11/blob/51f4cd5ad9425a491dedf951a3679346d416e51c/vignettes/FAQ.Rmd#L287

// The continuation token for one exported call. Making it allocates, and an
// allocation failure is an R error that would skip any owner already alive, so
// an exported function creates its scope before its first owner, and every R
// call that can fail while the scope is alive goes through qx_unwind_protect.
// Calls outside protection, including any that can warn (an error under
// warn = 2) such as R_ExpandFileName and normalize_nthreads, go before the
// scope opens or after it closes.
//
// Rcpp::unwindProtect makes a fresh token on each call and preserves it again
// after a jump, both outside protection. Here the token is preserved up front,
// and a jump hands it to Rcpp, which releases it when it resumes the unwind.
class qx_unwind_scope {
public:
    qx_unwind_scope() : previous(current_scope()) {
        token = PROTECT(R_MakeUnwindCont());
        R_PreserveObject(token);
        UNPROTECT(1);
        current_scope() = this;
    }

    ~qx_unwind_scope() {
        current_scope() = previous;
        if (owns_token) {
            R_ReleaseObject(token);
        }
    }

    qx_unwind_scope(const qx_unwind_scope&) = delete;
    qx_unwind_scope& operator=(const qx_unwind_scope&) = delete;

    // Main thread only: workers never call R.
    static qx_unwind_scope& current() {
        qx_unwind_scope* scope = current_scope();
        if (scope == nullptr || !scope->owns_token || scope->active) {
            throw std::logic_error("qs2 internal error: unwind protection used without its own scope");
        }
        return *scope;
    }

    SEXP token;
    bool owns_token = true;
    // A nested call would share the token and overwrite a pending jump's value
    bool active = false;

private:
    qx_unwind_scope* const previous;  // R code run during a call may call qs2 again

    static qx_unwind_scope*& current_scope() {
        static qx_unwind_scope* scope = nullptr;
        return scope;
    }
};

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

struct jump_target {
    std::jmp_buf jmpbuf;
};

// R_UnwindProtect calls this from C frames, so leave them with longjmp and
// throw only once back in qx_unwind_protect.
inline void maybe_jump(void* data, Rboolean jump) {
    if (jump) {
        std::longjmp(static_cast<jump_target*>(data)->jmpbuf, 1);
    }
}

}  // namespace qx_unwind_detail

// C++ exceptions are held until after the boundary so they never cross R's C frames.
template <typename operation_type>
inline SEXP qx_unwind_protect(operation_type&& operation) {
    using op_type = typename std::remove_reference<operation_type>::type;
    static_assert(std::is_trivially_destructible<op_type>::value,
                  "unwind callback must be trivially destructible; an R jump skips its destructor");
    qx_unwind_scope& scope = qx_unwind_scope::current();
    qx_unwind_detail::call_state<op_type> state{&operation, std::exception_ptr()};
    qx_unwind_detail::jump_target target;
    if (setjmp(target.jmpbuf)) {
        scope.active = false;
        scope.owns_token = false;
        throw Rcpp::LongjumpException(scope.token);
    }
    scope.active = true;
    SEXP result = R_UnwindProtect(&qx_unwind_detail::call_body<op_type>, &state,
                                  &qx_unwind_detail::maybe_jump, &target, scope.token);
    scope.active = false;
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
