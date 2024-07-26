#ifndef _QS2_QS_UNWIND_PROTECT_H_
#define _QS2_QS_UNWIND_PROTECT_H_

#include <Rcpp.h>
#include <csetjmp>

#define SINGLE_ARG(...) __VA_ARGS__



// struct Guardian {
//     int x;
//     Guardian() : x(0) {}
//     ~Guardian() {
//         std::cout << "Guardian destructor" << std::endl;
//     }
// };

// template <typename io_type>
// inline void throw_unwind(void * args, Rboolean jump) {
//     if(jump) {
//         std::pair<SEXP, void *> * throw_unwind_args = static_cast<std::pair<SEXP, void *>*>(args);
//         // std::tuple<SEXP, Guardian*, io_type*> * throw_unwind_args = static_cast<std::tuple<SEXP, Guardian*, io_type*>*>(args);
//         io_type * io = throw_unwind_args->second;
//         Guardian * jump_guardian = std::get<1>( *throw_unwind_args );
//         io->cleanup();
//         throw RerrorUnwind{throw_unwind_args->first};
//     }
// }

struct RerrorUnwind {
    SEXP cont;
};


#define UNWIND_PROTECT_BEGIN() \
    Rcpp::RObject cont_token = R_MakeUnwindCont(); \
    try {

#define UNWIND_PROTECT_END() \
    } catch(RerrorUnwind & cont) { R_ContinueUnwind(cont.cont); } \
    return R_NilValue; // unreachable

#define DO_JMPBUF() \
    std::jmp_buf jmpbuf; \
    if (setjmp(jmpbuf)) { \
        block_io.cleanup(); \
        throw RerrorUnwind{cont_token}; \
    } \


#define DO_UNWIND_PROTECT(_FUNCTION_, _IO_TYPE_, _ARGS_) \
    SEXP output = R_UnwindProtect(_FUNCTION_ < _IO_TYPE_ >, (void*)(& _ARGS_), [](void* jmpbuf, Rboolean jump) { \
        if (jump == TRUE) { \
            longjmp(*static_cast<std::jmp_buf*>(jmpbuf), 1); \
        } \
    }, &jmpbuf, static_cast<SEXP>(cont_token)); \
    return output

#endif

// Notes to future self on current understanding of how this works / is supposed to work

// unwind signature
// SEXP R_UnwindProtect(SEXP (*fun)(void *data), void *data,
//                      void (*cleanfun)(void *data, Rboolean jump),
//                      void *cleandata, SEXP cont);

// for R_serialize / R_unserialize, any error or keyboard interrupt is handled by R's internal error handling
// which jumps directly outside of the function, leaving RAII destructors un-called and memory leaking
// R_UnwindProtect gives us a chance to go back to the C++ code, throw an error there (which calls RAII destructors)
// Then R_ContinueUnwind will jump back to R's internal error handling

// For multithreading, we also need to call block_io.cleanup() which stops the async TBB graph that is currently running
// If we try to do RAII destruction before the graph is stopped it will crash (segfault)
// Possibly, we can write our own destructor to handle this

// According to cpp11, we cannot throw directly from "C frame" (cleanfun) and instead need to do a std::jmp_buf in cleanfun
// back to the "C++ frame"
// In some ways this is easier as we don't need to pass objects that need to be cleaned up to cleanfun / cleandata

// We don't need this for our own methods (qdata format) using C++/Rcpp only

// References
// https://yutani.rbind.io/post/r-rust-protect-and-unwinding/
// https://github.com/r-lib/cpp11/blob/51f4cd5ad9425a491dedf951a3679346d416e51c/vignettes/FAQ.Rmd#L287
