#ifndef _QS2_QX_STRING_ARG_H_
#define _QS2_QX_STRING_ARG_H_

// String arguments to exported functions arrive as SEXP rather than
// std::string. R still reports many errors and interrupts by jumping, and a
// jump steps over C++ frames without running destructors. Rcpp materializes a
// `const std::string&` parameter in the generated .Call wrapper, which sits in
// exactly such a frame, so any jump below it -- an R error, or a warning under
// options(warn = 2) -- leaks the string's buffer. A SEXP is trivially
// destructible, so the wrapper has nothing left to skip.

#include <Rinternals.h>

#include <stdexcept>
#include <string>

// The returned pointer belongs to the CHARSXP and stays valid for the duration
// of the call, since R protects the argument for it.
//
// Semantics match Rcpp::as<std::string>, which is also CHAR(STRING_ELT(x, 0))
// with no encoding translation, with one deliberate difference: NA is rejected
// rather than silently rendered as the text "NA", which would otherwise be
// accepted as a file name.
inline const char* qs2_as_single_string(SEXP x, const char* const arg_name) {
    if(TYPEOF(x) != STRSXP || Rf_xlength(x) != 1) {
        throw std::runtime_error(std::string(arg_name) + " must be a single character string");
    }
    SEXP element = STRING_ELT(x, 0);
    if(element == NA_STRING) {
        throw std::runtime_error(std::string(arg_name) + " must not be NA");
    }
    return CHAR(element);
}

// Byte length of a string already validated by qs2_as_single_string, for
// callers that need the size without taking a copy.
inline R_xlen_t qs2_single_string_length(SEXP x) {
    return XLENGTH(STRING_ELT(x, 0));
}

#endif
