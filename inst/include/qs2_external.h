#ifndef QS2_EXTERNAL_H
#define QS2_EXTERNAL_H

#include <R.h>
#include <Rinternals.h>
#include <Rdefines.h>
#include <Rconfig.h>
#include <R_ext/Rdynload.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

char * c_qs_serialize(SEXP object, uint64_t * len, const int compress_level = 3, const bool shuffle = true, const int nthreads = 1) {
  static char *(*fun)(SEXP, uint64_t *, const int, const bool, const int) = (char *(*)(SEXP, uint64_t *, const int, const bool, const int)) R_GetCCallable("qs2", "c_qs_serialize");
  return fun(object, len, compress_level, shuffle, nthreads);
}
SEXP c_qs_deserialize(const char * buffer, const uint64_t len, const bool validate_checksum = false, const int nthreads = 1) {
  static SEXP(*fun)(const char *, const uint64_t, const bool, const int) = (SEXP(*)(const char *, const uint64_t, const bool, const int)) R_GetCCallable("qs2", "c_qs_deserialize");
  return fun(buffer, len, validate_checksum, nthreads);
}
char * c_qd_serialize(SEXP object, uint64_t * len, const int compress_level = 3, const bool shuffle = true, const bool warn_unsupported_types = true, const int nthreads = 1) {
  static char *(*fun)(SEXP, uint64_t *, const int, const bool, const bool, const int) = (char *(*)(SEXP, uint64_t *, const int, const bool, const bool, const int)) R_GetCCallable("qs2", "c_qd_serialize");
  return fun(object, len, compress_level, shuffle, warn_unsupported_types, nthreads);
}
SEXP c_qd_deserialize(const char * buffer, const uint64_t len, const bool use_alt_rep = false, const bool validate_checksum = false, const int nthreads = 1) {
  static SEXP(*fun)(const char *, const uint64_t, const bool, const bool, const int) = (SEXP(*)(const char *, const uint64_t, const bool, const bool, const int)) R_GetCCallable("qs2", "c_qd_deserialize");
  return fun(buffer, len, use_alt_rep, validate_checksum, nthreads);
}

#ifdef __cplusplus
}
#endif

#endif // include guard