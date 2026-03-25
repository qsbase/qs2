#ifndef QS2_EXTERNAL_H
#define QS2_EXTERNAL_H

#include <R.h>
#include <Rinternals.h>
#include <Rdefines.h>
#include <Rconfig.h>
#include <R_ext/Rdynload.h>
#include <stdint.h>
#include <string>

#ifdef __cplusplus
extern "C" {
#endif

inline SEXP qs_save(SEXP object, const std::string& file, const int compress_level = 3, const bool shuffle = true, const int nthreads = 1) {
  static SEXP (*fun)(SEXP, const std::string&, const int, const bool, int) =
    (SEXP (*)(SEXP, const std::string&, const int, const bool, int)) R_GetCCallable("qs2", "qs_save");
  return fun(object, file, compress_level, shuffle, nthreads);
}
inline SEXP qs_serialize(SEXP object, const int compress_level = 3, const bool shuffle = true, const int nthreads = 1) {
  static SEXP (*fun)(SEXP, const int, const bool, int) =
    (SEXP (*)(SEXP, const int, const bool, int)) R_GetCCallable("qs2", "qs_serialize");
  return fun(object, compress_level, shuffle, nthreads);
}
inline SEXP qs_read(const std::string& file, const bool validate_checksum = false, const int nthreads = 1) {
  static SEXP (*fun)(const std::string&, const bool, int) =
    (SEXP (*)(const std::string&, const bool, int)) R_GetCCallable("qs2", "qs_read");
  return fun(file, validate_checksum, nthreads);
}
inline SEXP qs_deserialize(SEXP input, const bool validate_checksum = false, const int nthreads = 1) {
  static SEXP (*fun)(SEXP, const bool, int) =
    (SEXP (*)(SEXP, const bool, int)) R_GetCCallable("qs2", "qs_deserialize");
  return fun(input, validate_checksum, nthreads);
}

inline SEXP qd_save(SEXP object, const std::string& file, const int compress_level = 3, const bool shuffle = true, const bool warn_unsupported_types = true, const int nthreads = 1) {
  static SEXP (*fun)(SEXP, const std::string&, const int, const bool, const bool, int) =
    (SEXP (*)(SEXP, const std::string&, const int, const bool, const bool, int)) R_GetCCallable("qs2", "qd_save");
  return fun(object, file, compress_level, shuffle, warn_unsupported_types, nthreads);
}
inline SEXP qd_serialize(SEXP object, const int compress_level = 3, const bool shuffle = true, const bool warn_unsupported_types = true, const int nthreads = 1) {
  static SEXP (*fun)(SEXP, const int, const bool, const bool, int) =
    (SEXP (*)(SEXP, const int, const bool, const bool, int)) R_GetCCallable("qs2", "qd_serialize");
  return fun(object, compress_level, shuffle, warn_unsupported_types, nthreads);
}
inline SEXP qd_read(const std::string& file, const bool use_alt_rep = false, const bool validate_checksum = false, const int nthreads = 1) {
  static SEXP (*fun)(const std::string&, const bool, const bool, int) =
    (SEXP (*)(const std::string&, const bool, const bool, int)) R_GetCCallable("qs2", "qd_read");
  return fun(file, use_alt_rep, validate_checksum, nthreads);
}
inline SEXP qd_deserialize(SEXP input, const bool use_alt_rep = false, const bool validate_checksum = false, const int nthreads = 1) {
  static SEXP (*fun)(SEXP, const bool, const bool, int) =
    (SEXP (*)(SEXP, const bool, const bool, int)) R_GetCCallable("qs2", "qd_deserialize");
  return fun(input, use_alt_rep, validate_checksum, nthreads);
}

inline int qs2_get_compress_level() {
  static int (*fun)() = (int (*)()) R_GetCCallable("qs2", "qs2_get_compress_level");
  return fun();
}
inline void qs2_set_compress_level(int value) {
  static void (*fun)(int) = (void (*)(int)) R_GetCCallable("qs2", "qs2_set_compress_level");
  fun(value);
}
inline bool qs2_get_shuffle() {
  static bool (*fun)() = (bool (*)()) R_GetCCallable("qs2", "qs2_get_shuffle");
  return fun();
}
inline void qs2_set_shuffle(bool value) {
  static void (*fun)(bool) = (void (*)(bool)) R_GetCCallable("qs2", "qs2_set_shuffle");
  fun(value);
}
inline int qs2_get_nthreads() {
  static int (*fun)() = (int (*)()) R_GetCCallable("qs2", "qs2_get_nthreads");
  return fun();
}
inline void qs2_set_nthreads(int value) {
  static void (*fun)(int) = (void (*)(int)) R_GetCCallable("qs2", "qs2_set_nthreads");
  fun(value);
}
inline bool qs2_get_validate_checksum() {
  static bool (*fun)() = (bool (*)()) R_GetCCallable("qs2", "qs2_get_validate_checksum");
  return fun();
}
inline void qs2_set_validate_checksum(bool value) {
  static void (*fun)(bool) = (void (*)(bool)) R_GetCCallable("qs2", "qs2_set_validate_checksum");
  fun(value);
}
inline bool qs2_get_warn_unsupported_types() {
  static bool (*fun)() = (bool (*)()) R_GetCCallable("qs2", "qs2_get_warn_unsupported_types");
  return fun();
}
inline void qs2_set_warn_unsupported_types(bool value) {
  static void (*fun)(bool) = (void (*)(bool)) R_GetCCallable("qs2", "qs2_set_warn_unsupported_types");
  fun(value);
}
inline bool qs2_get_use_alt_rep() {
  static bool (*fun)() = (bool (*)()) R_GetCCallable("qs2", "qs2_get_use_alt_rep");
  return fun();
}
inline void qs2_set_use_alt_rep(bool value) {
  static void (*fun)(bool) = (void (*)(bool)) R_GetCCallable("qs2", "qs2_set_use_alt_rep");
  fun(value);
}

#ifdef __cplusplus
}
#endif

#endif // include guard
