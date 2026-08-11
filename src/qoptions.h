#ifndef _QS2_QOPT_H_
#define _QS2_QOPT_H_

#include <Rcpp.h>
using namespace Rcpp;

// Whether the session's native encoding is UTF-8, used to decide whether
// CE_NATIVE strings need translating before they are written.
//
// Determined once, at package load, by .onLoad() in R/zzz.R -- not per call.
// Re-checking the locale for every string would cost far more than it could
// save, so a Sys.setlocale() partway through a session is not picked up until
// qs2 is reloaded. This replaces a value that used to be baked in at compile
// time by configure, which was wrong for any binary package whose build machine
// and user did not share a locale.
//
// Defaults to false, i.e. "assume translation is needed", which fails safe: in
// a UTF-8 locale R's own translateCharUTF8() short-circuits on native strings
// (needsTranslationUTF8 returns NT_NONE when utf8locale), so guessing wrong in
// this direction costs nothing, while the other direction would write native
// bytes tagged as UTF-8.
static bool qs2_utf8_locale = false;

// [[Rcpp::export(rng = false)]]
bool qs2_get_utf8_locale() {
  return qs2_utf8_locale;
}

// [[Rcpp::export(rng = false, invisible = true)]]
void qs2_set_utf8_locale(bool value) {
  qs2_utf8_locale = value;
}

// Static variables for qs2 options
static int qs2_compress_level = 3;
static bool qs2_shuffle = true;
static int qs2_nthreads = 1;
static bool qs2_validate_checksum = false;
static bool qs2_warn_unsupported_types = true;
static bool qs2_use_alt_rep = false;

// Get and set functions for compress_level
// [[Rcpp::export(rng = false)]]
int qs2_get_compress_level() {
  return qs2_compress_level;
}

// [[Rcpp::export(rng = false)]]
void qs2_set_compress_level(int value) {
  qs2_compress_level = value;
}

// Get and set functions for shuffle
// [[Rcpp::export(rng = false)]]
bool qs2_get_shuffle() {
  return qs2_shuffle;
}

// [[Rcpp::export(rng = false)]]
void qs2_set_shuffle(bool value) {
  qs2_shuffle = value;
}

// Get and set functions for nthreads
// [[Rcpp::export(rng = false)]]
int qs2_get_nthreads() {
  return qs2_nthreads;
}

// [[Rcpp::export(rng = false)]]
void qs2_set_nthreads(int value) {
  qs2_nthreads = value;
}

// Get and set functions for validate_checksum
// [[Rcpp::export(rng = false)]]
bool qs2_get_validate_checksum() {
  return qs2_validate_checksum;
}

// [[Rcpp::export(rng = false)]]
void qs2_set_validate_checksum(bool value) {
  qs2_validate_checksum = value;
}

// Get and set functions for warn_unsupported_types
// [[Rcpp::export(rng = false)]]
bool qs2_get_warn_unsupported_types() {
  return qs2_warn_unsupported_types;
}

// [[Rcpp::export(rng = false)]]
void qs2_set_warn_unsupported_types(bool value) {
  qs2_warn_unsupported_types = value;
}

// Get and set functions for use_alt_rep
// [[Rcpp::export(rng = false)]]
bool qs2_get_use_alt_rep() {
  return qs2_use_alt_rep;
}

// [[Rcpp::export(rng = false)]]
void qs2_set_use_alt_rep(bool value) {
  qs2_use_alt_rep = value;
}

#endif
