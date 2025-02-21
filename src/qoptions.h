#ifndef _QS2_QOPT_H_
#define _QS2_QOPT_H_

#include <Rcpp.h>
using namespace Rcpp;

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
