library(qs2)

cat("Testing qs2_external.h C++ overloads...\n")

stopifnot(requireNamespace("Rcpp", quietly = TRUE))

include_dir <- system.file("include", package = "qs2")
stopifnot(nzchar(include_dir))
stopifnot(file.exists(file.path(include_dir, "qs2_external.h")))

old_r_tests <- Sys.getenv("R_TESTS", unset = NA_character_)
on.exit({
  if (is.na(old_r_tests)) {
    Sys.unsetenv("R_TESTS")
  } else {
    Sys.setenv(R_TESTS = old_r_tests)
  }
}, add = TRUE)

if (!is.na(old_r_tests) && nzchar(old_r_tests)) {
  normalized_r_tests <- tryCatch(
    normalizePath(old_r_tests, winslash = "/", mustWork = TRUE),
    error = function(...) old_r_tests
  )
  Sys.setenv(R_TESTS = normalized_r_tests)
}

source_file <- tempfile("qs2-external-", fileext = ".cpp")
paths <- tempfile(c("qs-string-", "qd-string-", "qs-sexp-", "qd-sexp-"))
on.exit(unlink(c(source_file, paths)), add = TRUE)

probe_source <- paste(
  "// [[Rcpp::depends(qs2)]]",
  "// [[Rcpp::plugins(cpp17)]]",
  "#include <Rcpp.h>",
  "#include <string>",
  "",
  "#include \"qs2_external.h\"",
  "",
  "SEXP make_result(SEXP qs_result, SEXP qd_result) {",
  "    SEXP result = PROTECT(Rf_allocVector(VECSXP, 2));",
  "    SET_VECTOR_ELT(result, 0, qs_result);",
  "    SET_VECTOR_ELT(result, 1, qd_result);",
  "    UNPROTECT(1);",
  "    return result;",
  "}",
  "",
  "// [[Rcpp::export]]",
  "SEXP qs2_external_string_probe(SEXP object, const std::string& qs_file,",
  "                                const std::string& qd_file) {",
  "    qs_save(object, qs_file, 1, false, 1);",
  "    SEXP qs_result = PROTECT(qs_read(qs_file, true, 1));",
  "    qd_save(object, qd_file, 1, false, true, 1);",
  "    SEXP qd_result = PROTECT(qd_read(qd_file, false, true, 1));",
  "    SEXP result = make_result(qs_result, qd_result);",
  "    UNPROTECT(2);",
  "    return result;",
  "}",
  "",
  "// [[Rcpp::export]]",
  "SEXP qs2_external_sexp_probe(SEXP object, SEXP qs_file, SEXP qd_file) {",
  "    qs_save(object, qs_file, 1, false, 1);",
  "    SEXP qs_result = PROTECT(qs_read(qs_file, true, 1));",
  "    qd_save(object, qd_file, 1, false, true, 1);",
  "    SEXP qd_result = PROTECT(qd_read(qd_file, false, true, 1));",
  "    SEXP result = make_result(qs_result, qd_result);",
  "    UNPROTECT(2);",
  "    return result;",
  "}",
  sep = "\n"
)

writeLines(probe_source, source_file, useBytes = TRUE)
Rcpp::sourceCpp(file = source_file, rebuild = TRUE, showOutput = TRUE)

object <- list(integer = c(1L, NA_integer_, 3L), text = c("one", NA, "three"))
string_result <- qs2_external_string_probe(object, paths[[1L]], paths[[2L]])
sexp_result <- qs2_external_sexp_probe(object, paths[[3L]], paths[[4L]])

stopifnot(
  identical(string_result[[1L]], object),
  identical(string_result[[2L]], object),
  identical(sexp_result[[1L]], object),
  identical(sexp_result[[2L]], object)
)

cat("qs2_external.h tests completed.\n")
