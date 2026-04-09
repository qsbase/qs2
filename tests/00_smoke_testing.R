library(qs2)

cat("Running smoke tests...\n")

x <- generate_test_data(2048L, seed = 42L)
stopifnot(identical(names(x), c("string", "numeric", "int")))
stopifnot(nrow(x) == 2048L)

nthreads_to_test <- 1L
if (isTRUE(qs2:::check_TBB())) {
  nthreads_to_test <- c(nthreads_to_test, 4L)
}

for (nthreads in nthreads_to_test) {
  cat("Smoke test nthreads =", nthreads, "\n")

  tmp_qs <- tempfile(fileext = ".qs2")
  tmp_qd <- tempfile(fileext = ".qdata")

  qs_save(x, tmp_qs, compress_level = 1L, nthreads = nthreads)
  stopifnot(identical(qs_read(tmp_qs, validate_checksum = TRUE, nthreads = nthreads), x))
  stopifnot(identical(qs_deserialize(qs_serialize(x, compress_level = 1L, nthreads = nthreads),
                                     validate_checksum = TRUE,
                                     nthreads = nthreads), x))

  qd_save(x, tmp_qd, compress_level = 1L, nthreads = nthreads)
  stopifnot(identical(qd_read(tmp_qd, validate_checksum = TRUE, nthreads = nthreads), x))
  stopifnot(identical(qd_deserialize(qd_serialize(x, compress_level = 1L, nthreads = nthreads),
                                     validate_checksum = TRUE,
                                     nthreads = nthreads), x))
}

cat("Smoke tests completed.\n")
