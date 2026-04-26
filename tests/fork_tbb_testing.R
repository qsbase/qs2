library(qs2)
library(parallel)

cat("Testing fork and PSOCK TBB behavior...\n")

if (isTRUE(qs2:::check_TBB())) {
  x <- generate_test_data(3e5, seed = 20260425L)
  tmp_qd <- tempfile(fileext = ".qdata")
  on.exit(unlink(tmp_qd), add = TRUE)
  qd_save(x, tmp_qd, compress_level = 1L, nthreads = 1L)

  if (.Platform$OS.type != "windows") {
    fork_results <- mclapply(seq_len(2L), function(i) {
      identical(qs2::qd_read(tmp_qd, validate_checksum = TRUE, nthreads = 2L), x)
    }, mc.cores = 2L)
    stopifnot(all(unlist(fork_results, use.names = FALSE)))
  }

  cl <- makeCluster(2L)
  psock_results <- tryCatch({
    clusterExport(cl, c("tmp_qd", "x"))
    clusterEvalQ(cl, library(qs2))
    clusterEvalQ(cl, identical(qs2::qd_read(tmp_qd, validate_checksum = TRUE, nthreads = 2L), x))
  }, finally = stopCluster(cl))
  stopifnot(all(unlist(psock_results, use.names = FALSE)))
} else {
  cat("Skipping fork and PSOCK TBB tests because TBB is not available.\n")
}

cat("Fork and PSOCK TBB tests completed.\n")
