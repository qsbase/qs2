args <- commandArgs(TRUE)
outfile <- if (length(args) >= 1L) args[[1]] else "mtcars.qdata"

stopifnot(requireNamespace("qs2", quietly = TRUE))

qs2::qd_save(mtcars, outfile, nthreads = 1)
cat("wrote", outfile, "\n")
