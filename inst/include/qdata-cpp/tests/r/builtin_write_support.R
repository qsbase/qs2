args <- commandArgs(TRUE)
writer_exe <- normalizePath(args[[1]], winslash = "/", mustWork = TRUE)
outdir <- normalizePath(args[[2]], winslash = "/", mustWork = FALSE)

stopifnot(requireNamespace("qs2", quietly = TRUE))

dir.create(outdir, recursive = TRUE, showWarnings = FALSE)

status <- system2(writer_exe, outdir)
if (!identical(status, 0L)) {
  stop("built-in writer helper failed")
}

read_case <- function(name) {
  qs2::qd_read(file.path(outdir, paste0(name, ".qdata")), use_alt_rep = FALSE, nthreads = 1)
}

expected <- list(
  logical_core = c(TRUE, FALSE, NA),
  logical_bool = c(TRUE, FALSE, TRUE),
  logical_optional = c(TRUE, NA, FALSE),
  integer_core = c(1L, NA_integer_, 3L),
  integer_int32 = c(1L, 2L, 3L),
  integer_int = c(4L, 5L, 6L),
  integer_optional = c(7L, NA_integer_, 9L),
  real_core = c(1.5, 2.5),
  real_double = c(3.25, 4.5),
  real_float = c(5.25, 6.5),
  complex_core = c(1 + 2i, 3 + 4i),
  complex_std = c(5 + 6i, 7 + 8i),
  string_core = c("alpha", NA_character_, ""),
  string_std = c("beta", "gamma", ""),
  string_optional = c("delta", NA_character_, "epsilon"),
  string_view = c("theta", "lambda"),
  raw_core = as.raw(c(0x01, 0x7F, 0xFF)),
  raw_byte = as.raw(c(0x10, 0x20, 0x30)),
  raw_uint8 = as.raw(c(0x40, 0x50, 0x60)),
  list_core = list(c(10L, 20L), "zeta"),
  list_iterable = list(c(1L, 2L, 3L), c(4L, 5L)),
  list_writable = list(c("one", "two"), c(11L, 12L, 13L), "owned")
)

for (name in names(expected)) {
  value <- read_case(name)
  if (!identical(value, expected[[name]])) {
    stop("built-in write support mismatch for case: ", name)
  }
}

attributed <- read_case("integer_with_attrs")
attributed_values <- attributed
attributes(attributed_values) <- NULL
if (!identical(attributed_values, c(21L, 22L, 23L))) {
  stop("integer_with_attrs payload mismatch")
}
if (!identical(attr(attributed, "label"), "numbers")) {
  stop("integer_with_attrs label attribute mismatch")
}
if (!identical(attr(attributed, "groups"), list(c(1L, 2L), c(3L, 4L)))) {
  stop("integer_with_attrs groups attribute mismatch")
}

cat("qdata-cpp built-in write support tests passed\n")
