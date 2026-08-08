total_time <- Sys.time()

args <- commandArgs(TRUE)
file_roundtrip_exe <- normalizePath(args[[1]], winslash = "/", mustWork = TRUE)
memory_roundtrip_exe <- normalizePath(args[[2]], winslash = "/", mustWork = TRUE)

stopifnot(requireNamespace("qs2", quietly = TRUE))
stopifnot(requireNamespace("stringi", quietly = TRUE))
stopifnot(requireNamespace("stringfish", quietly = TRUE))


options(warn = 1)

do_gc <- function() {
  gc(full=TRUE)
}

do_extended_tests <- toupper(Sys.getenv("QS_EXTENDED_TESTS", "0")) %in% c("1", "TRUE", "YES")
if (do_extended_tests) {
  cat("performing extended tests\n")
  reps <- 3
  internal_reps <- 2
  test_points <- c(0, 1, 2, 4, 8, 2^5 - 1, 2^5 + 1, 2^5, 2^8 - 1, 2^8 + 1, 2^8, 2^16 - 1, 2^16 + 1, 2^16, 1e6, 1e7)
  test_points_raw_vector <- sort(c(test_points, seq(2^19 - 1000, 2^19)))
  test_points_character_vector <- test_points
  max_size <- 1e7
  random_cl <- 19
} else {
  cat("performing basic tests\n")
  reps <- 1
  internal_reps <- 1
  test_points <- c(0, 1, 2, 4, 8, 2^5 - 1, 2^5, 2^5 + 1, 2^8 - 1, 2^8, 2^8 + 1, 2^12)
  test_points_raw_vector <- sort(unique(c(test_points, seq(2^12 - 32, 2^12 + 32))))
  test_points_character_vector <- c(0, 1, 2, 4, 8, 2^5 - 1, 2^5, 2^5 + 1, 2^8 - 1, 2^8, 2^8 + 1, 1024)
  max_size <- 1e5
  random_cl <- 9
}
if (qs2:::check_TBB() == FALSE) {
  cat("TBB not detected\n")
  random_threads <- 1
} else {
  cat("TBB detected\n")
  random_threads <- 5
}

obj_size <- 0
get_obj_size <- function() {
  get("obj_size", envir = globalenv())
}
set_obj_size <- function(x) {
  assign("obj_size", get_obj_size() + as.numeric(object.size(x)), envir = globalenv())
  get_obj_size()
}

string_pool_size <- max(test_points_character_vector)
cat("building string pool of size", format(string_pool_size, scientific = FALSE, trim = TRUE), "\n")
string_pool_time <- Sys.time()
string_pool <- stringfish::convert_to_sf(stringi::stri_rand_strings(string_pool_size, round(rexp(string_pool_size, 1 / 90))))
string_pool_time <- Sys.time() - string_pool_time
cat("string pool built in", format(signif(as.numeric(string_pool_time), 4), trim = TRUE), "seconds\n")
do_gc()

rand_strings <- function(n) {
  if (n == 0) {
    return(character(0))
  }
  string_pool[sample.int(length(string_pool), n, replace = TRUE)]
}

drop_selfref <- function(x) {
  attr(x, ".internal.selfref") <- NULL
  x
}

random_object_generator <- function(n) {
  ret <- as.list(seq_len(n))
  for (i in seq_len(n)) {
    if (get_obj_size() > get("max_size", envir = globalenv())) break
    otype <- sample(10, size = 1)
    z <- NULL
    is_attribute <- ifelse(i == 1, FALSE, sample(c(FALSE, TRUE), size = 1))
    if (otype == 1) { z <- rnorm(1e4); set_obj_size(z) }
    else if (otype == 2) { z <- sample(1e4) - 5e2; set_obj_size(z) }
    else if (otype == 3) { z <- sample(c(TRUE, FALSE, NA), size = 1e4, replace = TRUE); set_obj_size(z) }
    else if (otype == 4) { z <- as.raw(sample(256, size = 1e4, replace = TRUE) - 1); set_obj_size(z) }
    else if (otype == 5) { z <- replicate(sample(1e4, size = 1), paste(rep(letters, length.out = sample(10, size = 1)), collapse = "")); set_obj_size(z) }
    else if (otype == 6) { z <- paste(rep(letters, length.out = sample(1e4, size = 1)), collapse = ""); set_obj_size(z) }
    else if (otype == 7) { z <- as.list(runif(sample(10, size = 1))); set_obj_size(z) }
    else if (otype == 8) { z <- complex(real = rnorm(100), imaginary = runif(100)); set_obj_size(z) }
    else if (otype == 9) { z <- factor(sample(letters[1:5], 500, replace = TRUE), ordered = TRUE); set_obj_size(z) }
    else { z <- random_object_generator(4) }
    if (is_attribute) {
      attr(ret[[i - 1]], as.character(runif(1))) <- z
    } else {
      ret[[i]] <- z
    }
  }
  ret
}

nested_df <- function() {
  if (!requireNamespace("tibble", quietly = TRUE)) {
    return(NULL)
  }
  sub_df <- function(nr = 60, nc = 4) {
    cols <- lapply(seq_len(nc), function(i) rand_strings(nr))
    names(cols) <- make.unique(paste0(sample(letters, nc), rand_strings(nc)))
    tibble::as_tibble(cols)
  }
  tibble::tibble(
    col1 = rand_strings(100),
    col2 = rand_strings(100),
    col3 = lapply(seq_len(100), function(i) sub_df()),
    col4 = lapply(seq_len(100), function(i) sub_df())
  )
}

parse_qdata_timings <- function(output, expected_names) {
  timing_line <- grep("^QDATA_TIMINGS ", output, value = TRUE)
  if (length(timing_line) != 1L) {
    stop("failed to parse qdata-cpp timings:\n", paste(output, collapse = "\n"))
  }

  fields <- strsplit(sub("^QDATA_TIMINGS\\s+", "", timing_line), "\\s+")[[1]]
  parts <- strsplit(fields, "=", fixed = TRUE)
  timings <- setNames(
    vapply(parts, function(x) as.numeric(x[[2]]), numeric(1)),
    vapply(parts, `[[`, character(1), 1)
  )

  if (!setequal(names(timings), expected_names)) {
    stop("unexpected qdata-cpp timing keys:\n", paste(output, collapse = "\n"))
  }

  timings[expected_names]
}

print_test_timings <- function(test_format, label, value, timings) {
  cat(sprintf(
    "%s %s %s: %s\n",
    test_format,
    label,
    format(value, scientific = FALSE, trim = TRUE),
    paste(
      sprintf("%ss", format(signif(as.numeric(timings), 2), trim = TRUE)),
      collapse = " "
    )
  ))
  flush(stdout())
}

read_bytes <- function(path) {
  con <- file(path, "rb")
  on.exit(close(con), add = TRUE)
  readBin(con, what = "raw", n = file.info(path)$size)
}

run_roundtrip_file <- function(x, compress_level, nthreads, validate_checksum) {
  infile <- tempfile(fileext = ".qdata")
  outfile <- tempfile(fileext = ".qdata")
  on.exit(unlink(c(infile, outfile)), add = TRUE)

  qs2_save_time <- system.time(
    qs2::qd_save(x, infile, compress_level = compress_level, nthreads = nthreads)
  )[["elapsed"]]
  output <- system2(
    file_roundtrip_exe,
    c(infile, outfile, as.character(compress_level), "1", as.integer(validate_checksum), as.character(nthreads), "--timings"),
    stdout = TRUE,
    stderr = TRUE
  )
  status <- attr(output, "status")
  if (is.null(status)) {
    status <- 0L
  }
  if (!identical(as.integer(status), 0L)) {
    stop("qdata-cpp file roundtrip failed:\n", paste(output, collapse = "\n"))
  }
  cpp_timings <- parse_qdata_timings(output, c("qdata-cpp-read", "qdata-cpp-save"))

  qs2_read_time <- system.time(
    restored <- qs2::qd_read(
      outfile,
      use_alt_rep = sample(c(TRUE, FALSE), 1),
      validate_checksum = validate_checksum,
      nthreads = nthreads
    )
  )[["elapsed"]]

  list(
    value = restored,
    timings = c(
      "qs2-save" = qs2_save_time,
      cpp_timings,
      "qs2-read" = qs2_read_time
    )
  )
}

roundtrip_case_exact <- function(name, x) {
  infile <- tempfile(pattern = paste0("qdata_in_", name, "_"), fileext = ".qdata")
  outfile <- tempfile(pattern = paste0("qdata_out_", name, "_"), fileext = ".qdata")
  on.exit(unlink(c(infile, outfile)), add = TRUE)

  qs2::qd_save(x, infile, compress_level = 3, shuffle = TRUE, nthreads = 1)
  status <- system2(file_roundtrip_exe, c(infile, outfile, "3", "1", "0", "1"))
  stopifnot(identical(status, 0L))

  before <- read_bytes(infile)
  after <- read_bytes(outfile)
  stopifnot(identical(before, after))

  restored <- qs2::qd_read(outfile, use_alt_rep = FALSE, nthreads = 1)
  stopifnot(identical(restored, x))
}

run_roundtrip_memory <- function(x, compress_level, nthreads, validate_checksum) {
  infile <- tempfile(fileext = ".bin")
  outfile <- tempfile(fileext = ".bin")
  on.exit(unlink(c(infile, outfile)), add = TRUE)

  qs2_serialize_time <- system.time(
    input <- qs2::qd_serialize(x, compress_level = compress_level, nthreads = nthreads)
  )[["elapsed"]]
  con <- file(infile, "wb")
  writeBin(input, con)
  close(con)

  output <- system2(
    memory_roundtrip_exe,
    c(infile, outfile, as.character(compress_level), "1", as.integer(validate_checksum), as.character(nthreads), "--timings"),
    stdout = TRUE,
    stderr = TRUE
  )
  status <- attr(output, "status")
  if (is.null(status)) {
    status <- 0L
  }
  if (!identical(as.integer(status), 0L)) {
    stop("qdata-cpp memory roundtrip failed:\n", paste(output, collapse = "\n"))
  }
  cpp_timings <- parse_qdata_timings(output, c("qdata-cpp-deserialize", "qdata-cpp-serialize"))

  con_out <- file(outfile, "rb")
  output <- readBin(con_out, what = "raw", n = file.info(outfile)$size)
  close(con_out)

  qs2_deserialize_time <- system.time(
    restored <- qs2::qd_deserialize(
      output,
      use_alt_rep = sample(c(TRUE, FALSE), 1),
      validate_checksum = validate_checksum,
      nthreads = nthreads
    )
  )[["elapsed"]]

  list(
    value = restored,
    timings = c(
      "qs2-serialize" = qs2_serialize_time,
      cpp_timings,
      "qs2-deserialize" = qs2_deserialize_time
    )
  )
}

qdata_roundtrip_rand <- function(x, format) {
  nthreads <- sample.int(random_threads, 1)
  compress_level <- sample.int(random_cl, 1)
  validate_checksum <- sample(c(TRUE, FALSE), 1)

  if (format == "qdata") {
    run_roundtrip_file(x, compress_level, nthreads, validate_checksum)
  } else if (format == "qdata_memory") {
    run_roundtrip_memory(x, compress_level, nthreads, validate_checksum)
  } else {
    stop("unsupported format: ", format)
  }
}

tmp <- tempfile(fileext = ".qd")
x <- complex(real = c(1, 2), imaginary = c(3, 4))
attr(x, "note") <- "test"
restored <- run_roundtrip_file(x, 3L, 1L, TRUE)
stopifnot(identical(restored$value, x))

latin1_text <- iconv("façile", from = "UTF-8", to = "latin1")
Encoding(latin1_text) <- "latin1"

raw_boundary <- function(n) {
  x <- list(raw(n), a = "a")
  names(x) <- c("payload", "label")
  x
}

string_boundary <- function(n) {
  strrep("x", n)
}

list_string_boundary <- function(n) {
  list(
    left = string_boundary(n),
    right = string_boundary(32L)
  )
}

exact_cases <- list(
  strings = c(NA_character_, "", paste(rep(letters, length.out = 1000), collapse = "")),
  string_boundary_1 = string_boundary(2^19 - 1),
  string_boundary_2 = string_boundary(2^19),
  string_boundary_3 = string_boundary(2^19 + 1),
  raw_boundary_1 = raw_boundary(2^19 - 1),
  raw_boundary_2 = raw_boundary(2^19),
  raw_boundary_3 = raw_boundary(2^19 + 1),
  list_string_boundary_1 = list_string_boundary(2^19 - 1),
  list_string_boundary_2 = list_string_boundary(2^19),
  list_string_boundary_3 = list_string_boundary(2^19 + 1),
  character_vector = c(NA_character_, "", sprintf("s%03d", seq_len(100))),
  integer_vector = c(NA_integer_, seq_len(1024L)),
  numeric_vector = c(pi, NaN, Inf, -Inf, seq(0, 1, length.out = 32)),
  logical_vector = c(TRUE, FALSE, NA),
  factor = factor(c("low", "high", "low"), levels = c("low", "high")),
  encoding = c("plain", latin1_text),
  matrix = matrix(1:6, nrow = 2),
  mtcars = mtcars
)

complex_attr_exact <- complex(real = c(1, 2), imaginary = c(3, 4))
attr(complex_attr_exact, "note") <- "test"
exact_cases$complex_with_attr <- complex_attr_exact

nested_attr_exact <- list(a = 1:3, b = c("x", "y"))
attr(nested_attr_exact, "meta") <- list(c(TRUE, NA, FALSE), c("left", "right"))
exact_cases$nested_list <- nested_attr_exact

for (name in names(exact_cases)) {
  roundtrip_case_exact(name, exact_cases[[name]])
}

for (format in c("qdata_memory", "qdata")) {
for (q in seq_len(reps)) {
  cat("########################################\n")
  cat("Format", format, "rep", q, "of", reps, "\n")

  for (idx in seq_along(test_points)) {
    tp <- test_points[[idx]]
    timings <- vector("list", length = internal_reps)
    for (i in seq_len(internal_reps)) {
      x1 <- c(NA_character_, "", paste(rep(letters, length.out = tp), collapse = ""))
      z <- qdata_roundtrip_rand(x1, format)
      timings[[i]] <- z$timings
      do_gc()
      stopifnot(identical(z$value, x1))
    }
    print_test_timings(format, "strings", tp, colMeans(do.call(rbind, timings)))
  }

  for (idx in seq_along(test_points_raw_vector)) {
    tp <- test_points_raw_vector[[idx]]
    timings <- vector("list", length = internal_reps)
    for (i in seq_len(internal_reps)) {
      x1 <- list(raw(tp), a = "a")
      z <- qdata_roundtrip_rand(x1, format)
      timings[[i]] <- z$timings
      do_gc()
      stopifnot(identical(z$value, x1))
    }
    print_test_timings(format, "Raw vector", tp, colMeans(do.call(rbind, timings)))
  }

  for (idx in seq_along(test_points_character_vector)) {
    tp <- test_points_character_vector[[idx]]
    timings <- vector("list", length = internal_reps)
    for (i in seq_len(internal_reps)) {
      x1 <- c(NA_character_, "", rand_strings(tp))
      z <- qdata_roundtrip_rand(x1, format)
      timings[[i]] <- z$timings
      do_gc()
      stopifnot(identical(z$value, x1))
    }
    print_test_timings(format, "Character vectors", tp, colMeans(do.call(rbind, timings)))
  }

  for (idx in seq_along(test_points)) {
    tp <- test_points[[idx]]
    timings <- vector("list", length = internal_reps)
    for (i in seq_len(internal_reps)) {
      x1 <- c(NA_integer_, sample.int(max(1, tp), size = tp, replace = TRUE))
      z <- qdata_roundtrip_rand(x1, format)
      timings[[i]] <- z$timings
      do_gc()
      stopifnot(identical(z$value, x1))
    }
    print_test_timings(format, "Integers", tp, colMeans(do.call(rbind, timings)))
  }

  for (idx in seq_along(test_points)) {
    tp <- test_points[[idx]]
    timings <- vector("list", length = internal_reps)
    for (i in seq_len(internal_reps)) {
      x1 <- c(NA_real_, rnorm(tp))
      z <- qdata_roundtrip_rand(x1, format)
      timings[[i]] <- z$timings
      do_gc()
      stopifnot(identical(z$value, x1))
    }
    print_test_timings(format, "Numeric", tp, colMeans(do.call(rbind, timings)))
  }

  for (idx in seq_along(test_points)) {
    tp <- test_points[[idx]]
    timings <- vector("list", length = internal_reps)
    for (i in seq_len(internal_reps)) {
      x1 <- sample(c(TRUE, FALSE, NA), replace = TRUE, size = tp)
      z <- qdata_roundtrip_rand(x1, format)
      timings[[i]] <- z$timings
      do_gc()
      stopifnot(identical(z$value, x1))
    }
    print_test_timings(format, "Logical", tp, colMeans(do.call(rbind, timings)))
  }

  for (idx in seq_along(test_points)) {
    tp <- test_points[[idx]]
    timings <- vector("list", length = internal_reps)
    for (i in seq_len(internal_reps)) {
      x1 <- as.list(runif(tp))
      z <- qdata_roundtrip_rand(x1, format)
      timings[[i]] <- z$timings
      do_gc()
      stopifnot(identical(z$value, x1))
    }
    print_test_timings(format, "List", tp, colMeans(do.call(rbind, timings)))
  }

  for (idx in seq_along(test_points)) {
    tp <- test_points[[idx]]
    timings <- vector("list", length = internal_reps)
    for (i in seq_len(internal_reps)) {
      x1 <- c(NA_complex_, complex(real = rnorm(tp), imaginary = runif(tp)))
      z <- qdata_roundtrip_rand(x1, format)
      timings[[i]] <- z$timings
      do_gc()
      stopifnot(identical(z$value, x1))
    }
    print_test_timings(format, "Complex", tp, colMeans(do.call(rbind, timings)))
  }

  timings <- vector("list", length = internal_reps)
  for (i in seq_len(internal_reps)) {
    x1 <- mtcars
    z <- qdata_roundtrip_rand(x1, format)
    timings[[i]] <- z$timings
    do_gc()
    stopifnot(identical(z$value, x1))
  }
  print_test_timings(format, "Data.frame", nrow(x1), colMeans(do.call(rbind, timings)))

  if (!do_extended_tests) {
    next
  }

  if (requireNamespace("data.table", quietly = TRUE)) {
    timings <- vector("list", length = internal_reps)
    for (i in seq_len(internal_reps)) {
      x1 <- data.table::data.table(str = rand_strings(2e4), num = runif(2e4))
      z <- qdata_roundtrip_rand(x1, format)
      timings[[i]] <- z$timings
      do_gc()
      # .internal.selfref is an externalptr, i.e. an address in this process, so
      # qdata drops it by design. Compare without it rather than weakening the
      # test to a value-only check.
      stopifnot(identical(drop_selfref(z$value), drop_selfref(x1)))
    }
    print_test_timings(format, "Data.table", nrow(x1), colMeans(do.call(rbind, timings)))
  }

  if (requireNamespace("tibble", quietly = TRUE)) {
    timings <- vector("list", length = internal_reps)
    for (i in seq_len(internal_reps)) {
      x1 <- tibble::tibble(str = rand_strings(2e4), num = runif(2e4))
      z <- qdata_roundtrip_rand(x1, format)
      timings[[i]] <- z$timings
      do_gc()
      stopifnot(identical(z$value, x1))
    }
    print_test_timings(format, "Tibble", nrow(x1), colMeans(do.call(rbind, timings)))
  }

  if (Sys.info()[["sysname"]] != "Windows") {
    timings <- vector("list", length = internal_reps)
    for (i in seq_len(internal_reps)) {
      x1 <- c(iconv(latin1_text, "latin1", "UTF-8"), latin1_text)
      z <- qdata_roundtrip_rand(x1, format)
      timings[[i]] <- z$timings
      do_gc()
      stopifnot(identical(z$value, x1))
    }
    print_test_timings(format, "Encoding", length(x1), colMeans(do.call(rbind, timings)))
  } else {
    cat("(Encoding test not run on windows)\n")
  }

  for (idx in seq_along(test_points)) {
    tp <- test_points[[idx]]
    timings <- vector("list", length = internal_reps)
    for (i in seq_len(internal_reps)) {
      x1 <- factor(rep(letters, length.out = tp), levels = sample(letters), ordered = TRUE)
      z <- qdata_roundtrip_rand(x1, format)
      timings[[i]] <- z$timings
      do_gc()
      stopifnot(identical(z$value, x1))
    }
    print_test_timings(format, "Factors", tp, colMeans(do.call(rbind, timings)))
  }

  for (i in seq_len(8)) {
    obj_size <- 0
    x1 <- random_object_generator(12)
    z <- qdata_roundtrip_rand(x1, format)
    do_gc()
    stopifnot(identical(z$value, x1))
    print_test_timings(format, "Random objects", as.numeric(object.size(x1)), z$timings)
  }

  timings <- vector("list", length = internal_reps)
  for (i in seq_len(internal_reps)) {
    x1 <- as.list(1:26)
    attr(x1[[26]], letters[26]) <- rnorm(100)
    for (j in 25:1) {
      attr(x1[[j]], letters[j]) <- x1[[j + 1]]
    }
    z <- qdata_roundtrip_rand(x1, format)
    timings[[i]] <- z$timings
    do_gc()
    stopifnot(identical(z$value, x1))
  }
  print_test_timings(format, "Nested attributes", length(x1), colMeans(do.call(rbind, timings)))

  x1 <- nested_df()
  if (!is.null(x1)) {
    timings <- vector("list", length = internal_reps)
    for (i in seq_len(internal_reps)) {
      z <- qdata_roundtrip_rand(x1, format)
      timings[[i]] <- z$timings
      do_gc()
      stopifnot(identical(z$value, x1))
    }
    print_test_timings(format, "Nested tibble", nrow(x1), colMeans(do.call(rbind, timings)))
  }
}
}

cat("qdata-cpp roundtrip_testing.R passed in", signif(difftime(Sys.time(), total_time, units = "secs"), 4), "seconds\n")
