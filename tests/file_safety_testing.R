library(qs2)

cat("Testing file collision and cleanup safety...\n")

local({
  paths <- character()
  new_path <- function(fileext = "") {
    path <- tempfile(fileext = fileext)
    paths <<- c(paths, path)
    path
  }
  on.exit(unlink(paths), add = TRUE)

  read_raw_file <- function(path) {
    readBin(path, "raw", n = file.info(path)$size)
  }
  errors <- function(expr) {
    inherits(try(force(expr), silent = TRUE), "try-error")
  }

  payload <- as.raw(rep(1:100, 20L))
  input <- new_path()
  writeBin(payload, input)

  stopifnot(errors(zstd_compress_file(input, input, compress_level = 1L)))
  stopifnot(identical(read_raw_file(input), payload))

  dotted_input <- file.path(dirname(input), ".", basename(input))
  stopifnot(errors(zstd_compress_file(input, dotted_input,
                                      compress_level = 1L)))
  stopifnot(identical(read_raw_file(input), payload))

  compressed <- new_path(fileext = ".zst")
  zstd_compress_file(input, compressed, compress_level = 1L)
  compressed_bytes <- read_raw_file(compressed)
  stopifnot(errors(zstd_decompress_file(compressed, compressed)))
  stopifnot(identical(read_raw_file(compressed), compressed_bytes))

  symlink <- new_path()
  if (isTRUE(suppressWarnings(file.symlink(input, symlink)))) {
    stopifnot(errors(zstd_compress_file(input, symlink, compress_level = 1L)))
    stopifnot(identical(read_raw_file(input), payload))
  }

  rds_input <- new_path(fileext = ".rds")
  rds_value <- list(number = 1:10, text = letters)
  saveRDS(rds_value, rds_input, compress = "gzip")
  rds_bytes <- read_raw_file(rds_input)

  stopifnot(errors(rds_to_qs(rds_input, rds_input)))
  stopifnot(identical(read_raw_file(rds_input), rds_bytes))

  existing_output <- new_path(fileext = ".qs2")
  sentinel <- as.raw(c(11, 22, 33, 44))
  writeBin(sentinel, existing_output)
  stopifnot(errors(rds_to_qs(rds_input, existing_output,
                             compress_level = 999999L)))
  stopifnot(identical(read_raw_file(existing_output), sentinel))

  first_rds_output <- new_path()
  second_rds_output <- new_path()
  writeBin(as.raw(5), first_rds_output)
  writeBin(as.raw(6), second_rds_output)
  stopifnot(errors(rds_to_qs(rds_input,
                             c(first_rds_output, second_rds_output))))
  stopifnot(identical(read_raw_file(first_rds_output), as.raw(5)))
  stopifnot(identical(read_raw_file(second_rds_output), as.raw(6)))

  rds_to_qs(rds_input, existing_output, compress_level = 1L)
  stopifnot(identical(qs_read(existing_output, validate_checksum = TRUE),
                      rds_value))

  first_tmp <- new_path()
  second_tmp <- new_path()
  writeBin(as.raw(1), first_tmp)
  writeBin(as.raw(2), second_tmp)
  stopifnot(errors(zstd_in(identity, file = compressed,
                           tmpfile = c(first_tmp, second_tmp))))
  stopifnot(identical(read_raw_file(first_tmp), as.raw(1)))
  stopifnot(identical(read_raw_file(second_tmp), as.raw(2)))

  stopifnot(errors(zstd_in(identity, file = compressed, tmpfile = compressed)))
  stopifnot(identical(read_raw_file(compressed), compressed_bytes))

  zstd_target <- new_path(fileext = ".zst")
  writeBin(sentinel, zstd_target)
  stopifnot(errors(zstd_out(function(file) writeBin(payload, file),
                            file = zstd_target, tmpfile = zstd_target)))
  stopifnot(identical(read_raw_file(zstd_target), sentinel))

  unused_target <- new_path(fileext = ".zst")
  stopifnot(errors(zstd_out(function(file) writeBin(payload, file),
                            file = unused_target, tmpfile = unused_target)))
  stopifnot(!file.exists(unused_target))

  vector_tmp_one <- new_path()
  vector_tmp_two <- new_path()
  writeBin(as.raw(3), vector_tmp_one)
  writeBin(as.raw(4), vector_tmp_two)
  stopifnot(errors(zstd_out(function(file) writeBin(payload, file),
                            file = zstd_target,
                            tmpfile = c(vector_tmp_one, vector_tmp_two))))
  stopifnot(identical(read_raw_file(vector_tmp_one), as.raw(3)))
  stopifnot(identical(read_raw_file(vector_tmp_two), as.raw(4)))

  failed_tmp <- new_path()
  stopifnot(errors(zstd_out(function(file) {
    writeBin(payload, file)
    stop("writer failed")
  }, file = zstd_target, tmpfile = failed_tmp)))
  stopifnot(!file.exists(failed_tmp))
  stopifnot(identical(read_raw_file(zstd_target), sentinel))

  supplied_tmp <- new_path()
  successful_target <- new_path(fileext = ".zst")
  zstd_out(function(file) writeBin(payload, file),
           file = successful_target, tmpfile = supplied_tmp)
  stopifnot(!file.exists(supplied_tmp))

  decoded_tmp <- new_path()
  decoded <- zstd_in(function(file) read_raw_file(file),
                     file = successful_target, tmpfile = decoded_tmp)
  stopifnot(identical(decoded, payload))
  stopifnot(!file.exists(decoded_tmp))

  failed_read_tmp <- new_path()
  stopifnot(errors(zstd_in(function(file) stop("reader failed"),
                           file = successful_target,
                           tmpfile = failed_read_tmp)))
  stopifnot(!file.exists(failed_read_tmp))
})

cat("File collision and cleanup safety tests completed.\n")
