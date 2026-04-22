#' qs2 to RDS format
#'
#' Converts a file saved in the `qs2` format to the `RDS` format.
#'
#' @usage qs_to_rds(input_file, output_file, compress_level = 6)
#'
#' @param input_file The `qs2` file to convert.
#' @param output_file The `RDS` file to write.
#' @param compress_level The gzip compression level to use when writing the RDS file (a value between 0 and 9).
#' @return No value is returned. The converted file is written to disk.
#'
#' @examples
#' qs_tmp <- tempfile(fileext = ".qs2")
#' rds_tmp <- tempfile(fileext = ".RDS")
#' 
#' x <- runif(1e6)
#' qs_save(x, qs_tmp)
#' qs_to_rds(input_file = qs_tmp, output_file = rds_tmp)
#' x2 <- readRDS(rds_tmp)
#' stopifnot(identical(x, x2))
#' 
#' @export
qs_to_rds <- function(input_file, output_file, compress_level = 6) {
  dump <- qx_dump(input_file)
  if(dump$format != "qs2") {
    stop("qs2 format not detected")
  }
  if(identical(dump$stored_hash, "0")) {
    stop("qs2 file does not contain a stored hash")
  }
  if(!identical(dump$stored_hash, dump$computed_hash)) {
    stop("qs2 file hash mismatch")
  }
  con <- gzfile(output_file, "wb", compression = compress_level)
  ok <- FALSE
  on.exit({
    try(close(con), silent = TRUE)
    if(!ok && file.exists(output_file)) {
      unlink(output_file)
    }
  }, add = TRUE)
  for(i in seq_along(dump$blocks)) {
    writeBin(dump$blocks[[i]], con)
  }
  close(con)
  ok <- TRUE
}

is_gzip_file <- function(path) {
  con <- file(path, "rb")
  on.exit(close(con), add = TRUE)
  identical(readBin(con, "raw", n = 2L), as.raw(c(0x1f, 0x8b)))
}

#' RDS to qs2 format
#'
#' Converts a file saved in the `RDS` format to the `qs2` format.
#'
#' @usage rds_to_qs(input_file, output_file, compress_level = 3)
#'
#' @param input_file The `RDS` file to convert.
#' @param output_file The `qs2` file to write.
#' @param compress_level The zstd compression level to use when writing the `qs2` file. See the `qs_save` help file for more details on this parameter.
#' 
#' @details
#' `rds_to_qs()` currently supports only gzip-compressed `RDS` input files.
#' Files that are uncompressed or use another compression format are rejected.
#'
#' The `shuffle` parameter is currently not supported when converting from `RDS` to `qs2`.
#' @return No value is returned. The converted file is written to disk.
#' 
#' @examples
#' qs_tmp <- tempfile(fileext = ".qs2")
#' rds_tmp <- tempfile(fileext = ".RDS")
#' 
#' x <- runif(1e6)
#' saveRDS(x, rds_tmp, compress = "gzip")
#' rds_to_qs(input_file = rds_tmp, output_file = qs_tmp)
#' x2 <- qs_read(qs_tmp, validate_checksum = TRUE)
#' stopifnot(identical(x, x2))
#' @export
rds_to_qs <- function(input_file, output_file, compress_level = 3) {
  MAX_BLOCKSIZE <- 1048576L # defined in io/io_common.h
  HEADER_SIZE <- 24 # defined in qx_file_headers.h

  if (!is_gzip_file(input_file)) {
    stop("rds_to_qs currently only supports gzip-compressed RDS input files")
  }

  tmp_output <- tempfile()
  in_con <- NULL
  out_con <- NULL
  ok <- FALSE

  on.exit({
    try(unlink(tmp_output), silent = TRUE)
    if (!is.null(in_con)) {
      try(close(in_con), silent = TRUE)
    }
    if (!is.null(out_con)) {
      try(close(out_con), silent = TRUE)
    }
    if (!ok && file.exists(output_file)) {
      unlink(output_file)
    }
  }, add = TRUE)

  qs_save(NULL, tmp_output, compress_level = compress_level, shuffle = FALSE)
  header_bytes <- readBin(tmp_output, "raw", n = HEADER_SIZE)
  in_con <- gzfile(input_file, "rb")
  out_con <- file(output_file, "wb")
  writeBin(header_bytes, out_con)
  # loop while in_con is not at EOF
  while (TRUE) {
    block <- readBin(in_con, "raw", n = MAX_BLOCKSIZE)
    if (length(block) == 0) break
    zblock <- zstd_compress_raw(block, compress_level)
    zblocksize <- as.integer(length(zblock)) # make sure we write out an integer
    writeBin(zblocksize, out_con)
    writeBin(zblock, out_con)
  }
  close(in_con)
  in_con <- NULL
  close(out_con)
  out_con <- NULL

  computed_hash <- internal_compute_qx_hash(output_file)
  internal_write_qx_hash(output_file, computed_hash)
  ok <- TRUE
}
