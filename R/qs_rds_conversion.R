#' qs2 to RDS format
#'
#' Converts a file saved in the `qs2` format to the `RDS` format.
#'
#' @usage qs_to_rds(input_file, output_file, compress_level = 6)
#'
#' @param input_file The `qs2` file to convert.
#' @param output_file The `RDS` file to write.
#' @param compress_level The gzip compression level to use when writing the RDS file (a value between 0 and 9).
#' @value No value is returned. The converted file is written to disk.
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
  con <- gzfile(output_file, "wb", compression = compress_level)
  for(i in 1:length(dump$blocks)) {
    writeBin(dump$blocks[[i]], con)
  }
  close(con)
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
#' @details The `shuffle` parameters is currently not supported when converting from `RDS` to `qs2`.
#' When reading the resulting `qs2` file, `validate_checksum` must be set to `FALSE`.
#' @value No value is returned. The converted file is written to disk.
#' 
#' @examples
#' qs_tmp <- tempfile(fileext = ".qs2")
#' rds_tmp <- tempfile(fileext = ".RDS")
#' 
#' x <- runif(1e6)
#' saveRDS(x, rds_tmp)
#' rds_to_qs(input_file = rds_tmp, output_file = qs_tmp)
#' x2 <- qs_read(qs_tmp, validate_checksum = FALSE)
#' stopifnot(identical(x, x2))
#' @export
rds_to_qs <- function(input_file, output_file, compress_level = 3) {
  MAX_BLOCKSIZE <- check_internal_blocksize() # defined in io/io_common.h
  HEADER_SIZE <- 24 # defined in qx_file_headers.h
  tmp_output <- tempfile()
  qs_save(NULL, tmp_output, compress_level = compress_level, shuffle = FALSE)
  header_bytes <- readBin(tmp_output, "raw", n = HEADER_SIZE)
  in_con <- gzfile(input_file, "rb")
  out_con <- file(output_file, "wb")
  writeBin(header_bytes, out_con)
  # loop while in_con is not at EOF
  while(TRUE) {
    block <- readBin(in_con, "raw", n = MAX_BLOCKSIZE)
    if(length(block) == 0) break;
    blocksize <- length(block)
    zblock <- zstd_compress_raw(block, compress_level)
    zblocksize <- as.integer(length(zblock)) # make sure we write out an integer
    writeBin(zblocksize, out_con)
    writeBin(zblock, out_con)
  }
  close(in_con)
  close(out_con)
}
