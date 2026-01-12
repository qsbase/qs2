#' Zstd file helpers
#'
#' Helpers for compressing and decompressing zstd files.
#'
#' @name zstd_file_functions
NULL

#' Zstd file compression
#'
#' A utility function to compresses a file with zstd.
#'
#' @usage zstd_compress_file(input_file, output_file, compress_level = qopt("compress_level"))
#'
#' @name zstd_compress_file
#' @param input_file Path to the input file.
#' @param output_file Path to the output file.
#' @param compress_level The compression level used.
#'
#' @return No value is returned. The file is written to disk.
#' @export
#' @rdname zstd_file_functions
#'
#' @examples
#' infile <- tempfile()
#' writeBin(as.raw(1:5), infile)
#' outfile <- tempfile()
#' zstd_compress_file(infile, outfile, compress_level = 1)
#' stopifnot(file.exists(outfile))
NULL

#' Zstd file decompression
#'
#' A utility function to decompresses a zstd file to disk.
#'
#' @usage zstd_decompress_file(input_file, output_file)
#' 
#' @name zstd_decompress_file
#' @param input_file Path to the input file.
#' @param output_file Path to the output file.
#' 
#' @return No value is returned. The file is written to disk.
#' @export
#' @rdname zstd_file_functions
#' 
#' @examples
#' infile <- tempfile()
#' writeBin(as.raw(1:5), infile)
#' zfile <- tempfile()
#' zstd_compress_file(infile, zfile, compress_level = 1)
#' outfile <- tempfile()
#' zstd_decompress_file(zfile, outfile)
#' stopifnot(identical(readBin(infile, what = "raw", n = 5), readBin(outfile, what = "raw", n = 5)))
NULL



#' Zstd file substitution for input
#'
#' Substitutes a zstd compressed file for a regular input file. The zstd compressed 
#' file is decompressed to the input `FUN`.
#'
#' This is a generic wrapper that works with any function that reads from a
#' file.
#'
#' @param FUN Function to call.
#' @param ... Arguments passed to `FUN`. The first named argument is treated as
#'   the file path.
#' @param envir Environment for `FUN` evaluation.
#' @param tmpfile Temporary file path. If not supplied, a temp file is created
#'   and removed on exit.
#'
#' @return The value returned by `FUN`.
#' @export
#' @rdname zstd_in_out
#'
#' @examples
#' if (requireNamespace("data.table", quietly = TRUE)) {
#'   zfile <- tempfile(fileext = ".csv.zst")
#'   zstd_out(data.table::fwrite, mtcars, file = zfile)
#'   dt <- zstd_in(data.table::fread, file = zfile)
#'   print(nrow(dt))
#' }
zstd_in <- function(FUN, ..., envir = parent.frame(), tmpfile = tempfile()) {
  params <- list(...)
  w <- which(names(params) != "")
  if (length(w) == 0) stop("expecting at least one named parameter for file path")
  w <- w[1]
  input_path <- params[[w]]
  if (!is.character(input_path) || length(input_path) != 1 || is.na(input_path)) {
    stop("file path must be a character string of length 1")
  }
  if (!file.exists(input_path)) {
    stop("file path does not exist: ", input_path)
  }
  on.exit(unlink(tmpfile), add = TRUE)
  zstd_decompress_file(input_path, tmpfile)
  params[[w]] <- tmpfile
  do.call(FUN, params, envir = envir)
}


#' Zstd file substitution for output
#'
#' Substitutes a zstd compressed file for a regular output file. The output of `FUN` is 
#' converted to a zstd compressed file at the target zstd file path. 
#'
#' This is a generic wrapper that works with any function that writes to a file.
#'
#' @param FUN Function to call.
#' @param ... Arguments passed to `FUN`. The first named argument is treated as
#'   the file path.
#' @param envir Environment for `FUN` evaluation.
#' @param tmpfile Temporary file path. If not supplied, a temp file is created
#'   and removed on exit.
#'
#' @return The value returned by `FUN`, with its visibility preserved.
#' @export
#' @rdname zstd_in_out
zstd_out <- function(FUN, ..., envir = parent.frame(), tmpfile = tempfile()) {
  params <- list(...)
  w <- which(names(params) != "")
  if (length(w) == 0) stop("expecting at least one named parameter for file path")
  w <- w[1]
  zstd_file_path <- params[[w]]
  if (!is.character(zstd_file_path) || length(zstd_file_path) != 1 || is.na(zstd_file_path)) {
    stop("file path must be a character string of length 1")
  }
  if (file.exists(zstd_file_path)) {
    if (file.access(zstd_file_path, 2) != 0) {
      stop("file path not writable: ", zstd_file_path)
    }
  } else {
    out_dir <- dirname(zstd_file_path)
    if (!dir.exists(out_dir)) {
      stop("output directory does not exist: ", out_dir)
    }
  }
  on.exit(unlink(tmpfile), add = TRUE)
  params[[w]] <- tmpfile
  out <- withVisible(do.call(FUN, params, envir = envir))
  zstd_compress_file(tmpfile, zstd_file_path)
  if (out$visible) {
    out$value
  } else {
    invisible(out$value)
  }
}
