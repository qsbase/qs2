#' Zstd file compression
#'
#' A utility function to compresses a file with zstd.
#'
#' @usage zstd_compress_file(input_file, output_file, compress_level = qopt("compress_level"))
#'
#' @param input_file Path to the file to compress.
#' @param output_file Path for the compressed file.
#' @param compress_level The compression level used.
#'
#' @return No value is returned. The file is written to disk.
#' @export
#' @name zstd_compress_file
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
#' @param input_file Path to the zstd file.
#' @param output_file Path for the decompressed file.
#' 
#' @return No value is returned. The file is written to disk.
#' @export
#' @name zstd_decompress_file
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
