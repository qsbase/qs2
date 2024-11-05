
shared_params_save <- function(file_output=TRUE, warn_unsupported_types=FALSE) {
  c('@param object The object to save.',
    '@param file The file name/path.'[file_output],
    '@param compress_level The compression level used (default 3).',
    '',
    'The maximum and minimum possible values depends on the version of ZSTD library used.',
    'As of ZSTD 1.5.6 the maximum compression level is 22, and the minimum is -131072. Usually, values in the low positive range offer very good performance in terms',
    'of speed and compression.',
    '@param shuffle Whether to allow byte shuffling when compressing data (default: `TRUE`).',
    '@param warn_unsupported_types Whether to warn when saving an object with an unsupported type (default `TRUE`).'[warn_unsupported_types],
    '@param nthreads The number of threads to use when compressing data (default: `1`).'
    )
}

shared_params_read <- function(file_input=TRUE, use_alt_rep=FALSE) {
  c('@param file The file name/path.'[file_input],
   '@param input The raw vector to deserialize.'[!file_input],
   '@param use_alt_rep Use ALTREP when reading in string data (default `FALSE`).'[use_alt_rep],
   '@param validate_checksum Whether to validate the stored checksum in the file (default `FALSE`). This can be used to test for file corruption but has a performance penality.',
   '@param nthreads The number of threads to use when reading data (default: `1`).')
}

#' qs_save
#'
#' Saves an object to disk using the `qs2` format.
#'
#' @usage qs_save(object, file, compress_level = 3L,
#' shuffle = TRUE, nthreads = 1L)
#'
#' @eval shared_params_save()
#' @return No value is returned. The file is written to disk.
#' @export
#' @name qs_save
#'
#' @examples
#' x <- data.frame(int = sample(1e3, replace=TRUE),
#'         num = rnorm(1e3),
#'         char = sample(state.name, 1e3, replace=TRUE),
#'         stringsAsFactors = FALSE)
#' myfile <- tempfile()
#' qs_save(x, myfile)
#' x2 <- qs_read(myfile)
#' identical(x, x2) # returns true
#'
#' # qs2 support multithreading
#' qs_save(x, myfile, nthreads=1)
#' x2 <- qs_read(myfile, nthreads=1)
#' identical(x, x2) # returns true
NULL

#' qs_serialize
#' 
#' Serializes an object to a raw vector using the `qs2` format.
#' 
#' @usage qs_serialize(object, compress_level = 3L, shuffle = TRUE, nthreads = 1L)
#' 
#' @eval shared_params_save(file_output=FALSE)
#' @return The serialized object as a raw vector.
#' @export
#' @name qs_serialize
#' 
#' @examples
#' x <- data.frame(int = sample(1e3, replace=TRUE),
#'         num = rnorm(1e3),
#'         char = sample(state.name, 1e3, replace=TRUE),
#'         stringsAsFactors = FALSE)
#' xserialized <- qs_serialize(x)
#' x2 <- qs_deserialize(xserialized)
#' identical(x, x2) # returns true
NULL

#' qs_read
#'
#' Reads an object that was saved to disk in the `qs2` format.
#'
#' @usage qs_read(file, validate_checksum=FALSE, nthreads = 1L)
#'
#' @eval shared_params_read()
#' @return The object stored in `file`.
#' @export
#' @name qs_read
#'
#' @examples
#' x <- data.frame(int = sample(1e3, replace=TRUE),
#'         num = rnorm(1e3),
#'         char = sample(state.name, 1e3, replace=TRUE),
#'         stringsAsFactors = FALSE)
#' myfile <- tempfile()
#' qs_save(x, myfile)
#' x2 <- qs_read(myfile)
#' identical(x, x2) # returns true
#'
#' # qs2 support multithreading
#' qs_save(x, myfile, nthreads=1)
#' x2 <- qs_read(myfile, nthreads=1)
#' identical(x, x2) # returns true
NULL

#' qs_deserialize
#' 
#' Deserializes a raw vector to an object using the `qs2` format.
#' 
#' @usage qs_deserialize(input, validate_checksum = FALSE, nthreads = 1L)
#' 
#' @eval shared_params_read(file_input=FALSE)
#' @return The deserialized object.
#' @export
#' @name qs_deserialize
#' 
#' @examples
#' x <- data.frame(int = sample(1e3, replace=TRUE),
#'         num = rnorm(1e3),
#'         char = sample(state.name, 1e3, replace=TRUE),
#'         stringsAsFactors = FALSE)
#' xserialized <- qs_serialize(x)
#' x2 <- qs_deserialize(xserialized)
#' identical(x, x2) # returns true
NULL

#' qd_save
#'
#' Saves an object to disk using the `qdata` format.
#'
#' @usage qd_save(object, file, compress_level = 3L,
#' shuffle = TRUE, warn_unsupported_types=TRUE,
#' nthreads = 1L)
#'
#' @eval shared_params_save(warn_unsupported_types = TRUE)
#' @return No value is returned. The file is written to disk.
#' @export
#' @name qd_save
#'
#' @examples
#' x <- data.frame(int = sample(1e3, replace=TRUE),
#'         num = rnorm(1e3),
#'         char = sample(state.name, 1e3, replace=TRUE),
#'         stringsAsFactors = FALSE)
#' myfile <- tempfile()
#' qd_save(x, myfile)
#' x2 <- qd_read(myfile)
#' identical(x, x2) # returns true
#'
#' # qdata support multithreading
#' qd_save(x, myfile, nthreads=1)
#' x2 <- qd_read(myfile, nthreads=1)
#' identical(x, x2) # returns true
NULL

#' qd_serialize
#' 
#' Serializes an object to a raw vector using the `qdata` format.
#' 
#' @usage qd_serialize(object, compress_level = 3L, shuffle = TRUE, warn_unsupported_types = TRUE, nthreads = 1L)
#' 
#' @eval shared_params_save(file_output=FALSE, warn_unsupported_types = TRUE)
#' @return The serialized object as a raw vector.
#' @export
#' @name qd_serialize
#' 
#' @examples
#' x <- data.frame(int = sample(1e3, replace=TRUE),
#'         num = rnorm(1e3),
#'         char = sample(state.name, 1e3, replace=TRUE),
#'         stringsAsFactors = FALSE)
#' xserialized <- qd_serialize(x)
#' x2 <- qd_deserialize(xserialized)
#' identical(x, x2) # returns true
NULL

#' qd_read
#'
#' Reads an object that was saved to disk in the `qdata` format.
#'
#' @usage qd_read(file, use_alt_rep = FALSE, validate_checksum=FALSE, nthreads = 1L)
#'
#' @eval shared_params_read(use_alt_rep = TRUE)
#' @return The object stored in `file`.
#' @export
#' @name qd_read
#'
#' @examples
#' x <- data.frame(int = sample(1e3, replace=TRUE),
#'         num = rnorm(1e3),
#'         char = sample(state.name, 1e3, replace=TRUE),
#'         stringsAsFactors = FALSE)
#' myfile <- tempfile()
#' qd_save(x, myfile)
#' x2 <- qd_read(myfile)
#' identical(x, x2) # returns true
#'
#' # qdata support multithreading
#' qd_save(x, myfile, nthreads=1)
#' x2 <- qd_read(myfile, nthreads=1)
#' identical(x, x2) # returns true
NULL

#' qd_deserialize
#' 
#' Deserializes a raw vector to an object using the `qdata` format.
#' 
#' @usage qd_deserialize(input, use_alt_rep = FALSE, validate_checksum = FALSE, nthreads = 1L)
#' 
#' @eval shared_params_read(file_input=FALSE, use_alt_rep = TRUE)
#' @return The deserialized object.
#' @export
#' @name qd_deserialize
#' 
#' @examples
#' x <- data.frame(int = sample(1e3, replace=TRUE),
#'         num = rnorm(1e3),
#'         char = sample(state.name, 1e3, replace=TRUE),
#'         stringsAsFactors = FALSE)
#' xserialized <- qd_serialize(x)
#' x2 <- qd_deserialize(xserialized)
#' identical(x, x2) # returns true
NULL

#' qx_dump
#'
#' Exports the uncompressed binary serialization to a list of raw vectors for both `qs2` and `qdata` formats.
#' For testing and exploratory purposes mainly.
#'
#' @usage qx_dump(file)
#'
#' @param file A file name/path.
#'
#' @return The uncompressed serialization.
#' @export
#' @name qx_dump
#'
#' @examples
#' x <- data.frame(int = sample(1e3, replace=TRUE),
#'         num = rnorm(1e3),
#'         char = sample(state.name, 1e3, replace=TRUE),
#'         stringsAsFactors = FALSE)
#' myfile <- tempfile()
#' qs_save(x, myfile)
#' binary_data <- qx_dump(myfile)
NULL

#' Zstd compression
#'
#' Compresses to a raw vector using the zstd algorithm. Exports the main zstd compression function.
#'
#' @usage zstd_compress_raw(data, compress_level)
#'
#' @param data Raw vector to be compressed.
#' @param compress_level The compression level used.
#'
#' @return The compressed data as a raw vector.
#' @export
#' @name zstd_compress_raw
#'
#' @examples
#' x <- 1:1e6
#' xserialized <- serialize(x, connection=NULL)
#' xcompressed <- zstd_compress_raw(xserialized, compress_level = 1)
#' xrecovered <- unserialize(zstd_decompress_raw(xcompressed))
NULL

#' Zstd decompression
#'
#' Decompresses a zstd compressed raw vector.
#'
#' @usage zstd_decompress_raw(data)
#'
#' @param data A raw vector to be decompressed.
#'
#' @return The decompressed data as a raw vector.
#' @export
#' @name zstd_decompress_raw
#'
#' @examples
#' x <- 1:1e6
#' xserialized <- serialize(x, connection=NULL)
#' xcompressed <- zstd_compress_raw(xserialized, compress_level = 1)
#' xrecovered <- unserialize(zstd_decompress_raw(xcompressed))
NULL

#' Zstd compress bound
#'
#' Exports the compress bound function from the zstd library. Returns the maximum potential compressed size of an object of length `size`.
#'
#' @usage zstd_compress_bound(size)
#'
#' @param size An integer size
#'
#' @return maximum compressed size
#' @export
#' @name zstd_compress_bound
#'
#' @examples
#' zstd_compress_bound(100000)
#' zstd_compress_bound(1e9)
NULL


#' Shuffle a raw vector
#'
#' Shuffles a raw vector using BLOSC shuffle routines.
#'
#' @usage blosc_shuffle_raw(data, bytesofsize)
#'
#' @param data A raw vector to be shuffled.
#' @param bytesofsize Either `4` or `8`.
#'
#' @return The shuffled vector
#' @export
#' @name blosc_shuffle_raw
#'
#' @examples
#' x <- serialize(1L:1000L, NULL)
#' xshuf <- blosc_shuffle_raw(x, 4)
#' xunshuf <- blosc_unshuffle_raw(xshuf, 4)
NULL

#' Un-shuffle a raw vector
#'
#' Un-shuffles a raw vector using BLOSC un-shuffle routines.
#'
#' @usage blosc_unshuffle_raw(data, bytesofsize)
#'
#' @param data A raw vector to be unshuffled.
#' @param bytesofsize Either `4` or `8`.
#'
#' @return The unshuffled vector.
#' @export
#' @name blosc_unshuffle_raw
#'
#' @examples
#' x <- serialize(1L:1000L, NULL)
#' xshuf <- blosc_shuffle_raw(x, 4)
#' xunshuf <- blosc_unshuffle_raw(xshuf, 4)
NULL

