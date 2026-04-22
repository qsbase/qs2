# Utility tests for file compression/decompression and raw helpers
# Note: do not run here; leave execution to the user/environment.

library(qs2)

cat("Testing zstd file compression (small file)...\n")
infile <- tempfile()
outfile <- tempfile()
decoded <- tempfile()

writeBin(as.raw(1:100), infile)
zstd_compress_file(infile, outfile, compress_level = 1L)
zstd_decompress_file(outfile, decoded)
stopifnot(file.exists(outfile))
stopifnot(file.exists(decoded))
stopifnot(identical(readBin(infile, what = "raw", n = file.info(infile)$size),
                    readBin(decoded, what = "raw", n = file.info(decoded)$size)))

cat("Testing zstd file compression (multi-buffer)...\n")
infile <- tempfile()
outfile <- tempfile()
decoded <- tempfile()
data <- as.raw(sample(0:255, size = 3 * 1024 * 1024, replace = TRUE))
writeBin(data, infile)
zstd_compress_file(infile, outfile, compress_level = 1L)
zstd_decompress_file(outfile, decoded)
stopifnot(file.exists(outfile))
stopifnot(file.exists(decoded))
stopifnot(identical(readBin(decoded, what = "raw", n = length(data)), data))

cat("Testing zstd file compression (buffer edges)...\n")
infile <- tempfile()
outfile <- tempfile()
decoded <- tempfile()
buf_size <- 1024 * 1024
data_exact <- as.raw(sample(0:255, size = buf_size, replace = TRUE))
writeBin(data_exact, infile)
zstd_compress_file(infile, outfile, compress_level = 1L)
zstd_decompress_file(outfile, decoded)
stopifnot(identical(readBin(decoded, what = "raw", n = length(data_exact)), data_exact))

data_over <- c(data_exact, as.raw(1L))
writeBin(data_over, infile)
zstd_compress_file(infile, outfile, compress_level = 1L)
zstd_decompress_file(outfile, decoded)
stopifnot(identical(readBin(decoded, what = "raw", n = length(data_over)), data_over))

cat("Testing raw compression utilities...\n")
x <- serialize(list(a = 1:5, b = letters[1:10]), connection = NULL)
z <- zstd_compress_raw(x, compress_level = 1L)
x2 <- unserialize(zstd_decompress_raw(z))
stopifnot(identical(x2, list(a = 1:5, b = letters[1:10])))

cat("Testing zstd_compress_bound...\n")
stopifnot(zstd_compress_bound(0L) >= 0L)
stopifnot(zstd_compress_bound(100000L) >= 100000L)
stopifnot(zstd_compress_bound(2^31) >= 2^31)
stopifnot(inherits(try(zstd_compress_bound(-1), silent = TRUE), "try-error"))
stopifnot(inherits(try(zstd_compress_bound(1.5), silent = TRUE), "try-error"))

cat("Testing blosc shuffle/unshuffle...\n")
raw_data <- serialize(1L:1000L, NULL)
shuf4 <- blosc_shuffle_raw(raw_data, 4L)
unshuf4 <- blosc_unshuffle_raw(shuf4, 4L)
stopifnot(identical(unshuf4, raw_data))
shuf8 <- blosc_shuffle_raw(raw_data, 8L)
unshuf8 <- blosc_unshuffle_raw(shuf8, 8L)
stopifnot(identical(unshuf8, raw_data))

cat("Testing xxhash_raw...\n")
raw_hash <- xxhash_raw(as.raw(c(1, 2, 3, 4)))
stopifnot(is.character(raw_hash))

cat("Testing base85 encode/decode...\n")
b85_raw <- as.raw(sample(0:255, size = 128, replace = TRUE))
b85_enc <- base85_encode(b85_raw)
b85_dec <- base85_decode(b85_enc)
stopifnot(identical(b85_dec, b85_raw))
stopifnot(identical(base85_encode(raw(0)), ""))
stopifnot(identical(base85_decode(""), raw(0)))
stopifnot(inherits(try(base85_decode("31"), silent = TRUE), "try-error"))
stopifnot(inherits(try(base85_decode("961"), silent = TRUE), "try-error"))
stopifnot(inherits(try(base85_decode("rr91"), silent = TRUE), "try-error"))
stopifnot(inherits(try(base85_decode("%nSc1"), silent = TRUE), "try-error"))

cat("Testing base91 encode/decode...\n")
b91_raw <- as.raw(sample(0:255, size = 256, replace = TRUE))
b91_enc <- base91_encode(b91_raw)
b91_dec <- base91_decode(b91_enc)
stopifnot(identical(b91_dec, b91_raw))
stopifnot(identical(base91_encode(raw(0)), ""))
stopifnot(identical(base91_decode(""), raw(0)))
stopifnot(inherits(try(base91_decode("A "), silent = TRUE), "try-error"))
stopifnot(inherits(try(base91_decode(intToUtf8(c(65L, 92L))), silent = TRUE), "try-error"))

cat("Testing qx_dump...\n")
tmp_qs <- tempfile(fileext = ".qs2")
obj <- as.raw(sample(0:255, size = 2 * 1024 * 1024, replace = TRUE))
qs_save(obj, tmp_qs, shuffle = TRUE, compress_level = 1L)
qd <- qx_dump(tmp_qs)
stopifnot(any(qd$block_shuffled == 1))
recovered <- do.call(c, qd$blocks)
stopifnot(identical(unserialize(recovered), obj))
stopifnot(identical(qd$stored_hash, qd$computed_hash))

cat("Testing qs_to_rds and rds_to_qs with large random strings...\n")
large_strings <- stringfish::random_strings(
  N = 1e6,
  string_size = 23,
  vector_mode = "normal"
)

tmp_qs_large <- tempfile(fileext = ".qs2")
tmp_rds_large <- tempfile(fileext = ".rds")
tmp_qs_from_rds <- tempfile(fileext = ".qs2")

qs_save(large_strings, tmp_qs_large, compress_level = 1L)
qs_to_rds(tmp_qs_large, tmp_rds_large, compress_level = 1L)
from_rds <- readRDS(tmp_rds_large)
stopifnot(identical(from_rds, large_strings))

rds_to_qs(tmp_rds_large, tmp_qs_from_rds, compress_level = 1L)
from_qs <- qs_read(tmp_qs_from_rds, validate_checksum = TRUE)
stopifnot(identical(from_qs, large_strings))
qd_large <- qx_dump(tmp_qs_from_rds)
stopifnot(identical(qd_large$stored_hash, qd_large$computed_hash))

rm(from_rds, from_qs, large_strings)
invisible(gc())

cat("Utility tests completed.\n")
