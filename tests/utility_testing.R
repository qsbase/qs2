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

cat("Testing base91 encode/decode...\n")
b91_raw <- as.raw(sample(0:255, size = 256, replace = TRUE))
b91_enc <- base91_encode(b91_raw)
b91_dec <- base91_decode(b91_enc)
stopifnot(identical(b91_dec, b91_raw))

cat("Testing qx_dump...\n")
tmp_qs <- tempfile(fileext = ".qs2")
obj <- as.raw(sample(0:255, size = 2 * 1024 * 1024, replace = TRUE))
qs_save(obj, tmp_qs, shuffle = TRUE, compress_level = 1L)
qd <- qx_dump(tmp_qs)
stopifnot(any(qd$block_shuffled == 1))
recovered <- do.call(c, qd$blocks)
stopifnot(identical(unserialize(recovered), obj))
stopifnot(identical(qd$stored_hash, qd$computed_hash))

cat("Utility tests completed.\n")
