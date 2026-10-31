# Focused qdata <-> charport coverage. This is a standalone test script, like
# zz_correctness_testing.R; it deliberately excludes qtable.
if (!requireNamespace("qs2", quietly = TRUE) ||
    !requireNamespace("charport", quietly = TRUE)) {
  stop("qs2 and charport are required for qdata charport tests")
}

# qdata writes character payloads in UTF-8 form and the reader tags decoded
# strings as UTF-8. CE_BYTES payload bytes are preserved without transcoding.
normalize_qdata_strings <- function(x) {
  if (is.character(x)) {
    at <- attributes(x)
    out <- character(length(x))
    for (i in seq_along(x)) {
      if (is.na(x[[i]])) {
        out[[i]] <- NA_character_
        next
      }
      value <- x[[i]]
      encoding <- Encoding(value)
      if (identical(encoding, "bytes")) {
        Encoding(value) <- "UTF-8"
      } else if (identical(encoding, "latin1") ||
                 (identical(encoding, "unknown") &&
                  !isTRUE(l10n_info()[["UTF-8"]]) &&
                  any(as.integer(charToRaw(value)) > 127L))) {
        value <- enc2utf8(value)
      } else {
        Encoding(value) <- "UTF-8"
      }
      out[[i]] <- value
    }
    if (!is.null(at)) {
      at <- lapply(at, normalize_qdata_strings)
      attributes(out) <- at
    }
    return(out)
  }
  if (is.list(x)) {
    at <- attributes(x)
    out <- lapply(x, normalize_qdata_strings)
    if (!is.null(at)) {
      at <- lapply(at, normalize_qdata_strings)
      attributes(out) <- at
    }
    return(out)
  }
  x
}

materialize_charvecs <- function(x) {
  if (is.character(x) && charport::is_charvec(x)) {
    return(charport::charport_materialize(x))
  }
  if (is.list(x)) {
    at <- attributes(x)
    out <- lapply(x, materialize_charvecs)
    if (!is.null(at)) {
      at <- lapply(at, materialize_charvecs)
      attributes(out) <- at
    }
    return(out)
  }
  x
}

make_character_fixture <- function(as_charvec) {
  utf8 <- enc2utf8("snowman \u2603")
  latin1 <- iconv("caf\u00e9", from = "UTF-8", to = "latin1")
  Encoding(latin1) <- "latin1"
  bytes <- rawToChar(as.raw(c(0xE9L, 0x2DL, 0x78L)))
  Encoding(bytes) <- "bytes"
  strings <- c(
    "ASCII", utf8, latin1, NA_character_, "", bytes,
    rep(c("batch-a", "batch-b", NA_character_, ""), length.out = 520L),
    strrep("large-cross-block-", 70000L)
  )
  if (as_charvec) strings <- charport::as_charvec(strings)
  names(strings) <- c("ascii", "utf8", "latin1", "missing", "empty", "bytes",
                      paste0("item", seq_len(length(strings) - 6L)))
  class(strings) <- c("qdata_test_strings", "character")
  attr(strings, "metadata") <- list(label = "root \u03BB", codes = c("one", "two"))

  child <- c("nested", NA_character_, "tail", enc2utf8("\u03BC"))
  if (as_charvec) child <- charport::as_charvec(child)
  names(child) <- c("first", "missing", "last", "unicode")
  attr(child, "note") <- "nested metadata"

  structure(
    list(main = strings, nested = list(child = child, empty = character(), tail = "ok")),
    class = c("qdata_test_container", "list"),
    names = c("main", "nested"),
    note = list(owner = "qdata", tags = c("roundtrip", "charport"))
  )
}

threads <- 1L
if (isTRUE(qs2:::check_TBB())) threads <- c(1L, 4L)

# Consecutive ordinary singleton vectors share Reader batches on write. Check
# ordering across batch boundaries and switches back to larger/ALTREP vectors.
singletons <- as.list(rep(c("one", NA_character_, "", "\u03bb"), 200L))
singletons[[257L]] <- c("longer", "vector")
singletons[[513L]] <- charport::as_charvec(c("borrowed", NA_character_))
for (nthreads in threads) {
  encoded <- qs2::qd_serialize(singletons, nthreads = nthreads)
  for (alt in c(FALSE, TRUE)) {
    stopifnot(identical(qs2::qd_deserialize(encoded, use_alt_rep = alt,
                                           validate_checksum = TRUE,
                                           nthreads = nthreads), singletons))
  }
}

for (input_charvec in c(FALSE, TRUE)) {
  input <- make_character_fixture(input_charvec)
  expected <- normalize_qdata_strings(make_character_fixture(FALSE))

  for (nthreads in threads) {
    for (shuffle in c(FALSE, TRUE)) {
      memory <- qs2::qd_serialize(input, nthreads = nthreads, shuffle = shuffle)
      memory_out <- qs2::qd_deserialize(memory, use_alt_rep = TRUE,
                                        validate_checksum = TRUE, nthreads = nthreads)
      stopifnot(charport::is_charvec(memory_out$main))
      stopifnot(charport::is_charvec(memory_out$nested$child))
      stopifnot(identical(normalize_qdata_strings(
        materialize_charvecs(memory_out)), expected))

      file <- tempfile(fileext = ".qd")
      qs2::qd_save(input, file, nthreads = nthreads, shuffle = shuffle)
      file_out <- qs2::qd_read(file, use_alt_rep = TRUE,
                               validate_checksum = TRUE, nthreads = nthreads)
      stopifnot(charport::is_charvec(file_out$main))
      stopifnot(charport::is_charvec(file_out$nested$child))
      stopifnot(identical(normalize_qdata_strings(
        materialize_charvecs(file_out)), expected))
      unlink(file)

      # Retained nested charvec references must stay valid after parent collection.
      retained <- memory_out$nested$child
      rm(memory_out, file_out)
      invisible(gc())
      stopifnot(charport::is_charvec(retained))
      stopifnot(identical(normalize_qdata_strings(
        charport::charport_materialize(retained)), expected$nested$child))

      # Re-serialization should consume the lazy charvec without forcing it.
      lazy <- qs2::qd_deserialize(memory, use_alt_rep = TRUE,
                                  validate_checksum = TRUE, nthreads = nthreads)
      stopifnot(!charport::charport_info(lazy$main)$is_materialized)
      saved_again <- qs2::qd_serialize(lazy, nthreads = nthreads, shuffle = shuffle)
      stopifnot(!charport::charport_info(lazy$main)$is_materialized)
      decoded_again <- qs2::qd_deserialize(saved_again, use_alt_rep = TRUE,
                                           validate_checksum = TRUE, nthreads = nthreads)
      stopifnot(identical(normalize_qdata_strings(
        materialize_charvecs(decoded_again)), expected))
      rm(lazy, decoded_again, retained)
      invisible(gc())
    }
  }
}

# The default reader setting remains ordinary STRSXP vectors.
plain_file <- tempfile(fileext = ".qd")
plain_input <- make_character_fixture(FALSE)
qs2::qd_save(plain_input, plain_file, nthreads = 1L)
plain_out <- qs2::qd_read(plain_file, use_alt_rep = FALSE, nthreads = 1L)
stopifnot(!charport::is_charvec(plain_out$main))
stopifnot(identical(normalize_qdata_strings(plain_out),
                    normalize_qdata_strings(plain_input)))
unlink(plain_file)
