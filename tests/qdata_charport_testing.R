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

is_charpack <- function(x) {
  identical(charport::charport_info(x)$altrep_class, "charport::charpack")
}

is_materialized <- function(x) charport::charport_info(x)$is_materialized

materialize_charvecs <- function(x) {
  if (is.character(x) && (charport::is_charvec(x) || is_charpack(x))) {
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
      stopifnot(is_charpack(memory_out$nested$child))
      stopifnot(identical(normalize_qdata_strings(
        materialize_charvecs(memory_out)), expected))

      file <- tempfile(fileext = ".qd")
      qs2::qd_save(input, file, nthreads = nthreads, shuffle = shuffle)
      file_out <- qs2::qd_read(file, use_alt_rep = TRUE,
                               validate_checksum = TRUE, nthreads = nthreads)
      stopifnot(charport::is_charvec(file_out$main))
      stopifnot(is_charpack(file_out$nested$child))
      stopifnot(identical(normalize_qdata_strings(
        materialize_charvecs(file_out)), expected))
      unlink(file)

      # Retained nested charpack references must stay valid after parent collection.
      retained <- memory_out$nested$child
      rm(memory_out, file_out)
      invisible(gc())
      stopifnot(is_charpack(retained))
      stopifnot(identical(normalize_qdata_strings(
        charport::charport_materialize(retained)), expected$nested$child))

      # Re-serialization should consume the lazy vectors without forcing them.
      lazy <- qs2::qd_deserialize(memory, use_alt_rep = TRUE,
                                  validate_checksum = TRUE, nthreads = nthreads)
      stopifnot(!is_materialized(lazy$main), !is_materialized(lazy$nested$child))
      saved_again <- qs2::qd_serialize(lazy, nthreads = nthreads, shuffle = shuffle)
      stopifnot(!is_materialized(lazy$main), !is_materialized(lazy$nested$child))
      decoded_again <- qs2::qd_deserialize(saved_again, use_alt_rep = TRUE,
                                           validate_checksum = TRUE, nthreads = nthreads)
      stopifnot(identical(normalize_qdata_strings(
        materialize_charvecs(decoded_again)), expected))
      rm(lazy, decoded_again, retained)
      invisible(gc())
    }
  }
}

# use_alt_rep = FALSE returns ordinary STRSXP vectors.
plain_file <- tempfile(fileext = ".qd")
plain_input <- make_character_fixture(FALSE)
qs2::qd_save(plain_input, plain_file, nthreads = 1L)
plain_out <- qs2::qd_read(plain_file, use_alt_rep = FALSE, nthreads = 1L)
stopifnot(!charport::is_charvec(plain_out$main))
stopifnot(identical(normalize_qdata_strings(plain_out),
                    normalize_qdata_strings(plain_input)))
unlink(plain_file)

# Vectors of 1 to 256 strings read as charpacks, longer ones as charvecs, and
# empty ones as ordinary vectors. A charpack whose strings would exceed 1 MiB
# spills to an ordinary STRSXP held as its materialized state.
mib <- 1024L * 1024L
lengths_input <- list(
  one = "x", at_limit = rep(c("a", NA_character_, "", "\u03bb"), 64L),
  over_limit = rep("b", 257L), empty = character(0)
)
spill_input <- list(
  fits = c(strrep("f", mib - 1L), "g"),                  # exactly 1 MiB: stays packed
  first = c(strrep("h", mib + 1L), NA_character_, "", "i"),  # spills at the first string
  middle = c("j", NA_character_, "", strrep("k", mib), "l", NA_character_)
)
attr_input <- list(
  named = c(a = "x", b = NA_character_, c = ""),
  matrix = matrix(letters[1:6], 2L, dimnames = list(c("r1", "r2"), c("c1", "c2", "c3"))),
  factor = factor(c("lo", "hi", "lo", NA)),
  frame = data.frame(k = c("p", "q"), v = 1:2, stringsAsFactors = FALSE)
)
for (nthreads in threads) {
  out <- qs2::qd_deserialize(qs2::qd_serialize(lengths_input, nthreads = nthreads),
                             use_alt_rep = TRUE, nthreads = nthreads)
  stopifnot(is_charpack(out$one), is_charpack(out$at_limit), charport::is_charvec(out$over_limit))
  stopifnot(!is_materialized(out$one), !is_materialized(out$at_limit))
  stopifnot(is.na(charport::charport_info(out$empty)$altrep_class))
  stopifnot(identical(out, lengths_input))

  out <- qs2::qd_deserialize(qs2::qd_serialize(spill_input, nthreads = nthreads),
                             use_alt_rep = TRUE, nthreads = nthreads)
  stopifnot(all(vapply(out, is_charpack, logical(1))))
  stopifnot(!is_materialized(out$fits), is_materialized(out$first), is_materialized(out$middle))
  stopifnot(identical(out, spill_input))

  out <- qs2::qd_deserialize(qs2::qd_serialize(attr_input, nthreads = nthreads),
                             use_alt_rep = TRUE, nthreads = nthreads)
  stopifnot(is_charpack(out$named), is_charpack(out$matrix), is_charpack(out$frame$k))
  stopifnot(identical(out, attr_input))
}

# A charpack from qd_read stays packed through qd_save, serialize and saveRDS.
packed <- qs2::qd_deserialize(qs2::qd_serialize(list(v = c("s", NA_character_, "", "t"))),
                              use_alt_rep = TRUE)
again <- qs2::qd_deserialize(qs2::qd_serialize(packed), use_alt_rep = TRUE)
rds <- unserialize(serialize(packed, NULL))
# identical() reads the strings, so check the states first
stopifnot(!is_materialized(packed$v), is_charpack(rds$v), !is_materialized(rds$v))
stopifnot(identical(again, packed), identical(rds, packed))
