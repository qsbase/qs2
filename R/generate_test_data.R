#' Generate deterministic mixed-column test data
#'
#' Creates a small deterministic data frame with string, numeric, and integer
#' columns. The numeric and integer columns combine seeded random noise with a
#' mild linear trend so downstream smoke tests exercise both repetition and
#' variation.
#'
#' @param nrows Number of rows to generate.
#' @param seed Integer seed used to make the output reproducible.
#'
#' @return A data frame with columns `string`, `numeric`, and `int`.
#' @importFrom stringfish random_strings
#' @export
generate_test_data <- function(nrows, seed = 1L) {
  if (!is.numeric(nrows) || length(nrows) != 1L || is.na(nrows) || nrows < 0 || nrows != floor(nrows)) {
    stop("nrows must be a single non-negative whole number", call. = FALSE)
  }
  if (!is.numeric(seed) || length(seed) != 1L || is.na(seed) || seed != floor(seed)) {
    stop("seed must be a single whole number", call. = FALSE)
  }

  nrows <- as.integer(nrows)
  seed <- as.integer(seed)

  set.seed(seed)

  index <- seq_len(nrows)
  trend <- if (nrows > 1L) {
    (index - 1L) / (nrows - 1L)
  } else if (nrows == 1L) {
    0
  } else {
    numeric(0)
  }

  data.frame(
    string = random_strings(N = nrows, string_size = 16L, vector_mode = "normal"),
    numeric = trend + stats::rnorm(nrows, sd = 0.05),
    int = as.integer(100L + index %/% 8L + sample.int(7L, nrows, replace = TRUE) - 4L),
    stringsAsFactors = FALSE
  )
}
