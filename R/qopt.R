#' qs2 Option Getter/Setter
#'
#' Get or set a global qs2 option.
#'
#' This function provides an interface to retrieve or update internal qs2 options
#' such as compression level, shuffle flag, number of threads, checksum validation,
#' warning for unsupported types, and ALTREP usage. It directly calls the underlying
#' C-level functions.
#'
#' @details The default settings are:
#'   \itemize{
#'     \item \code{compress_level}: 3L
#'     \item \code{shuffle}: TRUE
#'     \item \code{nthreads}: 1L
#'     \item \code{validate_checksum}: FALSE
#'     \item \code{warn_unsupported_types}: TRUE (used only in \code{qd_save})
#'     \item \code{use_alt_rep}: FALSE (used only in \code{qd_read})
#'   }
#'
#' When \code{value} is \code{NULL}, the current value of the specified option is returned.
#' Otherwise, the option is set to \code{value} and the new value is returned invisibly.
#'
#' @param parameter A character string specifying the option to access. Must be one of
#'        "compress_level", "shuffle", "nthreads", "validate_checksum", "warn_unsupported_types",
#'        or "use_alt_rep".
#' @param value If \code{NULL} (the default), the current value is retrieved.
#'        Otherwise, the global option is set to \code{value}.
#'
#' @return If \code{value} is \code{NULL}, returns the current value of the specified option.
#'         Otherwise, sets the option and returns the new value invisibly.
#'
#' @examples
#' # Get the current compression level:
#' qopt("compress_level")
#'
#' # Set the compression level to 5:
#' qopt("compress_level", value = 5)
#'
#' # Get the current shuffle setting:
#' qopt("shuffle")
#'
#' # Get the current setting for warn_unsupported_types (used in qd_save):
#' qopt("warn_unsupported_types")
#'
#' # Get the current setting for use_alt_rep (used in qd_read):
#' qopt("use_alt_rep")
#'
#' @export
qopt <- function(parameter, value = NULL) {
  if (parameter == "compress_level") {
    if (is.null(value)) {
      return(.Call(`_qs2_qs2_get_compress_level`))
    } else {
      .Call(`_qs2_qs2_set_compress_level`, value)
      invisible(.Call(`_qs2_qs2_get_compress_level`))
    }
  } else if (parameter == "shuffle") {
    if (is.null(value)) {
      return(.Call(`_qs2_qs2_get_shuffle`))
    } else {
      .Call(`_qs2_qs2_set_shuffle`, value)
      invisible(.Call(`_qs2_qs2_get_shuffle`))
    }
  } else if (parameter == "nthreads") {
    if (is.null(value)) {
      return(.Call(`_qs2_qs2_get_nthreads`))
    } else {
      .Call(`_qs2_qs2_set_nthreads`, value)
      invisible(.Call(`_qs2_qs2_get_nthreads`))
    }
  } else if (parameter == "validate_checksum") {
    if (is.null(value)) {
      return(.Call(`_qs2_qs2_get_validate_checksum`))
    } else {
      .Call(`_qs2_qs2_set_validate_checksum`, value)
      invisible(.Call(`_qs2_qs2_get_validate_checksum`))
    }
  } else if (parameter == "warn_unsupported_types") {
    if (is.null(value)) {
      return(.Call(`_qs2_qs2_get_warn_unsupported_types`))
    } else {
      .Call(`_qs2_qs2_set_warn_unsupported_types`, value)
      invisible(.Call(`_qs2_qs2_get_warn_unsupported_types`))
    }
  } else if (parameter == "use_alt_rep") {
    if (is.null(value)) {
      return(.Call(`_qs2_qs2_get_use_alt_rep`))
    } else {
      .Call(`_qs2_qs2_set_use_alt_rep`, value)
      invisible(.Call(`_qs2_qs2_get_use_alt_rep`))
    }
  } else {
    stop("Unknown parameter: ", parameter)
  }
}
