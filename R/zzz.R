.onLoad <- function(libname, pkgname) {
  loaded_in_fork_child(detect_parallel_fork())
}

.onAttach <- function(libname, pkgname) {
  packageStartupMessage("qs2 ", utils::packageVersion("qs2"))
}

# We need to test if we are in a forked process, since TBB is not supported under fork
# https://www.intel.com/content/www/us/en/docs/onetbb/developer-guide-api-reference/2021-11/known-limitations.html
# Adapted from parallelly::isForkedChild():
# https://github.com/futureverse/parallelly/blob/develop/R/isForkedChild.R
detect_parallel_fork <- function() {
  if (!"parallel" %in% loadedNamespaces()) {
    return(FALSE)
  }

  ns <- asNamespace("parallel")
  is_child <- get0("isChild", mode = "function", envir = ns, inherits = FALSE)
  if (!is.function(is_child)) {
    return(FALSE)
  }

  isTRUE(is_child())
}
