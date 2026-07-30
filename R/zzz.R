.onAttach <- function(libname, pkgname) {
  packageStartupMessage("qs2 ", utils::packageVersion("qs2"))
}
