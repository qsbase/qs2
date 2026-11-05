.onLoad <- function(libname, pkgname) {
  qs2_check_charport_abi()
}

.onAttach <- function(libname, pkgname) {
  packageStartupMessage("qs2 ", utils::packageVersion("qs2"))
}
