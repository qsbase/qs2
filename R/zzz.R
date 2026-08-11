.onLoad <- function(libname, pkgname) {
  # Whether the session's native encoding is UTF-8 decides whether strings
  # marked CE_NATIVE have to be translated before being written. It is resolved
  # here, once, rather than on every string: the check is not free and the
  # answer effectively never changes.
  #
  # The consequence is that calling Sys.setlocale() partway through a session
  # does not change how qs2 treats native strings until the package is
  # reloaded. Erring towards "translate" is harmless -- in a UTF-8 locale R's
  # own translateCharUTF8() returns native strings unchanged.
  #
  # This used to be baked in at compile time by configure, which gave the wrong
  # answer for any binary package whose build machine and user disagreed.
  qs2_set_utf8_locale(isTRUE(l10n_info()[["UTF-8"]]))
}

.onAttach <- function(libname, pkgname) {
  packageStartupMessage("qs2 ", utils::packageVersion("qs2"))
}
