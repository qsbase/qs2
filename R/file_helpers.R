.qs2_validate_path <- function(path, argument) {
  if (!is.character(path) || length(path) != 1L || is.na(path) || !nzchar(path)) {
    stop(argument, " must be a non-empty character string of length 1", call. = FALSE)
  }
  invisible(path)
}

.qs2_path_key <- function(path) {
  expanded <- path.expand(path)
  directory <- normalizePath(dirname(expanded), winslash = "/", mustWork = TRUE)
  key <- file.path(directory, basename(expanded))
  if (.Platform$OS.type == "windows") {
    key <- tolower(key)
  }
  key
}

.qs2_path_entry_exists <- function(path) {
  link_target <- Sys.readlink(path)
  file.exists(path) || (!is.na(link_target) && nzchar(link_target))
}

.qs2_existing_paths_same <- function(first_path, second_path) {
  if (!file.exists(first_path) || !file.exists(second_path)) {
    return(FALSE)
  }
  first_path <- normalizePath(first_path, winslash = "/", mustWork = TRUE)
  second_path <- normalizePath(second_path, winslash = "/", mustWork = TRUE)
  identical(first_path, second_path)
}

.qs2_validate_tmpfile <- function(tmpfile, other_path, other_argument) {
  .qs2_validate_path(tmpfile, "tmpfile")
  tmpdir <- dirname(path.expand(tmpfile))
  if (!dir.exists(tmpdir)) {
    stop("tmpfile directory does not exist: ", tmpdir, call. = FALSE)
  }

  same_path <- identical(.qs2_path_key(tmpfile), .qs2_path_key(other_path))
  same_file <- .qs2_existing_paths_same(tmpfile, other_path)
  if (same_path || same_file) {
    stop("tmpfile and ", other_argument, " must refer to different files", call. = FALSE)
  }
  if (.qs2_path_entry_exists(tmpfile)) {
    stop("tmpfile must not already exist", call. = FALSE)
  }
  invisible(tmpfile)
}
