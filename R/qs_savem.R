#' qs_savem
#'
#' Saves (serializes) multiple objects to disk.
#'
#' This function extends [qs_save()] to replicate the functionality of [base::save()] to save multiple objects. Read them back with [qs_readm()].
#'
#' @param ... Objects to serialize. Named arguments will be passed to [qs_save()] during saving. Un-named arguments will be saved. A named `file` argument is required.
#'
#' @export
#'
#' @examples
#' x1 <- data.frame(int = sample(1e3, replace=TRUE),
#'                  num = rnorm(1e3),
#'                  char = sample(starnames$`IAU Name`, 1e3, replace=TRUE),
#'                  stringsAsFactors = FALSE)
#' x2 <- data.frame(int = sample(1e3, replace=TRUE),
#'                  num = rnorm(1e3),
#'                  char = sample(starnames$`IAU Name`, 1e3, replace=TRUE),
#'                  stringsAsFactors = FALSE)
#' myfile <- tempfile()
#' qs_savem(x1, x2, file=myfile)
#' rm(x1, x2)
#' qs_readm(myfile)
#' exists('x1') && exists('x2') # returns true
qs_savem <- function (...) {
  full_call <- as.list(sys.call())[-1]
  objects <- list(...)
  unnamed <- which(names(objects) == "")
  unnamed_list <- objects[unnamed]
  names(unnamed_list) <- sapply(unnamed, function(i) parse(text = full_call[[i]]))
  named_list <- objects[-unnamed]
  named_list[["object"]] <- unnamed_list
  do.call(qs_save,named_list)
}


#' qs_readm
#'
#' Reads an object in a file serialized to disk using [qs_savem()].
#'
#' This function extends [qs_read] to replicate the functionality of [base::load()] to load multiple saved objects into your workspace.
#'
#' @param file The file name/path.
#' @param env The environment where the data should be loaded. Default is the calling environment ([parent.frame()]).
#' @param ... additional arguments will be passed to [qs_read].
#'
#' @return Nothing is explicitly returned, but the function will load the saved objects into the workspace.
#' @export
#'
#' @examples
#' x1 <- data.frame(int = sample(1e3, replace=TRUE),
#'                  num = rnorm(1e3),
#'                  char = sample(starnames$`IAU Name`, 1e3, replace=TRUE),
#'                  stringsAsFactors = FALSE)
#' x2 <- data.frame(int = sample(1e3, replace=TRUE),
#'                  num = rnorm(1e3),
#'                  char = sample(starnames$`IAU Name`, 1e3, replace=TRUE),
#'                  stringsAsFactors = FALSE)
#' myfile <- tempfile()
#' qs_savem(x1, x2, file=myfile)
#' rm(x1, x2)
#' qs_readm(myfile)
#' exists('x1') && exists('x2') # returns true
qs_readm <- function(file, env = parent.frame(), ...) {
  savelist <- qs_read(file, ...)
  if (!is.list(savelist) || is.null(names(savelist))) stop(paste0("Object read from ", file, " is not a named list."))
  invisible(list2env(savelist, env))
}
