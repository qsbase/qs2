# Report check-step times for each reverse dependency, old (CRAN qs2) vs new.
# Reads the R CMD check logs that revdepcheck leaves under revdep/checks;
# times come from _R_CHECK_TIMINGS_=0, as "[cpu/elapsed]" on each step.
# Usage: Rscript .github/scripts/revdep_timings.R [dir]
# Searches <dir> recursively, so it also works on several downloaded shard artifacts.
args <- commandArgs(TRUE)
root <- if (length(args)) args[1] else "revdep"
logs <- list.files(root, pattern = "^00check\\.log$",
                   recursive = TRUE, full.names = TRUE)
m <- regmatches(logs, regexec("checks/([^/]+)/(old|new)/", logs))
keep <- lengths(m) == 3
logs <- logs[keep]; m <- m[keep]

secs <- function(x) {
  v <- as.numeric(sub("[sm]$", "", x))
  ifelse(endsWith(x, "m"), v * 60, v)
}
rows <- list()
for (i in seq_along(logs)) {
  lines <- readLines(logs[i], warn = FALSE)
  hit <- regmatches(lines, regexec("\\[([0-9.]+[sm])/([0-9.]+[sm])\\]", lines))
  # a line holding only a time (" [21s/22s] OK") belongs to the step above it
  head <- cumsum(startsWith(lines, "* "))
  for (j in which(lengths(hit) == 3)) {
    line <- if (grepl("^\\s*\\[", lines[j])) lines[match(head[j], head)] else lines[j]
    step <- trimws(sub("\\s*\\.\\.\\..*$|\\s*\\[.*$", "", line))
    step <- sub("^\\* checking ", "", step)
    step <- sub("^whether package .* can be installed$", "install", step)
    step <- sub("^Running .(.*).$", "test file \\1", step)
    step <- sub("^.(.*). using .*$", "vignette \\1", step)
    rows[[length(rows) + 1]] <- data.frame(
      package = m[[i]][2], which = m[[i]][3], step = step,
      elapsed = secs(hit[[j]][3]), stringsAsFactors = FALSE)
  }
}

out <- file.path(root, "timings.md")
if (!length(rows)) {
  writeLines(c("# Revdep check timings", "", "No timed steps found in check logs."), out)
} else {
  d <- do.call(rbind, rows)
  d <- aggregate(elapsed ~ package + step + which, d, sum)
  w <- reshape(d, idvar = c("package", "step"), timevar = "which", direction = "wide")
  for (k in c("elapsed.old", "elapsed.new")) if (is.null(w[[k]])) w[[k]] <- NA_real_
  w <- w[order(w$package, w$step), ]
  # per-file test and vignette lines are already inside "tests" and "running R code from vignettes"
  part <- startsWith(w$step, "test file ") | startsWith(w$step, "vignette ")
  tot <- aggregate(cbind(elapsed.old, elapsed.new) ~ package, w[!part, ],
                   sum, na.action = na.pass)
  fmt <- function(x) ifelse(is.na(x), "", sprintf("%.0f", x))
  table_md <- function(df, cols, head) c(
    paste0("| ", paste(head, collapse = " | "), " |"),
    paste0("|", paste(rep("---", length(head)), collapse = "|"), "|"),
    sprintf("| %s |", do.call(paste, c(lapply(cols, function(cl) {
      v <- df[[cl]]; if (is.numeric(v)) fmt(v) else v
    }), sep = " | "))))
  writeLines(c(
    "# Revdep check timings",
    "",
    "Elapsed seconds per package, CRAN qs2 (old) vs this branch (new).",
    "Totals sum the timed steps (install, examples, tests, vignettes), not the per-file test and vignette lines.",
    "",
    "## Totals", "",
    table_md(tot, c("package", "elapsed.old", "elapsed.new"), c("package", "old (s)", "new (s)")),
    "", "## Steps", "",
    table_md(w, c("package", "step", "elapsed.old", "elapsed.new"), c("package", "step", "old (s)", "new (s)"))
  ), out)
}
cat(readLines(out), sep = "\n")
