# helper script for copy and reading benchmarks
suppressPackageStartupMessages({
  library(dplyr, quietly=TRUE)
  library(stringplus, quietly=TRUE)
  library(qs2, quietly=TRUE)
})

set_string_ops("&", "|")

args <- commandArgs(trailingOnly = TRUE)
algo <- args[1]
BLOCKSIZE <- args[2] %>% as.numeric
nthreads <- args[3] %>% as.numeric
file_path <- args[4] # must be full path not relative
output_temp_file <- args[5] # write results to here

now <- function() assign(".time", Sys.time(), envir = globalenv())
later <- function() { as.numeric(Sys.time() - get(".time", envir = globalenv()), units = "secs") }

invisible(qs2:::internal_set_blocksize(BLOCKSIZE))

# now()
# x <- file.copy(file_path, copy_path)
# copy_time <- later()
if(algo == "qs2") {
  suppressPackageStartupMessages( library(qs2, quietly=TRUE) )
  now()
  x <- qs2::qs_read(file_path, validate_checksum = FALSE, nthreads = nthreads)
  read_time <- later()
} else if(algo == "qdata") {
  suppressPackageStartupMessages( library(qs2, quietly=TRUE) )
  now()
  x <- qs2::qd_read(file_path, validate_checksum = FALSE, nthreads = nthreads)
  read_time <- later()
}

writeLines(text = c(read_time) %>% as.character, con = output_temp_file)

