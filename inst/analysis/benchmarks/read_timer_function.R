# helper script for copy and reading benchmarks
suppressPackageStartupMessages({
  library(dplyr, quietly=TRUE)
  library(stringplus, quietly=TRUE)
})

set_string_ops("&", "|")

args <- commandArgs(trailingOnly = TRUE)
algo <- args[1]
nthreads <- args[2] %>% as.numeric
file_path <- args[3] # must be full path not relative
output_temp_file <- args[4] # write results to here

now <- function() assign(".time", Sys.time(), envir = globalenv())
later <- function() { as.numeric(Sys.time() - get(".time", envir = globalenv()), units = "secs") }

# now()
# x <- file.copy(file_path, copy_path)
# copy_time <- later()
if(algo == "base_serialize") {
  now()
  con <- file(file_path, "rb")
  unserialize(con)
  close(con)
  read_time <- later()
} else if(algo == "rds") {
  now()
  x <- readRDS(file = file_path)
  read_time <- later()
} else if(algo == "qs-legacy") {
  suppressPackageStartupMessages( library(qs, quietly=TRUE) )
  now()
  x <- qs::qread(file_path, nthreads = nthreads)
  read_time <- later()
} else if(algo == "qs2") {
  suppressPackageStartupMessages( library(qs2, quietly=TRUE) )
  now()
  x <- qs2::qs_read(file_path, validate_checksum = FALSE, nthreads = nthreads)
  read_time <- later()
} else if(algo == "qdata") {
  suppressPackageStartupMessages( library(qs2, quietly=TRUE) )
  now()
  x <- qs2::qd_read(file_path, validate_checksum = FALSE, nthreads = nthreads)
  read_time <- later()
} else if(algo == "fst") {
  suppressPackageStartupMessages( library(fst, quietly=TRUE) )
  fst::threads_fst(nr_of_threads = nthreads)
  now()
  x <- fst::read_fst(path = file_path)
  read_time <- later()
} else if(algo == "parquet") {
  suppressPackageStartupMessages( library(arrow, quietly=TRUE) )
  options(arrow.use_altrep = FALSE) # don't use ALTREP/mmap for benchmark
  arrow::set_cpu_count(num_threads = nthreads)
  now()
  x <- arrow::read_parquet(file = file_path, mmap = FALSE)
  read_time <- later()
}

writeLines(text = c(read_time) %>% as.character, con = output_temp_file)

