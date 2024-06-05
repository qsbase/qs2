# general and plotting
library(data.table)
library(dplyr)
library(this.path)
library(stringplus)
set_string_ops("&", "|")

# libraries to benchmark
library(qs)
library(qs2)
library(fst)
library(arrow)

# timing helper functions
now <- function() assign(".time", Sys.time(), envir = globalenv())
later <- function() { as.numeric(Sys.time() - get(".time", envir = globalenv()), units = "secs") }

# Datasets for benchmarking
DATA_PATH <- "/mnt/n/R_stuff"
TMP_RESULTS_PATH <- tempfile()
OUTPUT_PATH <- "/mnt/n/temp_qs2_bench_results"

for(DATASET in c("enwik8", "gaia", "mnist")) {
if(DATASET == "enwik8") {
  DATA <- fread(DATA_PATH & "/enwik8.csv.gz", sep = ",", data.table=FALSE, colClasses = "character")
} else if(DATASET == "gaia") {
  DATA <- fread(DATA_PATH & "/gaia_pseudocolor.csv.gz", data.table=FALSE, colClasses = "numeric")
} else if(DATASET == "mnist") {
  library(dslabs)
  DATA <- dslabs::read_mnist()
}

reps <- 1:3
compress_levels <- c(1,3,5,7,9,11)
fst_compress_levels <- seq(10,85,by=15)
grid <- rbind(expand.grid(algo = "qs-legacy", compress_level = compress_levels, nthreads=c(1,4), rep=reps, stringsAsFactors = FALSE),
              expand.grid(algo = "qs2", compress_level = compress_levels, nthreads=c(1,4), rep=reps, stringsAsFactors = FALSE),
              expand.grid(algo = "qdata", compress_level = compress_levels, nthreads=c(1,4), rep=reps, stringsAsFactors = FALSE),
              expand.grid(algo = "fst", compress_level = fst_compress_levels, nthreads=c(1,4), rep = reps, stringsAsFactors = FALSE),
              expand.grid(algo = "parquet", compress_level = compress_levels, nthreads=c(1,4), rep=reps, stringsAsFactors = FALSE)) %>% sample_n(nrow(.))

grid <- filter(grid, algo %in% c("qs-legacy", "qs2", "qdata"))
# if(DATASET == "mnist") grid <- filter(grid, algo %in% c("qs-legacy", "qs2", "qdata"))

res <- lapply(1:nrow(grid), function(i) {
  print(i)
  print(grid[i,])
  algo <- grid$algo[i]
  cl <- grid$compress_level[i]
  nthreads <- grid$nthreads[i]
  rep <- grid$rep[i]
  
  output_file <- "%s/%s_%s_cl%s_nt%s_rep%s.out" | c(OUTPUT_PATH, DATASET, algo, cl, nthreads, rep)
  
  now()
  if(algo == "qs-legacy") {
    qs::qsave(DATA, file = output_file, preset = "custom", algorithm = "zstd", compress_level = cl, nthreads = nthreads, check_hash = FALSE)
  } else if(algo == "qs2") {
    qs2::qs_save(DATA, file = output_file, compress_level = cl, nthreads = nthreads)
  } else if(algo == "qdata") {
    qs2::qd_save(DATA, file = output_file, compress_level = cl, nthreads = nthreads)
  } else if(algo == "fst") {
    fst::threads_fst(nr_of_threads = nthreads)
    fst::write_fst(DATA, path = output_file, compress = cl)
  } else if(algo == "parquet") {
    arrow::set_cpu_count(num_threads = nthreads)
    arrow::write_parquet(DATA, sink = output_file, compression = "zstd", compression_level = cl)
  }
  save_time <- later()
  fsize <- file.info(output_file)[1,"size"] / 1048576
  
  system("Rscript %s/read_timer_function.R %s %s %s %s" | c(this.dir(), algo, nthreads, output_file, TMP_RESULTS_PATH))
  rctime <- readLines(TMP_RESULTS_PATH) %>% as.numeric
  # invisible(file.remove(OUTPUT_PATH))
  invisible(file.remove(TMP_RESULTS_PATH))
  
  data.frame(save_time, read_time = rctime[1], file_size = fsize)
}) %>% rbindlist
res <- cbind(grid, res)

fwrite(res, file = this.dir() & "/" & DATASET & "_serialization_benchmarks.csv", sep = ",")

}
