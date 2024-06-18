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

setwd(this.dir())

PLATFORM <- "ubuntu" # for results naming purposes

DATA_PATH <- "../../local"
OUTPUT_PATH <- tempdir()
TMP_RESULTS_PATH <- tempfile()

tests_datasets <- c("enwik8") #, "gaia", "tcell", "mnist")
test_algos <- c("qs-legacy", "qs2", "qdata", "fst", "parquet", "rds", "base_serialize")
# test_algos <- c("qs-legacy", "qs2", "qdata")
for(DATASET in tests_datasets) {
if(DATASET == "enwik8") {
  DATA <- fread(DATA_PATH & "/enwik8.csv.gz", sep = ",", data.table=FALSE, colClasses = "character")
} else if(DATASET == "gaia") {
  DATA <- fread(DATA_PATH & "/gaia_pseudocolor.csv.gz", data.table=FALSE, colClasses = "numeric")
} else if(DATASET == "mnist") {
  library(dslabs)
  DATA <- dslabs::read_mnist()
} else if(DATASET == "tcell") {
  DATA <- readRDS(DATA_PATH & "/T_cell_ADIRP0000010.rds")
}
  
# object.size(DATA)
# enwik8: 146242320
# gaia: 288986496
# mnist: 219801584
# tcell: 293786648

reps <- 1:5
compress_levels <- c(1,3,5,7,9,11)
fst_compress_levels <- seq(10,85,by=15)
grid <- rbind(
  data.frame(algo = "rds", compress_level = 1, nthreads = 1, rep=reps),
  data.frame(algo = "base_serialize", compress_level = 1, nthreads = 1, rep=reps),
  expand.grid(algo = "qs-legacy", compress_level = compress_levels, nthreads=c(1,4), rep=reps, stringsAsFactors = FALSE),
  expand.grid(algo = "qs2", compress_level = compress_levels, nthreads=c(1,4), rep=reps, stringsAsFactors = FALSE),
  expand.grid(algo = "qdata", compress_level = compress_levels, nthreads=c(1,4), rep=reps, stringsAsFactors = FALSE),
  expand.grid(algo = "fst", compress_level = fst_compress_levels, nthreads=c(1,4), rep = reps, stringsAsFactors = FALSE),
  expand.grid(algo = "parquet", compress_level = compress_levels, nthreads=c(1,4), rep=reps, stringsAsFactors = FALSE)) %>% sample_n(nrow(.))

grid <- filter(grid, algo %in% test_algos)
if(DATASET == "mnist") grid <- filter(grid, algo %in% c("qs-legacy", "qs2", "qdata")) # mnist is not a dataframe

res <- lapply(1:nrow(grid), function(i) {
  print(i)
  print(grid[i,])
  algo <- grid$algo[i]
  cl <- grid$compress_level[i]
  nthreads <- grid$nthreads[i]
  rep <- grid$rep[i]
  
  output_file <- "%s/%s_%s_cl%s_nt%s_rep%s.out" | c(OUTPUT_PATH, DATASET, algo, cl, nthreads, rep)
  
  now()
  if(algo == "base_serialize") {
    con <- file(output_file, "wb")
    serialize(DATA, con, ascii = FALSE, xdr = FALSE)
    close(con)
  } else if(algo == "rds") {
    saveRDS(DATA, file = output_file)
  } else if(algo == "qs-legacy") {
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
  invisible(file.remove(TMP_RESULTS_PATH))
  invisible(file.remove(output_file))
  
  data.frame(save_time, read_time = rctime[1], file_size = fsize)
}) %>% rbindlist
res <- cbind(grid, res)

output_file <- "%s/results/%s_serialization_benchmarks_%s.csv" | c(this.dir(), PLATFORM, DATASET)
fwrite(res, file = output_file, sep = ",")

}
