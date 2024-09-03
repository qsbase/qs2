library(ggplot2)
library(ggrepel)
library(patchwork)
library(data.table)
library(dplyr)
library(this.path)
library(stringplus)
set_string_ops("&", "|")

options(warn=1)

PLATFORM <- "ubuntu"
DATA_PATH <- "~/datasets/processed"
datasets <- DATA_PATH & "/" & c("1000genomes_noncoding_vcf.csv.gz", "B_cell_petshopmouse3.tsv.gz",
                                "ip_location_2023.csv.gz", "Netflix_Ratings.csv.gz")
read_dataset <- function(d) {
  if(d %like% "json.gz") {
    DATA <- RcppSimdJson::fload(d)
  } else if(d %like% ".csv.gz") {
    DATA <- fread(d, sep = ",", data.table=F)
  } else if(d %like% ".tsv.gz") {
    DATA <- fread(d, sep = "\t", data.table=F)
  } else {
    DATA <- readRDS(d)
  }
}

df <- lapply(datasets, function(DATASET) {
  obj_size <- read_dataset(DATASET) %>% object.size() %>% as.numeric %>% {. / 1048576}
  DATASET_NAME <- basename(DATASET) %>% gsub("\\..+", "", .)
  print(DATASET_NAME)
  fread("%s/results/%s_serialization_benchmarks_%s.csv" | c(this.dir(), PLATFORM, DATASET_NAME)) %>%
    mutate(dataset = DATASET_NAME, obj_size = obj_size)
}) %>% rbindlist

dfs <- df %>% 
  group_by(algo, nthreads, compress_level, dataset, obj_size) %>%
  summarize(file_size = max(file_size), save_time = median(save_time), read_time = median(read_time))

g1 <- ggplot(df %>% filter(algo %like% "^q" | algo == "parquet"), 
             aes(x = file_size, y = save_time, color = algo, shape = factor(nthreads))) + 
  geom_point() + 
  geom_line(data = dfs %>% filter(algo %like% "^q" | algo == "parquet"), 
            aes(lty = factor(nthreads))) + 
  scale_x_log10() +
  scale_y_log10() + 
  facet_wrap(~dataset, scales="free") +
  scale_color_manual(values = palette.colors(palette = "Okabe-Ito")) +
  labs(color = "Format", lty = "Threads", pch = "Threads",
       x = "File Size (MB)", y = "Save Time (s)") + 
  theme_bw(base_size=11) + 
  guides(color = guide_legend(order = 1), 
         pch = guide_legend(order = 2),
         lty = guide_legend(order = 2))
  

g2 <- ggplot(df %>% filter(algo %like% "^q" | algo == "parquet"), 
             aes(x = save_time, y = read_time, color = algo, shape = factor(nthreads))) + 
  geom_point() + 
  geom_line(data = dfs %>% filter(algo %like% "^q" | algo == "parquet"), 
            aes(lty = factor(nthreads))) + 
  scale_x_log10() +
  scale_y_log10() + 
  facet_wrap(~dataset, scales="free") +
  scale_color_manual(values = palette.colors(palette = "Okabe-Ito")) +
  labs(color = "Format", lty = "Threads", pch = "Threads",
       x = "Save Time (s)", y = "Read Time (s)") + 
  theme_bw(base_size=11) + 
  guides(color = guide_legend(order = 1), 
         pch = guide_legend(order = 2),
         lty = guide_legend(order = 2))

ggsave(g1, file = "plots/%s_write_benchmarks.png" | PLATFORM, width = 7, height = 5)
ggsave(g2, file = "plots/%s_read_benchmarks.png" | PLATFORM, width = 7, height = 5)

################################################################################
# Summary table


dfs2 <- dfs %>%
    filter( (algo %like% "^q" & compress_level == 3) |
            (algo == "rds") | 
            (algo == "base_serialize") |
            (algo == "fst" & compress_level  == 50) |
            (algo == "parquet" & compress_level == 3))

dfs2 %>%
  mutate(compression = obj_size / file_size) %>%
  group_by(algo, nthreads) %>%
  summarize(compression = mean(compression), save_time = sum(save_time), read_time = sum(read_time)) %>%
  arrange(algo, nthreads) %>% as.data.frame

