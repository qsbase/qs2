library(ggplot2)
library(ggrepel)
library(patchwork)
library(data.table)
library(dplyr)
library(this.path)
library(stringplus)
set_string_ops("&", "|")

options(warn=1)

PLATFORM <- "windows"
DATA_PATH <- "N:/datasets_qs2/processed"
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
  group_by(algo, nthreads) %>%
  summarize(compression = ( sum(obj_size)/sum(file_size) ) %>% signif(3),
            save_time = sum(save_time) %>% signif(3), 
            read_time = sum(read_time) %>% signif(3)) %>%
  arrange(nthreads, algo) %>%
  fwrite(sep =",")

# algo,nthreads,compression,save_time,read_time
# base_serialize,1,1.1,8.87,51.4
# fst,1,2.59,5.09,46.3
# parquet,1,8.29,20.3,38.4
# qdata,1,8.45,10.5,34.8
# qs-legacy,1,7.97,9.13,48.1
# qs2,1,7.96,13.4,50.4
# rds,1,8.68,107,63.7
# fst,8,2.59,5.05,46.6
# parquet,8,8.29,20.2,37
# qdata,8,8.45,1.98,33.1
# qs-legacy,8,7.97,3.21,52
# qs2,8,7.96,3.79,48.1

dfs2 %>%
  as.data.frame %>%
  dplyr::select(dataset, obj_size) %>% unique

# dataset  obj_size
# 1 1000genomes_noncoding_vcf 2743.4075
# 2      B_cell_petshopmouse3 1057.2301
# 3           Netflix_Ratings  198.4124
# 4          ip_location_2023  570.7996
