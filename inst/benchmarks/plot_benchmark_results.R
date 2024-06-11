library(ggplot2)
library(ggrepel)
library(patchwork)
library(data.table)
library(dplyr)
library(this.path)
library(stringplus)
set_string_ops("&", "|")

options(warn=1)

PLATFORM <- "mac"
algo_subset <- c("qs-legacy", "qs2", "qdata")
for(DATASET in c("enwik8", "gaia", "mnist", "tcell")) {
print(DATASET)
df <- fread("%s/results/%s_serialization_benchmarks_%s.csv" | c(this.dir(), PLATFORM, DATASET)) %>%
  filter(algo %in% algo_subset)
df$algo <- factor(df$algo, levels = algo_subset)

dfs <- df %>% group_by(algo, nthreads, compress_level) %>%
  summarize(file_size = unique(file_size), save_time = median(save_time), read_time = median(read_time))

g1 <- ggplot(df, aes(x = file_size, y = save_time, color = algo, shape = factor(nthreads))) + 
  geom_point() + 
  geom_line(data = dfs, aes(lty = factor(nthreads))) + 
  # scale_x_log10() + 
  scale_y_log10() + 
  scale_color_manual(values = palette.colors(palette = "Okabe-Ito")) +
  labs(color = "Format", lty = "Threads", pch = "Threads",
       x = "File Size (MB)", y = "Save Time (s)") + 
  theme_bw(base_size=11) + 
  guides(color = guide_legend(order = 1), 
         pch = guide_legend(order = 2),
         lty = guide_legend(order = 2))
  

g2 <- ggplot(df, aes(x = save_time, y = read_time, color = algo, shape = factor(nthreads))) + 
  geom_point() + 
  geom_line(data = dfs, aes(lty = factor(nthreads))) + 
  scale_x_log10() + scale_y_log10() +
  scale_color_manual(values = palette.colors(palette = "Okabe-Ito")) +
  labs(color = "Format", lty = "Threads", pch = "Threads",
       y = "Read Time (s)", x = "Save Time (s)") + 
  theme_bw(base_size=11) + 
  guides(color = guide_legend(order = 1),
         pch = guide_legend(order = 2),
         lty = guide_legend(order = 2))

g <- g1 + g2 + patchwork::plot_layout(guides = "collect")

ggsave(g, file = "%s/plots/%s_%s_benchmark_plot.png" | c(this.dir(), PLATFORM, DATASET), width = 8, height = 3.75)

}

################################################################################
# total across 4 datasets

df <- lapply(c("enwik8", "gaia", "mnist", "tcell"), function(DATASET) {
  fread("%s/results/%s_serialization_benchmarks_%s.csv" | c(this.dir(), PLATFORM, DATASET)) %>%
    filter(algo %in% algo_subset) %>%
    mutate(dataset = DATASET)
}) %>% rbindlist
df$algo <- factor(df$algo, levels = algo_subset)

df <- df %>% 
  group_by(algo, nthreads, compress_level, rep) %>%
  summarize(file_size = sum(file_size), save_time = sum(save_time), read_time = sum(read_time))

dfs <- df %>%
  group_by(algo, nthreads, compress_level) %>%
  summarize(file_size = unique(file_size), save_time = median(save_time), read_time = median(read_time))


g1 <- ggplot(df, aes(x = file_size, y = save_time, color = algo, shape = factor(nthreads))) + 
  geom_point() + 
  geom_line(data = dfs, aes(lty = factor(nthreads))) + 
  # scale_x_log10() + 
  scale_y_log10() + 
  scale_color_manual(values = palette.colors(palette = "Okabe-Ito")) +
  labs(color = "Format", lty = "Threads", pch = "Threads",
       x = "File Size (MB)", y = "Save Time (s)") + 
  theme_bw(base_size=11) + 
  guides(color = guide_legend(order = 1), 
         pch = guide_legend(order = 2),
         lty = guide_legend(order = 2))


g2 <- ggplot(df, aes(x = save_time, y = read_time, color = algo, shape = factor(nthreads))) + 
  geom_point() + 
  geom_line(data = dfs, aes(lty = factor(nthreads))) + 
  scale_x_log10() + scale_y_log10() +
  scale_color_manual(values = palette.colors(palette = "Okabe-Ito")) +
  labs(color = "Format", lty = "Threads", pch = "Threads",
       y = "Read Time (s)", x = "Save Time (s)") + 
  theme_bw(base_size=11) + 
  guides(color = guide_legend(order = 1),
         pch = guide_legend(order = 2),
         lty = guide_legend(order = 2))

g <- g1 + g2 + patchwork::plot_layout(guides = "collect")

ggsave(g, file = "%s/plots/%s_combined_benchmark_plot.png" | c(this.dir(), PLATFORM), width = 8, height = 3.75)

################################################################################
# Summary table

# object.size(DATA)
# enwik8: 146242320
# gaia: 288986496
# mnist: 219801584
# tcell: 293786648

# obj_size <- data.frame(dataset = c("enwik8", "gaia", "mnist", "tcell"), 
#                        object_size = c(146242320, 288986496, 219801584, 293786648)) %>%
#   mutate(object_size = object_size / 1048576)

total_size <- (146242320 + 288986496 + 293786648) / 1048576

algo_subset <- c("qs2", "qdata","qs-legacy", "rds", "base_serialize", "fst", "parquet")
df <- lapply( c("enwik8", "gaia", "tcell"), function(DATASET) {
  fread("%s/results/%s_serialization_benchmarks_%s.csv" | c(this.dir(), PLATFORM, DATASET)) %>%
    filter(algo %in% algo_subset) %>%
    # filter for defaults
    filter( (algo %like% "^q" & compress_level == 5) |
            (algo == "rds") | 
            (algo == "base_serialize") | 
            (algo == "fst" & compress_level  == 55) |
            (algo == "parquet" & compress_level == 5)) %>%
    mutate(dataset = DATASET)
}) %>% rbindlist

df <- df %>% 
  group_by(algo, nthreads, rep) %>%
  summarize(file_size = sum(file_size), save_time = sum(save_time), read_time = sum(read_time))

df <- df %>%
  mutate(algo = factor(algo, levels = algo_subset))

dfs <- df %>%
  group_by(algo, nthreads) %>%
  summarize(file_size = unique(file_size), save_time = median(save_time), read_time = median(read_time)) %>%
  ungroup %>%
  mutate(compression = total_size / file_size) %>%
  arrange(nthreads, algo) %>%
  mutate(compression = total_size / file_size) %>%
  transmute(algorithm=algo, nthreads, save_time, read_time, compression)

fwrite(dfs, file = "results/%s_benchmark_summary_table.csv" | PLATFORM, sep = ",")
