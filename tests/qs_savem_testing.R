library(qs2)

cat("testing qs_savem and qs_readm\n")

expect_equal <- function(x, y) {
    stopifnot(identical(x, y))
}

file <- tempfile()

seurat <- 1
lineages <- 2
T.markers <- 3

qs_savem(file = file, seurat, lineages, T.markers)

rm(seurat, lineages, T.markers)
qs_readm(file = file)
expect_equal(c(seurat, lineages, T.markers), c(1, 2, 3))

# alternate synatx
qs_savem(seurat, lineages, T.markers, file = file)

rm(seurat, lineages, T.markers)
qs_readm(file = file)
expect_equal(c(seurat, lineages, T.markers), c(1, 2, 3))

# alternate synatx
qs_savem(seurat, lineages, file = file, T.markers)

rm(seurat, lineages, T.markers)
qs_readm(file = file)
expect_equal(c(seurat, lineages, T.markers), c(1, 2, 3))
