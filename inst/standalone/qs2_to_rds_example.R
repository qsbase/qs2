# A minimal standalone Rcpp converter for qs2 -> RDS without package dependency
# Requires linkage to zstd system library, shown below for mac/linux
Sys.setenv(
  PKG_CPPFLAGS = system("pkg-config --cflags libzstd", intern = TRUE),
  PKG_LIBS     = system("pkg-config --libs libzstd", intern = TRUE)
)
Rcpp::sourceCpp("qs2_to_rds.cpp")

# Create an example qs2 file
temp_qs2 <- tempfile(fileext = ".qs2")
temp_rds <- tempfile(fileext = ".RDS")
N <- 1e6
ex <- data.frame(x = rep(rownames(mtcars), length.out=N), y = rnorm(N))
qs2::qs_save(ex, file = temp_qs2)

# Call the conversion function
# This function does not depend on qs2
standalone_qs2_to_rds(temp_qs2, temp_rds)

# check identical
stopifnot(identical(readRDS(temp_rds), ex))