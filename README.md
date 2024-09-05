qs2
================

[![R-CMD-check](https://github.com/traversc/qs2/workflows/R-CMD-check/badge.svg)](https://github.com/traversc/qs2/actions)
[![CRAN-Status-Badge](http://www.r-pkg.org/badges/version/qs2)](https://cran.r-project.org/package=qs2)
[![CRAN-Downloads-Badge](https://cranlogs.r-pkg.org/badges/qs2)](https://cran.r-project.org/package=qs2)
[![CRAN-Downloads-Total-Badge](https://cranlogs.r-pkg.org/badges/grand-total/qs2)](https://cran.r-project.org/package=qs2)

*qs2: a framework for efficient serialization*

`qs2` is the successor to the `qs` package. The goal is to have reliable
and fast performance for saving and loading objects in R.

The `qs2` format directly uses R serialization (via the
`R_Serialize`/`R_Unserialize` C API) while improving underlying
compression and disk IO patterns. If you are familiar with the `qs`
package, the benefits and usage are the same.

``` r
qs_save(data, "myfile.qs2")
data <- qs_read("myfile.qs2")
```

Use the file extension `qs2` to distinguish it from the original `qs`
package. It is not compatible with the original `qs` format.

## Installation

``` r
install.packages("qs2")
```

On Mac or Linux, you can enable multi-threading by compiling from
source. It is enabled by default on Windows.

``` r
remotes::install_cran("qs2", type = "source", configure.args = " --with-TBB --with-simd=AVX2")
```

Multi-threading in `qs2` uses the `Intel Thread Building Blocks`
framework via the `RcppParallel` package.

## Converting qs2 to RDS

Because the `qs2` format directly uses R serialization, you can convert
it to RDS and vice versa.

``` r
file_qs2 <- tempfile(fileext = ".qs2")
file_rds <- tempfile(fileext = ".RDS")
x <- runif(1e6)

# save `x` with qs_save
qs_save(x, file_qs2)

# convert the file to RDS
qs_to_rds(input_file = file_qs2, output_file = file_rds)

# read `x` back in with `readRDS`
xrds <- readRDS(file_rds)
stopifnot(identical(x, xrds))
```

## Validating file integrity

The `qs2` format saves an internal checksum. This can be used to test
for file corruption before deserialization, but has a minor performance
penality.

``` r
qs_save(data, "myfile.qs2")
data <- qs_read("myfile.qs2", validate_checksum = TRUE)
```

# The qdata format

The package also introduces the `qdata` format which has its own
serialization layout and works with only data types (vectors, lists,
data frames, matrices).

It will replace internal types (functions, promises, external pointers,
environments, objects) with NULL. The `qdata` format differs from the
`qs2` format in that it is NOT a general.

The eventual goal of `qdata` is to also have interoperability with other
languages, particularly `Python`.

``` r
qd_save(data, "myfile.qs2")
data <- qd_read("myfile.qs2")
```

## Benchmarks

A summary across 4 datasets is presented below.

#### Single-threaded

| Algorithm       | nthreads | Save Time | Read Time | Compression |
| --------------- | -------- | --------- | --------- | ----------- |
| saveRDS         | 1        | 104.46    | 60.10     | 8.38        |
| base::serialize | 1        | 8.91      | 46.99     | 1.12        |
| qs2             | 1        | 13.02     | 48.69     | 8.10        |
| qdata           | 1        | 10.31     | 45.13     | 8.80        |
| qs-legacy       | 1        | 9.10      | 44.84     | 7.42        |

#### Multi-threaded

| Algorithm | nthreads | Save Time | Read Time | Compression |
| --------- | -------- | --------- | --------- | ----------- |
| qs2       | 8        | 3.64      | 43.87     | 8.10        |
| qdata     | 8        | 2.12      | 41.72     | 8.80        |
| qs-legacy | 8        | 3.34      | 47.90     | 7.42        |

  - `qs2`, `qdata` and `qs` used `compress_level = 3`
  - `base::serialize` was run with `ascii = FALSE` and `xdr = FALSE`

**Datasets**

  - `1000 genomes non-coding VCF` 1000 genomes non-coding variants
  - `B-cell data` B-cell mouse data (Greiff 2017)
  - `IP location` IPV4 range data with location information
  - `Netflix movie ratings` Netflix Prize open competition machine
    learning dataset

These datasets are openly licensed and represent a combination of
numeric and text data across multiple domains. See `inst/benchmarks` on
Github.
