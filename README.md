qs2
================

[![R-CMD-check](https://github.com/traversc/qs2/workflows/R-CMD-check/badge.svg)](https://github.com/traversc/qs2/actions)
[![CRAN-Status-Badge](http://www.r-pkg.org/badges/version/qs2)](https://cran.r-project.org/package=qs2)
[![CRAN-Downloads-Badge](https://cranlogs.r-pkg.org/badges/qs2)](https://cran.r-project.org/package=qs2)
[![CRAN-Downloads-Total-Badge](https://cranlogs.r-pkg.org/badges/grand-total/qs2)](https://cran.r-project.org/package=qs2)

# Still in development

*qs2: a framework for efficient serialization*

`qs2` will be the successor the `qs` package. The goal is to have
cutting edge serialization performance in R.

Two new formats are introduced: the `qs2` and `qdata` formats.

The `qs2` format directly uses R’s serialization functions, so it should
be fully compatible with all future version of R. For most people, this
is what you want to use. It’s a drop in replacement for `qs::qsave` or
`saveRDS`.

The `qdata` format has its own layout and works with only R data types
(vectors, lists, data frames, matrices). It will replace internal types
(functions, promises, external pointers, environments) with NULL. This
has slightly better performance but is not general.

## Usage

``` r
library(qs2)

qs_save(data, "myfile.qs2")
data <- qs_read("myfile.qs2")

qd_save(data, "myfile.qd")
data <- qd_read("myfile.qd")
```

## Benchmarks

TO DO: description of datasets used.

See `inst/benchmarks` for more details and a comparison to other
libraries.

### enwik8

![](inst/benchmarks/plots/enwik8_benchmark_plot.png "enwik8_bench")

### mnist

![](inst/benchmarks/plots/mnist_benchmark_plot.png "mnist_bench")

### gaia

![](inst/benchmarks/plots/gaia_benchmark_plot.png "gaia_bench")
