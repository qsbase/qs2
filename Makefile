SHELL   := /bin/bash
PACKAGE := $(shell perl -aF: -ne 'print, exit if s/^Package:\s+//' DESCRIPTION)
VERSION := $(shell perl -aF: -ne 'print, exit if s/^Version:\s+//' DESCRIPTION)
BUILD   := $(PACKAGE)_$(VERSION).tar.gz

.PHONY: doc build compile install test test-short test-qdata-charport vignette $(BUILD)

test-qdata-charport:
	Rscript tests/qdata_charport_testing.R

check: $(BUILD)
	R CMD check --as-cran $<

check-rhub: $(BUILD)
	Rscript -e 'rhub::check("$(BUILD)", platform = c("ubuntu-gcc-devel", "windows-x86_64-devel", "solaris-x86-patched", "solaris-x86-patched-ods", "macos-m1-bigsur-release"))'

check-solaris: $(BUILD)
	Rscript -e 'rhub::check("$(BUILD)", platform = c("solaris-x86-patched", "solaris-x86-patched-ods"))'

check-m1: $(BUILD)
	Rscript -e 'rhub::check("$(BUILD)", platform = c("macos-m1-bigsur-release"))'

reconf:
	autoreconf -fi

doc:
	Rscript -e "roxygen2::roxygenise('.', roclets = 'rd');"

# Compile native code without packaging, examples, vignettes, or tests.
compile:
	./configure
	Rscript -e "Rcpp::compileAttributes('.'); pkgbuild::compile_dll('.', debug = FALSE);"

build:
	find . -type d -exec chmod 755 {} \;
	find . -type f -exec chmod 644 {} \;
	chmod 755 cleanup
	chmod 755 configure
	# find src/ -type f -exec chmod 644 {} \;
	# chmod 644 ChangeLog DESCRIPTION Makefile NAMESPACE README.md
	./configure
	./cleanup
	Rscript -e "library(Rcpp); compileAttributes('.');"
	Rscript -e "devtools::load_all(); roxygen2::roxygenise('.');"
	find src -iname "*.a" -exec rm {} \;
	find src -iname "*.o" -exec rm {} \;
	find src -iname "*.so" -exec rm {} \;
	R CMD build .

install:
	find . -type f -exec chmod 644 {} \;
	find . -type d -exec chmod 755 {} \;
	chmod 755 cleanup
	chmod 755 configure
	# find src/ -type f -exec chmod 644 {} \;
	# chmod 644 ChangeLog DESCRIPTION Makefile NAMESPACE README.md
	./configure
	./cleanup
	Rscript -e "library(Rcpp); compileAttributes('.');"
	Rscript -e "devtools::load_all(); roxygen2::roxygenise('.');"
	find src -iname "*.a" -exec rm {} \;
	find src -iname "*.o" -exec rm {} \;
	find src -iname "*.so" -exec rm {} \;
	R CMD build . # --no-build-vignettes
	R CMD INSTALL $(BUILD)

install-arm:
	find . -type f -exec chmod 644 {} \;
	find . -type d -exec chmod 755 {} \;
	chmod 755 cleanup
	chmod 755 configure
	# find src/ -type f -exec chmod 644 {} \;
	# chmod 644 ChangeLog DESCRIPTION Makefile NAMESPACE README.md
	./configure
	./cleanup
	Rscript -e "library(Rcpp); compileAttributes('.');"
	Rscript -e "devtools::load_all(); roxygen2::roxygenise('.');"
	find src -iname "*.a" -exec rm {} \;
	find src -iname "*.o" -exec rm {} \;
	find src -iname "*.so" -exec rm {} \;
	R CMD build . # --no-build-vignettes
	R CMD INSTALL $(BUILD)


install-compile-zstd:
	find . -type f -exec chmod 644 {} \;
	find . -type d -exec chmod 755 {} \;
	chmod 755 cleanup
	chmod 755 configure
	# find src/ -type f -exec chmod 644 {} \;
	# chmod 644 ChangeLog DESCRIPTION Makefile NAMESPACE README.md
	./configure
	./cleanup
	Rscript -e "library(Rcpp); compileAttributes('.');"
	Rscript -e "devtools::load_all(); roxygen2::roxygenise('.');"
	find src -iname "*.a" -exec rm {} \;
	find src -iname "*.o" -exec rm {} \;
	find src -iname "*.so" -exec rm {} \;
	R CMD build . --no-build-vignettes
	R CMD INSTALL $(BUILD) --configure-args="--with-zstd-force-compile"

vignette:
	Rscript -e "rmarkdown::render(input='vignettes/vignette.rmd', output_format='html_vignette')"
	IS_GITHUB=Yes Rscript -e "rmarkdown::render(input='vignettes/vignette.rmd', output_file='../README.md', output_format=rmarkdown::github_document(html_preview=FALSE))"; unset IS_GITHUB
	# mv vignettes/vignette.md README.md
	# sed -r -i 's/\((.+)\.png/\(vignettes\/\1\.png/' README.md

test-short:
	Rscript tests/qdata_charport_testing.R
	Rscript tests/00_smoke_testing.R
	Rscript tests/fork_tbb_testing.R
	Rscript tests/qs_savem_testing.R
	Rscript tests/utility_testing.R
	Rscript tests/qs2_external_testing.R
	Rscript tests/qdata_cpp_external_testing.R
	Rscript tests/zz_correctness_testing.R

test:
	Rscript tests/qdata_charport_testing.R
	Rscript tests/00_smoke_testing.R
	Rscript tests/fork_tbb_testing.R
	Rscript tests/qs_savem_testing.R
	Rscript tests/utility_testing.R
	Rscript tests/qs2_external_testing.R
	Rscript tests/qdata_cpp_external_testing.R
	QS_EXTENDED_TESTS=1 Rscript tests/zz_correctness_testing.R; unset QS_EXTENDED_TESTS
