SHELL   := /bin/bash
PACKAGE := $(shell perl -aF: -ne 'print, exit if s/^Package:\s+//' DESCRIPTION)
VERSION := $(shell perl -aF: -ne 'print, exit if s/^Version:\s+//' DESCRIPTION)
BUILD   := $(PACKAGE)_$(VERSION).tar.gz

.PHONY: doc build install test vignette $(BUILD)

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
	find . -iname "*.a" -exec rm {} \;
	find . -iname "*.o" -exec rm {} \;
	find . -iname "*.so" -exec rm {} \;
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
	find . -iname "*.a" -exec rm {} \;
	find . -iname "*.o" -exec rm {} \;
	find . -iname "*.so" -exec rm {} \;
	R CMD build . # --no-build-vignettes
	R CMD INSTALL $(BUILD) --configure-args="--with-simd=AVX2 --with-TBB"

install-blocktest:
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
	find . -iname "*.a" -exec rm {} \;
	find . -iname "*.o" -exec rm {} \;
	find . -iname "*.so" -exec rm {} \;
	R CMD build . --no-build-vignettes
	R CMD INSTALL $(BUILD) --configure-args="--with-simd=AVX2 --with-TBB --with-qsblocksize=$(BLOCKSIZE)"

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
	find . -iname "*.a" -exec rm {} \;
	find . -iname "*.o" -exec rm {} \;
	find . -iname "*.so" -exec rm {} \;
	R CMD build . --no-build-vignettes
	R CMD INSTALL $(BUILD) --configure-args="--with-zstd-force-compile"

vignette:
	Rscript -e "rmarkdown::render(input='vignettes/vignette.rmd', output_format='html_vignette')"
	IS_GITHUB=Yes Rscript -e "rmarkdown::render(input='vignettes/vignette.rmd', output_file='../README.md', output_format=rmarkdown::github_document(html_preview=FALSE))"; unset IS_GITHUB
	# mv vignettes/vignette.md README.md
	# sed -r -i 's/\((.+)\.png/\(vignettes\/\1\.png/' README.md

test:
	QS_EXTENDED_TESTS=1 Rscript tests/correctness_testing.R; unset QS_EXTENDED_TESTS
