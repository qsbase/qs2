CMAKE ?= cmake
CTEST ?= ctest
BUILD_DIR ?= build
CONFIGURE_FLAGS ?= -DCMAKE_BUILD_TYPE=Release -DQDATA_BUILD_TESTS=ON -DQDATA_BUILD_EXAMPLES=ON
CTEST_FLAGS ?= --output-on-failure -V
QS_EXTENDED_TESTS ?= 1
ROWS ?=
REPS ?=
LICENSE_DIR ?= LICENSES

BENCH_ARGS :=
ifneq ($(strip $(ROWS)),)
BENCH_ARGS += $(ROWS)
endif
ifneq ($(strip $(REPS)),)
BENCH_ARGS += $(REPS)
endif

.PHONY: all configure example bench benchmark benchmark-build test test-extended get-license-files clean distclean

all: configure
	$(CMAKE) --build $(BUILD_DIR)

configure:
	$(CMAKE) -S . -B $(BUILD_DIR) $(CONFIGURE_FLAGS)

example: configure
	$(CMAKE) --build $(BUILD_DIR) --target qdata_mtcars_roundtrip

benchmark-build: configure
	$(CMAKE) --build $(BUILD_DIR) --target qdata_benchmark

# make benchmark ROWS=1000 REPS=1
benchmark: benchmark-build
	./$(BUILD_DIR)/qdata_benchmark $(BENCH_ARGS)

test: configure
	$(CMAKE) --build $(BUILD_DIR)
	$(CTEST) --test-dir $(BUILD_DIR) $(CTEST_FLAGS)

test-extended: configure
	$(CMAKE) --build $(BUILD_DIR)
	QS_EXTENDED_TESTS=$(QS_EXTENDED_TESTS) $(CTEST) --test-dir $(BUILD_DIR) $(CTEST_FLAGS)

get-license-files:
	@mkdir -p $(LICENSE_DIR)
	curl -fsSL https://www.gnu.org/licenses/gpl-3.0.txt -o LICENSE
	curl -fsSL https://raw.githubusercontent.com/Cyan4973/xxHash/dev/LICENSE -o $(LICENSE_DIR)/xxHash-BSD-2-Clause.txt
	curl -fsSL https://raw.githubusercontent.com/Blosc/c-blosc/main/LICENSE.txt -o $(LICENSE_DIR)/BLOSC-BSD-3-Clause.txt

clean:
	@if [ -d "$(BUILD_DIR)" ]; then $(CMAKE) --build $(BUILD_DIR) --target clean; fi

distclean:
	$(CMAKE) -E rm -rf $(BUILD_DIR)
