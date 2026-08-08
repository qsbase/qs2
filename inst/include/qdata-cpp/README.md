# qdata-cpp

[![Linux CI](https://github.com/qsbase/qdata-cpp/actions/workflows/linux.yml/badge.svg)](https://github.com/qsbase/qdata-cpp/actions/workflows/linux.yml)
[![macOS CI](https://github.com/qsbase/qdata-cpp/actions/workflows/macos.yml/badge.svg)](https://github.com/qsbase/qdata-cpp/actions/workflows/macos.yml)
[![Windows CI](https://github.com/qsbase/qdata-cpp/actions/workflows/windows.yml/badge.svg)](https://github.com/qsbase/qdata-cpp/actions/workflows/windows.yml)

`qdata-cpp` is the standalone C++17 library for the qdata format.

It is part of the `qs2` R package for efficient serialization, but is useful well beyond: in C++, qdata gives you a flexible, intuitive model for variable data structures, with very strong compression and performance characteristics.

### Usage

```cpp
#include "qdata.h"
#include <iostream>
#include <vector>
#include <string>

using namespace qdata;

real_vector read_rvec(const std::string& file) {
    return get<real_vector>(read(file));
}

int main() {
    std::vector<double> obj{1.5, 2.5, 3.5};
    save("values.qdata", obj);

    auto rvec = read_rvec("values.qdata");

    for(double x : rvec) std::cout << x << " ";
    std::cout << std::endl;
}
```

`qdata-cpp` is header-only for standalone use. Copy `include/*` to your project and link against `zstd` and `TBB`.

`zstd` is required but `TBB` is optional only if you want multithreading.

## Testing

The test suite is driven through `Rscript` and requires the `qs2`, `stringi`,
and `stringfish` packages when `QDATA_BUILD_TESTS=ON`.

Multithreading requires oneTBB 2021.1 or newer (`TBB_INTERFACE_VERSION` 12000
and up). Classic TBB, through 2020.3, spelled the flow-graph source as
`source_node` rather than `input_node` and is not supported; configure with
`-DQDATA_USE_TBB=OFF` to build single-threaded against an older toolchain.

## Core qdata types

qdata is built around a small set of data types:

- raw bytes
- logical values
- 32-bit values (int32_t)
- 64-bit values (double)
- 128-bit values (std::complex)
- strings
- nested lists plus named attributes built from those types

It is not a general "serialize any C++ object graph" library. Instead, custom types map onto these data types through traits.

## Custom type example

qdata does not need a native representation for every C++ POD type. Any same-size trivially copyable type can be mapped onto an existing qdata type with traits.

```cpp
#include "qdata.h"
#include <cstdint>
#include <vector>
#include <bit>

using namespace qdata;
template <>
struct qdata::REALSXP_traits<std::vector<uint64_t>> {
    static constexpr bool direct = false;
    static size_t size(const std::vector<uint64_t>& x) { return x.size(); }
    static double get(const std::vector<uint64_t>& x, size_t i) {
        return std::bit_cast<double>(x[i]); // bit_cast requires C++20
    }
};

int main() {
    real_vector out = get<real_vector>(read("values.qdata"));
    uint64_t first = std::bit_cast<uint64_t>(out[0]);
}
```

The same pattern works for any same-size POD mapping. The qdata type is only acting as 4-byte, 8-byte, or 16-byte storage; the bytes are copied or cast back on read.

If you have C++20 `std::bit_cast` is clean, if not, you can achieve the same effect with `std::memcpy`. Don't `reinterpret_cast` as that is UB.

The complete reference on format and traits can be found in [docs/qdata_spec.md](docs/qdata_spec.md).


## Mixed type list example

Heterogeneous lists use `qdata::writable`, which can either borrow values with `ref(...)` or own them with `own(...)`.

```cpp
#include "qdata.h"
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

using namespace qdata;

int main() {
    std::vector<writable> x = {
        writable::own(std::vector<int32_t>{1, 2, 3}),
        writable::own(std::vector<double>{1.5, 2.5}),
        writable::own(std::vector<std::string>{"left", "right"})
    };

    save("mixed.qdata", x);

    object obj = read("mixed.qdata");
    const list_vector& out = get<list_vector>(obj);

    const integer_vector& ints = get<integer_vector>(out[0]);
    const real_vector& reals = get<real_vector>(out[1]);
    const string_vector& strings = get<string_vector>(out[2]);

    std::cout << ints[0] << " " << reals[0] << " " << strings[0] << "\n";
}
```

## In-memory serialization

qdata also gives in-memory serialization with `serialize()` and `deserialize()`. The default container is `std::vector<std::byte>`, but any other contiguous, resizable 1-byte container such as `std::vector<char>` also works via templates.

```cpp
#include "qdata.h"
#include <iostream>
#include <vector>

using namespace qdata;

int main() {
    std::vector<double> x{1.5, 2.5, 3.5};
    std::vector<char> bytes = serialize<std::vector<char>>(x);
    object obj = deserialize(bytes);
}
```

## Bindings in R and Python

qdata is not just a C++ format. It is also available from R and Python, which makes it useful as a compact interchange layer across data workflows.

That means you can write qdata in one language, inspect or transform it in another, and keep the same underlying typed structure with full fidelity.

That combination of speed, flexible structure, and cross-language availability is what makes qdata useful both as a standalone C++ format and as a bridge between data tools.
