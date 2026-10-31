# Third-Party Notices

This repository includes vendored or adapted code from the following upstream
projects.

## xxHash

- Upstream project: <https://github.com/Cyan4973/xxHash>
- Vendored files:
  - `include/xxhash/xxhash.h`
  - `include/xxhash/xxhash.c`
- License: BSD 2-Clause
- Local license text: [LICENSES/xxHash-BSD-2-Clause.txt](LICENSES/xxHash-BSD-2-Clause.txt)

## Blosc

`include/shuffle/byte_shuffle.h` writes the byte-shuffle layout of the Blosc
shuffle filter (<https://github.com/Blosc/c-blosc>), which earlier versions
adapted from Blosc's code. It no longer contains Blosc code.

## Notes

- The project itself is distributed under the GNU General Public License,
  version 3. See [LICENSE](LICENSE).
