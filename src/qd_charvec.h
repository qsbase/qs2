#ifndef _QS2_QD_CHARVEC_H
#define _QS2_QD_CHARVEC_H

#include <Rcpp.h>
#include <charport.h>
#include <R_ext/Altrep.h>

#include <algorithm>
#include <cstdint>

// Fills charport charvecs in place, as qs did with stringfish. qdata stores the
// object tree before the strings, so the deserializer creates each charvec while
// reading the tree and writes its strings into the vector's Store afterwards.
//
// charport 0.1.1 has no public accessor for a charvec's Store: it is the address
// of the external pointer in data1. Nothing may read the charvec before it is
// filled; reading caches elements and materializing frees the Store.

namespace qd_charvec {

struct Factory {
    // A member rather than a local: wrap() can raise an R error before it takes
    // the Store, and that jump skips the destructors of locals.
    charport::charvec::Store pending;

    // Returns an unprotected charvec of NA strings. The charport ABI is checked
    // once, on load (qs2_check_charport_abi).
    SEXP create(uint64_t length) {
        pending = charport::charvec::Store(static_cast<size_t>(length), 0);
        return charport::charvec::wrap(std::move(pending));
    }
};

// Writes strings into a charvec's Store. Payload slices belong to the Store as
// soon as they are allocated and a record is set only after its bytes are read,
// so an error partway leaves a valid vector. Trivially destructible, so an R
// jump past it leaks nothing.
class Writer {
public:
    Writer(SEXP x, uint64_t length)
        : store(*static_cast<charport::charvec::Store*>(R_ExternalPtrAddr(R_altrep_data1(x)))),
          first_slice(std::min<uint64_t>(std::max<uint64_t>(length * 4, 64), 16 << 10)) {}

    // Space for a string of len > 0 bytes; pass it to set() once filled.
    char * reserve(uint32_t len) {
        if(len > slice_left) {
            const size_t grow = std::min<size_t>(std::max<size_t>(first_slice, allocated / 2), 256 << 10);
            const size_t bytes = std::max<size_t>(grow, len);
            slice = store.slices.push_front(bytes);
            slice_left = bytes;
            allocated += bytes;
        }
        char * dest = slice;
        slice += len;
        slice_left -= len;
        return dest;
    }

    void set(uint64_t i, const char * ptr, uint32_t len) {
        store.records.set(static_cast<size_t>(i), ptr, static_cast<int>(len), CETYPE_EXT_ASCII_OR_UTF8);
    }

    void set_empty(uint64_t i) {
        set(i, charport::charvec::components::empty_data(), 0);
    }

private:
    charport::charvec::Store & store;
    const size_t first_slice;
    char * slice = nullptr;
    size_t slice_left = 0;
    size_t allocated = 0;
};

}  // namespace qd_charvec

#endif
