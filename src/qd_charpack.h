#ifndef _QS2_QD_CHARPACK_H
#define _QS2_QD_CHARPACK_H

#include <Rcpp.h>
#include <charport.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

// Short character vectors are read as charport charpacks: the strings live in
// one R raw vector, with no external pointer or finalizer per vector. qdata
// stores the object tree before the strings, so the deserializer creates an
// unfilled placeholder while reading the tree and fills it once its strings
// are read.

namespace qd_charpack {

// Vectors of 1 to max_length strings are read as charpacks when use_alt_rep is
// TRUE; longer vectors are read as charvecs.
static constexpr uint64_t max_length = 256;

// A charpack holds its strings twice while it is filled (here and in the raw
// vector). A vector whose strings would exceed max_bytes spills instead: it is
// materialized as an ordinary STRSXP and the remaining strings are read the
// plain way.
static constexpr size_t max_bytes = 1 << 20;

// Collects one vector's strings in slices that are kept across vectors, so a
// read allocates them once, then copies them into the vector with
// fill_slices. Members only, so an R jump while filling leaks nothing.
class Filler {
public:
    // qdata strings are UTF-8; ASCII is resolved when R makes the CHARSXP.
    Filler() {
        std::fill(encodings, encodings + max_length, CETYPE_EXT_ASCII_OR_UTF8);
    }

    void start(uint64_t length) {
        n = static_cast<size_t>(length);
        std::fill(lengths, lengths + n, NA_INTEGER);
        for(size_t k = 0; k <= current && k < slices.size(); ++k) slices[k].used = 0;
        current = 0;
        total = 0;
    }

    // Whether string i of len bytes still fits under max_bytes.
    bool fits(size_t len) const {
        return len <= max_bytes - total;
    }

    void set_empty(uint64_t i) {
        lengths[i] = 0;
    }

    // Space for string i of 0 < len bytes, to be filled by the caller.
    char * set(uint64_t i, uint32_t len) {
        while(current < slices.size() && slices[current].size - slices[current].used < len) ++current;
        if(current == slices.size()) {
            const size_t grow = slices.empty() ? first_slice : std::min<size_t>(slices.back().size * 2, max_slice);
            const size_t size = std::max<size_t>(grow, len);
            slices.push_back(Slice{std::unique_ptr<char[]>(new char[size]), size, 0});
        }
        Slice & s = slices[current];
        char * out = s.data.get() + s.used;
        s.used += len;
        total += len;
        lengths[i] = static_cast<int>(len);
        ptrs[i] = out;
        return out;
    }

    void finish(SEXP x) {
        slice_ptrs.clear();
        slice_bytes.clear();
        for(size_t k = 0; k <= current && k < slices.size(); ++k) {
            if(slices[k].used == 0) continue;
            slice_ptrs.push_back(slices[k].data.get());
            slice_bytes.push_back(slices[k].used);
        }
        charport::charpack::fill_slices(x, static_cast<R_xlen_t>(n), lengths, encodings,
                                        static_cast<R_xlen_t>(slice_ptrs.size()),
                                        slice_ptrs.data(), slice_bytes.data());
    }

    // Copies the strings set so far (indices below i) into materialized, the
    // STRSXP from charport::charpack::fill_materialized_na. The caller must
    // keep materialized protected.
    void spill(SEXP materialized, uint64_t i) {
        for(uint64_t j = 0; j < i; ++j) {
            if(lengths[j] == NA_INTEGER) continue;
            SET_STRING_ELT(materialized, j, lengths[j] == 0 ? R_BlankString
                                                            : Rf_mkCharLenCE(ptrs[j], lengths[j], CE_UTF8));
        }
    }

private:
    static constexpr size_t first_slice = 4096;
    static constexpr size_t max_slice = 1 << 20;
    struct Slice {
        std::unique_ptr<char[]> data;
        size_t size;
        size_t used;
    };
    std::vector<Slice> slices;
    std::vector<const char *> slice_ptrs;
    std::vector<size_t> slice_bytes;
    size_t current = 0;
    size_t total = 0;
    size_t n = 0;
    int lengths[max_length];
    const char * ptrs[max_length];
    cetype_ext_t encodings[max_length];
};

}  // namespace qd_charpack

#endif
