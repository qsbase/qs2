#ifndef _QS2_QD_RAWVEC_H
#define _QS2_QD_RAWVEC_H

#include <charport.h>
#include <Rinternals.h>
#include <R_ext/Altrep.h>
#include <R_ext/Rdynload.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

// Prototype: ALTREP strings backed by an R raw vector. A charvec owns native
// memory through an external pointer and a finalizer, which costs two extra R
// objects and a weak reference per vector; for many short vectors that
// dominates. Here the bytes live in a RAWSXP that R owns, so there is no
// finalizer and R accounts for the memory.
//
// data1 layout: [uint64 n][uint32 size x n][uint8 encoding x n][bytes], the
// order of charvec's serialized state without its prefix. Encodings are
// charport cetype_ext_t values; NA is CETYPE_EXT_NA with size 0. Sizes come
// first so they are 4-byte aligned.
//
// The deserializer creates the vector with a shared 8-byte data1 holding only n
// (all NA), and swaps in the full raw vector once the strings are read. The
// first element access materializes the whole vector into data2 and drops
// data1. There is no Serialized_state method, so serialize() writes an ordinary
// character vector and reading it back does not need qs2.

namespace qd_rawvec {

static R_altrep_class_t class_t;

// Vectors shorter than this use the raw form when use_alt_rep is TRUE; 0 disables it.
static uint64_t threshold = 0;

static constexpr size_t header_bytes = sizeof(uint64_t);

inline uint64_t stored_length(SEXP data1) {
    uint64_t n;
    std::memcpy(&n, RAW(data1), sizeof(n));
    return n;
}

// Unfilled vectors share one preserved length-only placeholder per length, so
// creating a vector allocates only the ALTREP node. Placeholders are never
// modified; Filler::finish replaces data1.
inline SEXP placeholder(uint64_t length) {
    static std::vector<SEXP> cache;
    const bool cached = length < 4096;
    if(cached) {
        if(length < cache.size() && cache[length] != nullptr) return cache[length];
        if(cache.size() <= length) cache.resize(length + 1, nullptr);  // before any R allocation
    }
    SEXP data1 = PROTECT(Rf_allocVector(RAWSXP, header_bytes));
    std::memcpy(RAW(data1), &length, sizeof(length));
    if(cached) {
        R_PreserveObject(data1);
        cache[length] = data1;
    }
    UNPROTECT(1);
    return data1;
}

// Returns an unprotected vector of NA strings.
inline SEXP create(uint64_t length) {
    SEXP data1 = PROTECT(placeholder(length));
    SEXP out = R_new_altrep(class_t, data1, R_NilValue);
    UNPROTECT(1);
    return out;
}

inline bool is_rawvec(SEXP x) {
    return ALTREP(x) && R_altrep_inherits(x, class_t);
}

// String bytes for one vector, in slices that are kept across vectors so a
// read allocates them once; growing never copies. finish() copies the used
// part of each slice, in order, into the raw vector.
class SlicePool {
public:
    void reset() {
        for(size_t i = 0; i <= current && i < slices.size(); ++i) slices[i].used = 0;
        current = 0;
        total = 0;
    }

    char * reserve(size_t len) {
        while(current < slices.size() && slices[current].size - slices[current].used < len) ++current;
        if(current == slices.size()) {
            const size_t grow = slices.empty() ? first_slice : std::min<size_t>(slices.back().size * 2, max_slice);
            const size_t size = std::max(grow, len);
            slices.push_back(Slice{std::unique_ptr<char[]>(new char[size]), size, 0});
        }
        Slice & s = slices[current];
        char * out = s.data.get() + s.used;
        s.used += len;
        total += len;
        return out;
    }

    size_t bytes() const { return total; }

    void copy_to(char * dest) const {
        for(size_t i = 0; i <= current && i < slices.size(); ++i) {
            std::memcpy(dest, slices[i].data.get(), slices[i].used);
            dest += slices[i].used;
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
    size_t current = 0;
    size_t total = 0;
};

// Collects one vector's strings in buffers owned by the deserializer, then
// copies them into a raw vector, so an R jump while allocating leaks nothing.
// The vector length is known up front, so sizes and encodings never grow.
class Filler {
public:
    void start(uint64_t length) {
        sizes.assign(static_cast<size_t>(length), 0);
        encodings.assign(static_cast<size_t>(length), CETYPE_EXT_NA.value);
        bytes.reset();
    }
    // NA is the initial state
    void set_empty(uint64_t i) {
        encodings[i] = CETYPE_EXT_ASCII_OR_UTF8.value;
    }
    // Space for string i of len > 0 bytes, to be filled by the caller.
    char * set(uint64_t i, uint32_t len) {
        sizes[i] = len;
        encodings[i] = CETYPE_EXT_ASCII_OR_UTF8.value;
        return bytes.reserve(len);
    }
    void finish(SEXP x) {
        const size_t n = sizes.size();
        SEXP data1 = PROTECT(Rf_allocVector(RAWSXP, static_cast<R_xlen_t>(header_bytes + 5 * n + bytes.bytes())));
        unsigned char * p = RAW(data1);
        const uint64_t n64 = n;
        std::memcpy(p, &n64, sizeof(n64));
        if(n > 0) {
            std::memcpy(p + header_bytes, sizes.data(), 4 * n);
            std::memcpy(p + header_bytes + 4 * n, encodings.data(), n);
        }
        bytes.copy_to(reinterpret_cast<char *>(p + header_bytes + 5 * n));
        R_set_altrep_data1(x, data1);
        UNPROTECT(1);
    }

private:
    std::vector<uint32_t> sizes;
    std::vector<uint8_t> encodings;
    SlicePool bytes;
};

inline cetype_t to_cetype(uint8_t enc) {
    switch(enc) {
        case CETYPE_EXT_LATIN1.value: return CE_LATIN1;
        case CETYPE_EXT_BYTES.value: return CE_BYTES;
        case CETYPE_EXT_NATIVE.value:
        case CETYPE_EXT_ASCII.value: return CE_NATIVE;
        default: return CE_UTF8;  // UTF8, ASCII_OR_UTF8
    }
}

struct Methods {
    static R_xlen_t Length(SEXP x) {
        SEXP data2 = R_altrep_data2(x);
        if(data2 != R_NilValue) return XLENGTH(data2);
        return static_cast<R_xlen_t>(stored_length(R_altrep_data1(x)));
    }

    static SEXP Materialize(SEXP x) {
        SEXP data2 = R_altrep_data2(x);
        if(data2 != R_NilValue) return data2;
        SEXP data1 = R_altrep_data1(x);
        const R_xlen_t n = static_cast<R_xlen_t>(stored_length(data1));
        data2 = PROTECT(Rf_allocVector(STRSXP, n));  // NA until filled below
        if(XLENGTH(data1) > static_cast<R_xlen_t>(header_bytes)) {  // filled; a placeholder is all NA
            const unsigned char * sizes = RAW(data1) + header_bytes;
            const unsigned char * encodings = sizes + 4 * n;
            const char * s = reinterpret_cast<const char *>(encodings + n);
            for(R_xlen_t i = 0; i < n; ++i) {
                const uint8_t enc = encodings[i];
                if(enc == CETYPE_EXT_NA.value) {
                    SET_STRING_ELT(data2, i, NA_STRING);
                    continue;
                }
                uint32_t size;
                std::memcpy(&size, sizes + 4 * i, sizeof(size));
                SET_STRING_ELT(data2, i, size == 0 ? R_BlankString
                                                   : Rf_mkCharLenCE(s, static_cast<int>(size), to_cetype(enc)));
                s += size;
            }
        } else {
            for(R_xlen_t i = 0; i < n; ++i) SET_STRING_ELT(data2, i, NA_STRING);
        }
        R_set_altrep_data2(x, data2);
        R_set_altrep_data1(x, R_NilValue);  // the strings now live in data2
        UNPROTECT(1);
        return data2;
    }

    static SEXP Elt(SEXP x, R_xlen_t i) {
        return STRING_ELT(Materialize(x), i);
    }

    static void Set_elt(SEXP x, R_xlen_t i, SEXP v) {
        SET_STRING_ELT(Materialize(x), i, v);
    }

    static void * Dataptr(SEXP x, Rboolean /* writeable */) {
        return const_cast<void *>(static_cast<const void *>(STRING_PTR_RO(Materialize(x))));
    }

    static const void * Dataptr_or_null(SEXP x) {
        SEXP data2 = R_altrep_data2(x);
        return data2 == R_NilValue ? nullptr : static_cast<const void *>(STRING_PTR_RO(data2));
    }

    // data1 never changes once filled, so an unmaterialized copy shares it.
    static SEXP Duplicate(SEXP x, Rboolean /* deep */) {
        SEXP data2 = R_altrep_data2(x);
        if(data2 != R_NilValue) return Rf_duplicate(data2);
        return R_new_altrep(class_t, R_altrep_data1(x), R_NilValue);
    }

    static Rboolean Inspect(SEXP x, int, int, int, void (*)(SEXP, int, int, int)) {
        Rprintf(" qs2_rawvec (len=%lld, %s)\n", static_cast<long long>(Length(x)),
                R_altrep_data2(x) == R_NilValue ? "not materialized" : "materialized");
        return TRUE;
    }
};

inline void register_class(DllInfo * dll) {
    class_t = R_make_altstring_class("qs2_rawvec", "qs2", dll);
    R_set_altrep_Length_method(class_t, Methods::Length);
    R_set_altrep_Inspect_method(class_t, Methods::Inspect);
    R_set_altrep_Duplicate_method(class_t, Methods::Duplicate);
    R_set_altvec_Dataptr_method(class_t, Methods::Dataptr);
    R_set_altvec_Dataptr_or_null_method(class_t, Methods::Dataptr_or_null);
    R_set_altstring_Elt_method(class_t, Methods::Elt);
    R_set_altstring_Set_elt_method(class_t, Methods::Set_elt);
}

}  // namespace qd_rawvec

#endif
