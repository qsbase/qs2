#ifndef _QS2_QD_SERIALIZER_H_
#define _QS2_QD_SERIALIZER_H_


#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include <Rcpp.h>
#include <R_ext/Utils.h>
#include <Rversion.h>
#include <charport.h>

#include "qoptions.h"
#include "qx_file_headers.h"

using namespace Rcpp;

// Deliberately once per type per session, not once per call: an unsupported
// type usually appears many times within a single object, and warning on each
// would bury the user. The flags are intentionally never reset -- a second
// qd_save() of a similar object stays quiet.
//
// REprintf rather than Rf_warning: this runs underneath the serializer, where
// an R jump (options(warn = 2), or a calling handler) would abandon a
// half-written stream.
inline void qd_warn_unsupported_type_once(const char* const label, const SEXPTYPE type) {
    // TYPEOF is a 5-bit field, so every real SEXPTYPE indexes the table; the
    // guard is belt-and-braces for a corrupt type and must not itself index
    // out of bounds (Rf_type2char does no bounds checking of its own).
    static constexpr unsigned int QD_MAX_SEXPTYPE = 32;
    static bool warned_types[QD_MAX_SEXPTYPE] = {};
    const unsigned int type_index = static_cast<unsigned int>(type);
    if(type_index >= QD_MAX_SEXPTYPE) {
        REprintf("%s of an unrecognized type (%u) are not supported in qdata format\n", label, type_index);
        return;
    }
    if(warned_types[type_index]) return;
    warned_types[type_index] = true;
    REprintf("%s of type %s are not supported in qdata format; further warnings for this type are suppressed for the session\n",
             label, Rf_type2char(type));
}

inline bool qd_is_ascii(const char* ptr, std::size_t len) {
  const auto* bytes = reinterpret_cast<const unsigned char*>(ptr);
  for (std::size_t i = 0; i < len; ++i) {
    if ((bytes[i] & 0x80U) != 0) {
      return false;
    }
  }
  return true;
}


template<typename block_compress_writer>
struct QdataSerializer {
    block_compress_writer & writer;
    const bool warn;
    std::vector<std::pair<SEXP, uint64_t>> character_sexp;
    std::vector<std::pair<SEXP, uint64_t>> complex_sexp;
    std::vector<std::pair<SEXP, uint64_t>> real_sexp;
    std::vector<std::pair<SEXP, uint64_t>> integer_sexp; // and logical
    std::vector<std::pair<SEXP, uint64_t>> raw_sexp;
    // Keep the active charport borrow in the serializer's owning frame so an
    // R jump converted by the outer unwind boundary cannot strand its state.
    charport::Reader string_reader;

    // shared scratch, so no recursion frame owns heap across a fallible R call.
    // Each frame holds the slice [base, base + count) and pops it in write_attributes().
    // Depth is attribute nesting depth, not object nesting depth.
    std::vector< std::pair<SEXP, SEXP> > attr_stack;

    QdataSerializer(block_compress_writer & writer, const bool warn) :
    writer(writer), warn(warn) {}

    static bool attr_is_supported(SEXP const attr_value) {
        switch(TYPEOF(attr_value)) {
            case LGLSXP:
            case INTSXP:
            case REALSXP:
            case CPLXSXP:
            case STRSXP:
            case VECSXP:
            case RAWSXP:
                return true;
            default:
                return false;
        }
    }

    struct attr_ctx {
        QdataSerializer* self;
        uint32_t count;
    };

#if R_VERSION >= R_Version(4, 6, 0)
    static SEXP count_attrib(SEXP, SEXP attr_value, void* data) {
        attr_ctx* c = static_cast<attr_ctx*>(data);
        if(attr_is_supported(attr_value)) {
            c->count++;
        } else if(c->self->warn) {
            qd_warn_unsupported_type_once("Attributes", TYPEOF(attr_value));
        }
        return NULL;
    }
    static SEXP fill_attrib(SEXP tag, SEXP attr_value, void* data) {
        attr_ctx* c = static_cast<attr_ctx*>(data);
        if(attr_is_supported(attr_value)) {
            c->self->attr_stack.push_back(std::make_pair(PRINTNAME(tag), attr_value)); // reserved, cannot throw
        }
        return NULL;
    }
#endif

    // appends to attr_stack; counted first so the append cannot allocate mid-walk
    uint32_t collect_attributes(SEXP const object) {
        attr_ctx ctx{this, 0};
#if R_VERSION >= R_Version(4, 6, 0)
        R_mapAttrib(object, &count_attrib, &ctx);
        if(ctx.count > 0) {
            attr_stack.reserve(attr_stack.size() + ctx.count);
            R_mapAttrib(object, &fill_attrib, &ctx);
        }
#else
        for(SEXP alist = ATTRIB(object); alist != R_NilValue; alist = CDR(alist)) {
            if(attr_is_supported(CAR(alist))) {
                ctx.count++;
            } else if(warn) {
                qd_warn_unsupported_type_once("Attributes", TYPEOF(CAR(alist)));
            }
        }
        if(ctx.count > 0) {
            attr_stack.reserve(attr_stack.size() + ctx.count);
            for(SEXP alist = ATTRIB(object); alist != R_NilValue; alist = CDR(alist)) {
                if(attr_is_supported(CAR(alist))) {
                    attr_stack.push_back(std::make_pair(PRINTNAME(TAG(alist)), CAR(alist)));
                }
            }
        }
#endif
        return ctx.count;
    }

    void write_attributes(const size_t base, const uint32_t count) {
        for(size_t i = base; i < base + count; ++i) {
            SEXP name = attr_stack[i].first;   // copied out: the recursion may reallocate
            SEXP value = attr_stack[i].second;
            uint32_t alen = LENGTH(name);
            write_string_header(alen);
            writer.push_data(CHAR(name), alen);
            write_object(value);
        }
        attr_stack.resize(base);
    }

    void write_attr_header(uint32_t length) {
        if(length < MAX_5_BIT_LENGTH) {
            writer.push_pod( static_cast<uint8_t>(attribute_header_5 | static_cast<uint8_t>(length)) );
        } else if(length < MAX_8_BIT_LENGTH) {
            writer.push_pod(attribute_header_8);
            writer.push_pod_contiguous(static_cast<uint8_t>(length) );
        } else {
            writer.push_pod(attribute_header_32);
            writer.push_pod_contiguous( length );
        }
    }

    // LOGICAL header
    void write_header_lglsxp(uint64_t length, uint32_t attr_length) {
        bool has_attrs = attr_length > 0;
        if(has_attrs) write_attr_header(attr_length);
        if(length < MAX_5_BIT_LENGTH) {
            writer.push_pod( static_cast<uint8_t>(logical_header_5 | static_cast<uint8_t>(length)), has_attrs );
        } else if(length < MAX_8_BIT_LENGTH) {
            writer.push_pod(logical_header_8, has_attrs);
            writer.push_pod_contiguous(static_cast<uint8_t>(length) );
        } else if(length < MAX_16_BIT_LENGTH) {
            writer.push_pod(logical_header_16, has_attrs);
            writer.push_pod_contiguous(static_cast<uint16_t>(length) );
        } else if(length < MAX_32_BIT_LENGTH) {
            writer.push_pod(logical_header_32, has_attrs);
            writer.push_pod_contiguous(static_cast<uint32_t>(length) );
        } else {
            writer.push_pod(logical_header_64, has_attrs);
            writer.push_pod_contiguous(length);
        }
    }
    // REALSXP header
    void write_header_realsxp(uint64_t length, uint64_t attr_length) {
        bool has_attrs = attr_length > 0;
        if(has_attrs) write_attr_header(attr_length);
        if(length < MAX_5_BIT_LENGTH) {
            writer.push_pod( static_cast<uint8_t>(numeric_header_5 | static_cast<uint8_t>(length)), has_attrs );
        } else if(length < MAX_8_BIT_LENGTH) {
            writer.push_pod(numeric_header_8, has_attrs);
            writer.push_pod_contiguous( static_cast<uint8_t>(length) );
        } else if(length < MAX_16_BIT_LENGTH) {
            writer.push_pod(numeric_header_16, has_attrs);
            writer.push_pod_contiguous( static_cast<uint16_t>(length) );
        } else if(length < MAX_32_BIT_LENGTH) {
            writer.push_pod(numeric_header_32, has_attrs);
            writer.push_pod_contiguous( static_cast<uint32_t>(length) );
        } else {
            writer.push_pod(numeric_header_64, has_attrs);
            writer.push_pod_contiguous(length);
        }
        return;
    }
    // INTSXP header
    void write_header_intsxp(uint64_t length, uint64_t attr_length) {
        bool has_attrs = attr_length > 0;
        if(has_attrs) write_attr_header(attr_length);
        if(length < MAX_5_BIT_LENGTH) {
            writer.push_pod( static_cast<uint8_t>(integer_header_5 | static_cast<uint8_t>(length)), has_attrs );
        } else if(length < MAX_8_BIT_LENGTH) {
            writer.push_pod(integer_header_8, has_attrs);
            writer.push_pod_contiguous(static_cast<uint8_t>(length) );
        } else if(length < MAX_16_BIT_LENGTH) {
            writer.push_pod(integer_header_16, has_attrs);
            writer.push_pod_contiguous(static_cast<uint16_t>(length) );
        } else if(length < MAX_32_BIT_LENGTH) {
            writer.push_pod(integer_header_32, has_attrs);
            writer.push_pod_contiguous(static_cast<uint32_t>(length) );
        } else {
            writer.push_pod(integer_header_64, has_attrs);
            writer.push_pod_contiguous(static_cast<uint64_t>(length) );
        }
    }
    // CPLXSXP
    void write_header_cplxsxp(uint64_t length, uint64_t attr_length) {
        bool has_attrs = attr_length > 0;
        if(has_attrs) write_attr_header(attr_length);
        if(length < MAX_32_BIT_LENGTH) {
            writer.push_pod(complex_header_32, has_attrs);
            writer.push_pod_contiguous(static_cast<uint32_t>(length) );
        } else {
            writer.push_pod(complex_header_64, has_attrs);
            writer.push_pod_contiguous(length);
        }
    }
    // STRSXP
    void write_header_strsxp(uint64_t length, uint64_t attr_length) {
        bool has_attrs = attr_length > 0;
        if(has_attrs) write_attr_header(attr_length);
        if(length < MAX_5_BIT_LENGTH) {
            writer.push_pod( static_cast<uint8_t>(character_header_5 | static_cast<uint8_t>(length)), has_attrs );
        } else if(length < MAX_8_BIT_LENGTH) {
            writer.push_pod(character_header_8, has_attrs);
            writer.push_pod_contiguous(static_cast<uint8_t>(length) );
        } else if(length < MAX_16_BIT_LENGTH) {
            writer.push_pod(character_header_16, has_attrs);
            writer.push_pod_contiguous(static_cast<uint16_t>(length) );
        } else if(length < MAX_32_BIT_LENGTH) {
            writer.push_pod(character_header_32, has_attrs);
            writer.push_pod_contiguous(static_cast<uint32_t>(length) );
        } else {
            writer.push_pod(character_header_64, has_attrs);
            writer.push_pod_contiguous(static_cast<uint64_t>(length) );
        }
    }
    // VECSXP header
    void write_header_vecsxp(uint64_t length, uint64_t attr_length) {
        bool has_attrs = attr_length > 0;
        if(has_attrs) write_attr_header(attr_length);
        if(length < MAX_5_BIT_LENGTH) {
            writer.push_pod( static_cast<uint8_t>(list_header_5 | static_cast<uint8_t>(length)), has_attrs );
        } else if(length < MAX_8_BIT_LENGTH) {
            writer.push_pod(list_header_8, has_attrs);
            writer.push_pod_contiguous(static_cast<uint8_t>(length) );
        } else if(length < MAX_16_BIT_LENGTH) {
            writer.push_pod(list_header_16, has_attrs);
            writer.push_pod_contiguous(static_cast<uint16_t>(length) );
        } else if(length < MAX_32_BIT_LENGTH) {
            writer.push_pod(list_header_32, has_attrs);
            writer.push_pod_contiguous(static_cast<uint32_t>(length) );
        } else {
            writer.push_pod(list_header_64, has_attrs);
            writer.push_pod_contiguous(length);
        }
    }
    // RAWSXP header
    void write_header_rawsxp(uint64_t length, uint64_t attr_length) {
        bool has_attrs = attr_length > 0;
        if(has_attrs) write_attr_header(attr_length);
        if(length < MAX_32_BIT_LENGTH) {
            writer.push_pod(raw_header_32, has_attrs);
            writer.push_pod_contiguous(static_cast<uint32_t>(length) );
        } else {
            writer.push_pod(raw_header_64, has_attrs);
            writer.push_pod_contiguous(length);
        }
    }
    // NILSXP header
    void write_header_nilsxp() {
        writer.push_pod(nil_header);
        return;
    }

    // individual CHARSXP elements
    void write_string_header(const uint32_t length) {
        if(length < MAX_STRING_8_BIT_LENGTH) {
            writer.push_pod( static_cast<uint8_t>( length ));
        } else if(length < MAX_STRING_16_BIT_LENGTH) {
            writer.push_pod( string_header_16 );
            writer.push_pod_contiguous(static_cast<uint16_t>(length) );
        } else {
            writer.push_pod( string_header_32 );
            writer.push_pod_contiguous(length);
        }
    }

    // push_data may retain a whole MAX_BLOCKSIZE-sized input pointer for an
    // asynchronous compressor. Keep charport views scoped to the current batch
    // by ensuring every submitted piece takes the writer's copying path.
    void write_string_data(const char* data, uint32_t length) {
        constexpr uint32_t piece_size = MAX_BLOCKSIZE - 1;
        uint32_t offset = 0;
        while(offset < length) {
            const uint32_t piece = std::min(piece_size, length - offset);
            writer.push_data(data + offset, piece);
            offset += piece;
        }
    }

    void write_object(SEXP const object) {
        R_CheckStack();
        SEXPTYPE object_type = TYPEOF(object);
        switch(object_type) {
            case LGLSXP:
            {
                uint64_t object_length = Rf_xlength(object);
                const size_t base = attr_stack.size();
                const uint32_t attr_count = collect_attributes(object);
                write_header_lglsxp(object_length, attr_count);
                write_attributes(base, attr_count);
                if(object_length > 0) integer_sexp.push_back(std::make_pair(object, object_length));
                // writer.push_data(reinterpret_cast<char*>(LOGICAL(object)), object_length * 4);
                return;
            }
            case INTSXP:
            {
                uint64_t object_length = Rf_xlength(object);
                const size_t base = attr_stack.size();
                const uint32_t attr_count = collect_attributes(object);
                write_header_intsxp(object_length, attr_count);
                write_attributes(base, attr_count);
                if(object_length > 0) integer_sexp.push_back(std::make_pair(object, object_length));
                // writer.push_data(reinterpret_cast<char*>(INTEGER(object)), object_length * 4);
                return;
            }
            case REALSXP:
            {
                uint64_t object_length = Rf_xlength(object);
                const size_t base = attr_stack.size();
                const uint32_t attr_count = collect_attributes(object);
                write_header_realsxp(object_length, attr_count);
                write_attributes(base, attr_count);
                if(object_length > 0) real_sexp.push_back(std::make_pair(object, object_length));
                // writer.push_data(reinterpret_cast<char*>(REAL(object)), object_length * 8);
                return;
            }
            case CPLXSXP:
            {
                uint64_t object_length = Rf_xlength(object);
                const size_t base = attr_stack.size();
                const uint32_t attr_count = collect_attributes(object);
                write_header_cplxsxp(object_length, attr_count);
                write_attributes(base, attr_count);
                if(object_length > 0) complex_sexp.push_back(std::make_pair(object, object_length));
                // writer.push_data(reinterpret_cast<char*>(COMPLEX(object)), object_length * 16);
                return;
            }
            case STRSXP:
            {
                uint64_t object_length = Rf_xlength(object);
                const size_t base = attr_stack.size();
                const uint32_t attr_count = collect_attributes(object);
                write_header_strsxp(object_length, attr_count);
                write_attributes(base, attr_count);
                if(object_length > 0) character_sexp.push_back(std::make_pair(object, object_length));
                return;
            }
            case VECSXP:
            {
                uint64_t object_length = Rf_xlength(object);
                const size_t base = attr_stack.size();
                const uint32_t attr_count = collect_attributes(object);
                write_header_vecsxp(object_length, attr_count);
                write_attributes(base, attr_count);
                const SEXP * xptr = reinterpret_cast<const SEXP*>(DATAPTR_RO(object));
                for(uint64_t i=0; i<object_length; ++i) {
                    write_object(xptr[i]);
                }
                return;
            }
            case RAWSXP:
            {
                uint64_t object_length = Rf_xlength(object);
                const size_t base = attr_stack.size();
                const uint32_t attr_count = collect_attributes(object);
                write_header_rawsxp(object_length, attr_count);
                write_attributes(base, attr_count);
                if(object_length > 0) raw_sexp.push_back(std::make_pair(object, object_length));
                // writer.push_data(reinterpret_cast<char*>(RAW(object)), object_length);
                return;
            }
            case NILSXP:
                write_header_nilsxp();
                return;
            default:
                if(warn) qd_warn_unsupported_type_once("Objects", TYPEOF(object));
                write_header_nilsxp();
                return;
        }
    }
    void write_object_data() {
        constexpr R_xlen_t batch_capacity = 256;
        for(size_t vector_index = 0; vector_index < character_sexp.size();) {
            const auto& x = character_sexp[vector_index++];
            SEXP object = x.first;
            uint64_t object_length = x.second;
            bool grouped = false;
            // Payloads have no vector delimiters. Batch consecutive ordinary
            // scalars through one Reader, avoiding a resolver call per list item.
            // Registered ALTREP inputs keep their own non-materializing reader.
            if(object_length == 1 && !ALTREP(object)) {
                size_t end = vector_index;
                while(end < character_sexp.size() && end - vector_index + 1 < size_t(batch_capacity) &&
                      character_sexp[end].second == 1 && !ALTREP(character_sexp[end].first)) ++end;
                const size_t count = end - vector_index + 1;
                if(count > 1) {
                    SEXP batch = PROTECT(Rf_allocVector(STRSXP, static_cast<R_xlen_t>(count)));
                    SET_STRING_ELT(batch, 0, STRING_ELT(object, 0));
                    for(size_t j = 1; j < count; ++j)
                        SET_STRING_ELT(batch, static_cast<R_xlen_t>(j), STRING_ELT(character_sexp[vector_index + j - 1].first, 0));
                    object = batch;
                    object_length = count;
                    vector_index = end;
                    grouped = true;
                }
            }
            string_reader.reset(object);
            const char* ptrs[batch_capacity];
            int lengths[batch_capacity];
            cetype_ext_t encodings[batch_capacity];
            for(uint64_t start = 0; start < object_length;) {
                const R_xlen_t count = static_cast<R_xlen_t>(
                    std::min<uint64_t>(batch_capacity, object_length - start));
                string_reader.views(static_cast<R_xlen_t>(start), count, ptrs, lengths, encodings);
                for(R_xlen_t j = 0; j < count; ++j) {
                    const char* data = ptrs[j];
                    const int length = lengths[j];
                    const cetype_ext_t encoding = encodings[j];
                    if(encoding == CETYPE_EXT_NA || data == nullptr || length == NA_INTEGER) {
                        writer.push_pod(string_header_NA);
                        continue;
                    }
                    if(length < 0) {
                        throw std::runtime_error("invalid negative string length from charport Reader");
                    }

                    // Preserve qdata's existing byte policy: Latin-1 is always
                    // normalized; native text is normalized only outside a
                    // UTF-8 locale and only when it contains non-ASCII bytes.
                    const bool needs_translation = encoding == CETYPE_EXT_LATIN1 ||
                        (encoding == CETYPE_EXT_NATIVE && !qs2_get_utf8_locale() &&
                         !qd_is_ascii(data, static_cast<std::size_t>(length)));
                    if(needs_translation) {
                        const cetype_t base_encoding = encoding == CETYPE_EXT_LATIN1
                            ? CE_LATIN1 : CE_NATIVE;
                        SEXP charsxp = PROTECT(Rf_mkCharLenCE(data, length, base_encoding));
                        try {
                            const char* utf8 = Rf_translateCharUTF8(charsxp);
                            const uint32_t utf8_length = static_cast<uint32_t>(std::strlen(utf8));
                            write_string_header(utf8_length);
                            write_string_data(utf8, utf8_length);
                        } catch(...) {
                            UNPROTECT(1);
                            throw;
                        }
                        UNPROTECT(1);
                    } else {
                        const uint32_t stored_length = static_cast<uint32_t>(length);
                        write_string_header(stored_length);
                        write_string_data(data, stored_length);
                    }
                }
                start += static_cast<uint64_t>(count);
            }
            string_reader = charport::Reader();
            if(grouped) UNPROTECT(1);
        }
        for(auto & x : complex_sexp) {
            SEXP object = x.first;
            uint64_t object_length = x.second;
            writer.push_data(reinterpret_cast<char*>(COMPLEX(object)), object_length * 16);
        }
        for(auto & x : real_sexp) {
            SEXP object = x.first;
            uint64_t object_length = x.second;
            writer.push_data(reinterpret_cast<char*>(REAL(object)), object_length * 8);
        }
        for(auto & x : integer_sexp) {
            SEXP object = x.first;
            uint64_t object_length = x.second;
            writer.push_data(reinterpret_cast<char*>(INTEGER(object)), object_length * 4);
        }
        for(auto & x : raw_sexp) {
            SEXP object = x.first;
            uint64_t object_length = x.second;
            writer.push_data(reinterpret_cast<char*>(RAW(object)), object_length);
        }
    }
};

#endif
