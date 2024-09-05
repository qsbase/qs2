#ifndef _QS2_QD_SERIALIZER_H_
#define _QS2_QD_SERIALIZER_H_


#include <Rcpp.h>

#include "qx_file_headers.h"
#include "qd_constants.h"

#include "sf_external.h"

using namespace Rcpp;
#define FILE_SAVE_ERR_MSG "Failed to open for writing. Does the directory exist? Do you have file permissions? Is the file name long? (>255 chars)"

template<typename block_compress_writer>
struct QdataSerializer {
    block_compress_writer & writer;
    // std::vector<uint8_t> shuffleblock;
    const bool warn;
    // const bool shuffle;
    QdataSerializer(block_compress_writer & writer, const bool warn) : 
    writer(writer), warn(warn) {}

    // void shuffle_push(const void * const data, const uint64_t len, const uint64_t bytesoftype) {
    //     if( shuffle && (len > MIN_SHUFFLE_ARRAYSIZE)) {
    //         if(len > shuffleblock.size()) shuffleblock.resize(len);
    //         blosc_shuffle(reinterpret_cast<const uint8_t * const>(data), shuffleblock.data(), len, bytesoftype);
    //         writer.push_data(reinterpret_cast<const char * const>(shuffleblock.data()), len);
    //     } else if(len > 0) {
    //         writer.push_data(reinterpret_cast<const char * const>(data), len);
    //     }
    // }

    std::vector< std::pair<SEXP, SEXP> > get_attributes(SEXP const object) {
        std::vector< std::pair<SEXP, SEXP> > attrs;
        SEXP alist = ATTRIB(object);
        while(alist != R_NilValue) {
            SEXP attr_value = CAR(alist);
            switch(TYPEOF(attr_value)) {
                // valid attribute types
                case LGLSXP:
                case INTSXP:
                case REALSXP:
                case CPLXSXP:
                case STRSXP:
                case VECSXP:
                case RAWSXP:
                    attrs.push_back(std::make_pair(PRINTNAME(TAG(alist)), attr_value));
                    break;
                default:
                    if(warn) Rf_warning("Attributes of type %s are not supported in qdata format", Rf_type2char(TYPEOF(attr_value)));
                    break;
            }
            alist = CDR(alist);
        }
        return attrs;
    }

    bool is_unmaterialized_sf_vector(SEXP const object) {
        if (! ALTREP(object)) return false; // check if ALTREP
        if(DATAPTR_OR_NULL(object) != nullptr) return false; // check if unmaterialized (returning nullptr)
        SEXP info = ATTRIB(ALTREP_CLASS(object));
        const char * classname = CHAR(PRINTNAME(CAR(info)));
        if(std::strcmp(classname, "__sf_vec__") != 0) return false; // check if classname is __sf_vec__
        return true;
    }

    void write_attributes(std::vector< std::pair<SEXP, SEXP> > const & attrs) {
        for(uint64_t i = 0; i < attrs.size(); i++) {
            uint32_t alen = LENGTH(attrs[i].first);
            write_string_header(alen);
            writer.push_data(CHAR(attrs[i].first), alen);
            write_object(attrs[i].second);
        }
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
        if(attr_length > 0) write_attr_header(attr_length);
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

    void write_object(SEXP const object) {
        SEXPTYPE object_type = TYPEOF(object);
        switch(object_type) {
            case LGLSXP:
            {
                uint64_t object_length = Rf_xlength(object);
                std::vector< std::pair<SEXP, SEXP> > attrs = get_attributes(object);
                write_header_lglsxp(object_length, attrs.size());
                write_attributes(attrs);
                writer.push_data(reinterpret_cast<char*>(LOGICAL(object)), object_length * 4);
                return;
            }
            case INTSXP:
            {
                uint64_t object_length = Rf_xlength(object);
                std::vector< std::pair<SEXP, SEXP> > attrs = get_attributes(object);
                write_header_intsxp(object_length, attrs.size());
                write_attributes(attrs);
                writer.push_data(reinterpret_cast<char*>(INTEGER(object)), object_length * 4);
                return;
            }
            case REALSXP:
            {
                uint64_t object_length = Rf_xlength(object);
                std::vector< std::pair<SEXP, SEXP> > attrs = get_attributes(object);
                write_header_realsxp(object_length, attrs.size());
                write_attributes(attrs);
                writer.push_data(reinterpret_cast<char*>(REAL(object)), object_length * 8);
                return;
            }
            case CPLXSXP:
            {
                uint64_t object_length = Rf_xlength(object);
                std::vector< std::pair<SEXP, SEXP> > attrs = get_attributes(object);
                write_header_cplxsxp(object_length, attrs.size());
                write_attributes(attrs);
                writer.push_data(reinterpret_cast<char*>(COMPLEX(object)), object_length * 16);
                return;
            }
            case STRSXP:
            {
                uint64_t object_length = Rf_xlength(object);
                std::vector< std::pair<SEXP, SEXP> > attrs = get_attributes(object);
                write_header_strsxp(object_length, attrs.size());
                write_attributes(attrs);
                
                // check for special case, stringfish ALTREP vector
                if (is_unmaterialized_sf_vector(object)) {
                    auto & ref = sf_vec_data_ref(object); // sf_vec_data_ref from stringfish
                    for(uint64_t i=0; i<object_length; ++i) {
                        cetype_t_ext enc = ref[i].encoding;
                        if(enc == cetype_t_ext::CE_NA) { // cetype_t_ext is from stringfish
                            writer.push_pod(string_header_NA);
                        } else if ( (enc == cetype_t_ext::CE_LATIN1) || ((enc == cetype_t_ext::CE_NATIVE) && (IS_UTF8_LOCALE == 1)) ) {
                            // check if latin1 or (native AND IS_UTF8_LOCALE == 0), needs translation
                            SEXP xi = STRING_ELT(object, i); // materialize
                            const char * ci = Rf_translateCharUTF8(xi);
                            uint32_t li = strlen(ci);
                            write_string_header(li);
                            writer.push_data(ci, li);
                        } else {
                            write_string_header(ref[i].sdata.size());
                            writer.push_data(ref[i].sdata.c_str(), ref[i].sdata.size());
                        }
                    }
                    return;
                } else {
                    const SEXP * xptr = STRING_PTR_RO(object);
                    for(uint64_t i=0; i<object_length; ++i) {
                        SEXP xi = xptr[i]; // STRING_ELT(x, i);
                        if(xi == NA_STRING) {
                            writer.push_pod(string_header_NA);
                        } else {
                            cetype_t enc = Rf_getCharCE(xi);
                            uint32_t li = LENGTH(xi);
                            const char * ci = CHAR(xi);
                            // check if needs translation
                            bool needs_translation = (enc == cetype_t::CE_LATIN1) ||
                                                     ((enc == cetype_t::CE_NATIVE) && (IS_UTF8_LOCALE == 0) && !checkAscii(ci, li));
                            if(needs_translation) {
                                ci = Rf_translateCharUTF8(xi);
                                li = strlen(ci);
                            }
                            write_string_header(li);
                            writer.push_data(ci, li);
                        }
                    }
                    return;
                }
            }
            case VECSXP:
            {
                uint64_t object_length = Rf_xlength(object);
                std::vector< std::pair<SEXP, SEXP> > attrs = get_attributes(object);
                write_header_vecsxp(object_length, attrs.size());
                write_attributes(attrs);
                const SEXP * xptr = reinterpret_cast<const SEXP*>(DATAPTR_RO(object));
                for(uint64_t i=0; i<object_length; ++i) {
                    write_object(xptr[i]);
                }
                return;
            }
            case RAWSXP:
            {
                uint64_t object_length = Rf_xlength(object);
                std::vector< std::pair<SEXP, SEXP> > attrs = get_attributes(object);
                write_header_rawsxp(object_length, attrs.size());
                write_attributes(attrs);
                writer.push_data(reinterpret_cast<char*>(RAW(object)), object_length);
                return;
            }
            case NILSXP:
                write_header_nilsxp();
                return;
            default:
                if(warn) Rf_warning("Objects of type %s are not supported in qdata format", Rf_type2char(TYPEOF(object)));
                write_header_nilsxp();
                return;
        }

    }
};

#endif
