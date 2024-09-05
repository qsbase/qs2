#ifndef _QS2_QD_DESERIALIZER_H_
#define _QS2_QD_DESERIALIZER_H_


#include <Rcpp.h>
#include <tbb/global_control.h>

#include "qx_file_headers.h"
#include "qd_constants.h"
#include "io/io_common.h"

#include "sf_external.h"
#include "simple_array/small_array.h"

using namespace Rcpp;

#define FILE_READ_ERR_MSG "Failed to open for reading. Does the file exist? Do you have file permissions? Is the file name long? (>255 chars)"


struct DelayedStringAssignments {
    SEXP container;
    std::unique_ptr< trqwe::small_array<char>[] > strings;
    DelayedStringAssignments(SEXP container, size_t n) : container(container), strings(MAKE_UNIQUE_BLOCK_CUSTOM(trqwe::small_array<char>, n)) {}
    void do_assignments() {
        size_t len = Rf_xlength(container);
        for(size_t i = 0; i < len; ++i) {
            trqwe::small_array<char> & str = strings[i];
            if(str.size() != 0) {
                SET_STRING_ELT( container, i, Rf_mkCharLenCE(str.data(), str.size(), CE_UTF8) );
            }
        }
    }
};

template<typename block_compress_reader>
struct QdataDeserializer {
    block_compress_reader & reader;
    std::vector<DelayedStringAssignments> delayed_string_assignments;
    const bool use_alt_rep;
    QdataDeserializer(block_compress_reader & reader, const bool use_alt_rep) : 
    reader(reader), use_alt_rep(use_alt_rep) {}

    private:
    // will also call get_pod_contiguous for object lengths
    void read_header_impl(const uint8_t header_byte, qstype & type, uint64_t & len) {
        uint8_t header_byte_5 = header_byte & bitmask_type_5;
        if(header_byte_5 == 0) {
            switch(header_byte) {
                case nil_header:
                    type = qstype::NIL;
                    return;
                case logical_header_8:
                    type = qstype::LOGICAL;
                    len = reader.template get_pod_contiguous<uint8_t>();
                return;
                case logical_header_16:
                    type = qstype::LOGICAL;
                    len = reader.template get_pod_contiguous<uint16_t>();
                    return;
                case logical_header_32:
                    type = qstype::LOGICAL;
                    len = reader.template get_pod_contiguous<uint32_t>();
                    return;
                case logical_header_64:
                    type = qstype::LOGICAL;
                    len = reader.template get_pod_contiguous<uint64_t>();
                    return;
                case integer_header_8:
                    type = qstype::INTEGER;
                    len = reader.template get_pod_contiguous<uint8_t>();
                    return;
                case integer_header_16:
                    type = qstype::INTEGER;
                    len = reader.template get_pod_contiguous<uint16_t>();
                    return;
                case integer_header_32:
                    type = qstype::INTEGER;
                    len = reader.template get_pod_contiguous<uint32_t>();
                    return;
                case integer_header_64:
                    type = qstype::INTEGER;
                    len = reader.template get_pod_contiguous<uint64_t>();
                    return;
                case numeric_header_8:
                    type = qstype::REAL;
                    len = reader.template get_pod_contiguous<uint8_t>();
                    return;
                case numeric_header_16:
                    type = qstype::REAL;
                    len = reader.template get_pod_contiguous<uint16_t>();
                    return;
                case numeric_header_32:
                    type = qstype::REAL;
                    len = reader.template get_pod_contiguous<uint32_t>();
                    return;
                case numeric_header_64:
                    type = qstype::REAL;
                    len = reader.template get_pod_contiguous<uint64_t>();
                    return;
                case complex_header_32:
                    type = qstype::COMPLEX;
                    len = reader.template get_pod_contiguous<uint32_t>();
                    return;
                case complex_header_64:
                    type = qstype::COMPLEX;
                    len = reader.template get_pod_contiguous<uint64_t>();
                    return;
                case character_header_8:
                    type = qstype::CHARACTER;
                    len = reader.template get_pod_contiguous<uint8_t>();
                    return;
                case character_header_16:
                    type = qstype::CHARACTER;
                    len = reader.template get_pod_contiguous<uint16_t>();
                    return;
                case character_header_32:
                    type = qstype::CHARACTER;
                    len = reader.template get_pod_contiguous<uint32_t>();
                    return;
                case character_header_64:
                    type = qstype::CHARACTER;
                    len = reader.template get_pod_contiguous<uint64_t>();
                    return;
                case list_header_8:
                    type = qstype::LIST;
                    len = reader.template get_pod_contiguous<uint8_t>();
                    return;
                case list_header_16:
                    type = qstype::LIST;
                    len = reader.template get_pod_contiguous<uint16_t>();
                    return;
                case list_header_32:
                    type = qstype::LIST;
                    len = reader.template get_pod_contiguous<uint32_t>();
                    return;
                case list_header_64:
                    type = qstype::LIST;
                    len = reader.template get_pod_contiguous<uint64_t>();
                    return;
                case raw_header_32:
                    type = qstype::RAW;
                    len = reader.template get_pod_contiguous<uint32_t>();
                    return;
                case raw_header_64:
                    type = qstype::RAW;
                    len = reader.template get_pod_contiguous<uint64_t>();
                    return;
                case attribute_header_8:
                    type = qstype::ATTRIBUTE;
                    len = reader.template get_pod_contiguous<uint8_t>();
                    return;
                case attribute_header_32:
                    type = qstype::ATTRIBUTE;
                    len = reader.template get_pod_contiguous<uint32_t>();
                    return;
                default:
                    reader.cleanup_and_throw("Unknown header type");
            }
        } else { // h5
            len = header_byte & bitmask_length_5;
            switch(header_byte_5) {
                case logical_header_5:
                    type = qstype::LOGICAL;
                    return;
                case integer_header_5:
                    type = qstype::INTEGER;
                    return;
                case numeric_header_5:
                    type = qstype::REAL;
                    return;
                case character_header_5:
                    type = qstype::CHARACTER;
                    return;
                case list_header_5:
                    type = qstype::LIST;
                    return;
                case attribute_header_5:
                    type = qstype::ATTRIBUTE;
                    return;
                default:
                    reader.cleanup_and_throw("Unknown header type");
            }
        }
    }
    public:
    // template <typename T> void shuffle_get_data(T * buf, const uint64_t len, const uint64_t bytesoftype = sizeof(T)) {
    //     if( shuffle && (len > MIN_SHUFFLE_ARRAYSIZE) ) {
    //         if(len > shuffleblock.size()) shuffleblock.resize(len);
    //         reader.get_data( reinterpret_cast<char*>(shuffleblock.data()), len );
    //         blosc_unshuffle(reinterpret_cast<uint8_t *>(shuffleblock.data()), reinterpret_cast<uint8_t *>(buf), len, bytesoftype);
    //     } else {
    //         reader.get_data( reinterpret_cast<char*>(buf), len );
    //     }
    // }

    // len, attr_length should be pre-initialized to 0
    void read_header(qstype & type, uint64_t & object_length, uint32_t & attr_length) {
        uint8_t header_byte = reader.template get_pod<uint8_t>();
        read_header_impl(header_byte, type, object_length);
        if(type == qstype::ATTRIBUTE) {
            attr_length = object_length;
            header_byte = reader.template get_pod_contiguous<uint8_t>();
            read_header_impl(header_byte, type, object_length);
            // if the first header_byte is attribute, the next header_byte must be a real object type
            if(type == qstype::ATTRIBUTE) {
                reader.cleanup_and_throw("Unknown header type");
            }
        }
    }

    inline void read_string_header(uint32_t & string_len) {
        string_len = reader.template get_pod<uint8_t>();
        switch(string_len) {
            case string_header_16: // 253
                string_len = reader.template get_pod_contiguous<uint16_t>();
                break;
            case string_header_32: // 254
                string_len = reader.template get_pod_contiguous<uint32_t>();
                break;
            case string_header_NA: // 255
                string_len = NA_STRING_LENGTH;
                break;
            default:
                break;
        }
    }

    // inline void read_string_UF_header(uint32_t & string_len) {
    //     uint8_t header_byte = reader.template get_pod<uint8_t>();
    //     if(header_byte < string_UF_max_8) {
    //         string_len = header_byte;
    //     } else if(header_byte == string_UF_header_NA) {
    //         string_len = NA_STRING_LENGTH;
    //     } else {
    //         string_len = reader.template get_pod_contiguous<uint32_t>();
    //     }
    // }

    void read_and_assign_attributes(SEXP object, const uint32_t attr_length) {
        SEXP aptr = Rf_allocList(attr_length);
        SET_ATTRIB(object, aptr); // assign immediately for protection
        std::string attr_name; // use std::string here, must be null terminated for Rf_install
        for(uint64_t i=0; i<attr_length; ++i) {
            uint32_t string_len;
            read_string_header(string_len);
            attr_name.resize(string_len);
            reader.get_data(&attr_name[0], string_len);
            SET_TAG(aptr, Rf_install(attr_name.c_str()));
            SEXP aobj = read_object();
            SETCAR(aptr, aobj);
            // setting class name also sets object bit
            // can also be set using Rf_classgets
            if( strcmp(attr_name.c_str(), "class") == 0 ) {
                if((Rf_isString(aobj)) & (Rf_xlength(aobj) >= 1)) {
                    SET_OBJECT(object, 1);
                }
            }
            aptr = CDR(aptr);
        }
    }

    SEXP read_object() {
        SEXP object = nullptr; // initialize to null value to hush compiler warnings
        qstype type;
        uint64_t object_length = 0;
        uint32_t attr_length = 0;
        read_header(type, object_length, attr_length);
        switch(type) {
            case qstype::NIL:
                return R_NilValue; // R_NilValue cannot have attributes, so return immediately
            case qstype::LOGICAL:
                object = PROTECT(Rf_allocVector(LGLSXP, object_length));
                read_and_assign_attributes(object, attr_length);
                reader.get_data( reinterpret_cast<char*>(LOGICAL(object)), object_length*4);
                break;
            case qstype::INTEGER:
                object = PROTECT(Rf_allocVector(INTSXP, object_length));
                read_and_assign_attributes(object, attr_length);
                reader.get_data( reinterpret_cast<char*>(INTEGER(object)), object_length*4 );
                break;
            case qstype::REAL:
                object = PROTECT(Rf_allocVector(REALSXP, object_length));
                read_and_assign_attributes(object, attr_length);
                reader.get_data( reinterpret_cast<char*>(REAL(object)), object_length*8 );
                break;
            case qstype::COMPLEX:
                object = PROTECT(Rf_allocVector(CPLXSXP, object_length));
                read_and_assign_attributes(object, attr_length);
                reader.get_data( reinterpret_cast<char*>(COMPLEX(object)), object_length*16 );
                break;
            case qstype::CHARACTER:
            {
                if(use_alt_rep) {
                    object = PROTECT(sf_vector(object_length)); // stringfish ALTREP vector
                    read_and_assign_attributes(object, attr_length);
                    auto & ref = sf_vec_data_ref(object);
                    for(uint64_t i=0; i < object_length; ++i) {
                        uint32_t string_length;
                        read_string_header(string_length);
                        if(string_length == NA_STRING_LENGTH) {
                            ref[i] = sfstring(NA_STRING);
                        } else {
                            if(string_length == 0) {
                                ref[i] = sfstring();
                            } else {
                                std::string xi;
                                xi.resize(string_length);
                                reader.get_data(&xi[0], string_length);
                                ref[i] = sfstring(xi, CE_UTF8);
                            }
                        }
                    }
                } else {
                    object = PROTECT(Rf_allocVector(STRSXP, object_length));
                    read_and_assign_attributes(object, attr_length);
                    if(object_length > 0) {
                        DelayedStringAssignments dsa(object, object_length);
                        for(uint64_t i=0; i<object_length; ++i) {
                            uint32_t string_length;
                            read_string_header(string_length);
                            if(string_length == NA_STRING_LENGTH) {
                                SET_STRING_ELT(object, i, NA_STRING);
                            } else if(string_length == 0) {
                                SET_STRING_ELT(object, i, R_BlankString);
                            } else {
                                trqwe::small_array<char> xi(string_length);
                                reader.get_data(xi.data(), string_length);
                                dsa.strings[i] = std::move(xi);
                                // const char * string_ptr = reader.get_ptr(string_length);
                                // if(string_ptr == nullptr) {
                                //     std::unique_ptr<char[]> string_buf(MAKE_UNIQUE_BLOCK(string_length));
                                //     reader.get_data(string_buf.get(), string_length);
                                //     SET_STRING_ELT(object, i, Rf_mkCharLenCE(string_buf.get(), string_length, CE_UTF8));
                                // } else {
                                //     SET_STRING_ELT(object, i, Rf_mkCharLenCE(string_ptr, string_length, CE_UTF8));
                                // }
                            }
                        }
                        delayed_string_assignments.push_back(std::move(dsa));
                    }
                }
                break;
            }
            case qstype::LIST:
            {
                object = PROTECT(Rf_allocVector(VECSXP, object_length));
                read_and_assign_attributes(object, attr_length);
                for(uint64_t i=0; i<object_length; ++i) {
                    SET_VECTOR_ELT(object, i, read_object());
                }
                break;
            }
            case qstype::RAW:
                object = PROTECT(Rf_allocVector(RAWSXP, object_length));
                read_and_assign_attributes(object, attr_length);
                reader.get_data( reinterpret_cast<char*>(RAW(object)), object_length );
                break;
            default:
                // this statement should be unreachable
                reader.cleanup_and_throw("something went wrong (reading object type)");
        }
        UNPROTECT(1);
        return object;
    }
    void do_delayed_string_assignments() {
        for(auto & dsa : delayed_string_assignments) {
            dsa.do_assignments();
        }
    }
};

#endif
