#ifndef _QS2_QS_DESERIALIZER_H_
#define _QS2_QS_DESERIALIZER_H_

#include <Rcpp.h>
#include <tbb/global_control.h>
#include "qx_file_headers.h"

using namespace Rcpp;

#define FILE_READ_ERR_MSG "Failed to open for reading. Does the file exist? Do you have file permissions? Is the file name long? (>255 chars)"

template<typename block_compress_reader>
void qs_read_in_bytes(R_inpstream_t stream, void * buf, int length) {
    block_compress_reader * reader = reinterpret_cast<block_compress_reader*>(stream->data);
    reader->get_data( reinterpret_cast<char*>(buf), static_cast<uint64_t>(length) );
}

template<typename block_compress_reader>
int qs_read_in_char(R_inpstream_t stream) {
    char c;
    qs_read_in_bytes<block_compress_reader>(stream, &c, 1);
    return static_cast<int>(c);
}

template<typename block_compress_reader>
void R_UnserializeInit(R_inpstream_t stream, R_pstream_data_t data) {
    R_InitInPStream(stream, 
                    data, // member: data, void *
                    R_pstream_any_format, // format
                    qs_read_in_char<block_compress_reader>,
                    qs_read_in_bytes<block_compress_reader>,
                    NULL,        // phook
                    R_NilValue); // pdata
}

template<typename block_compress_reader>
SEXP qs_read_impl(void * _args) {
    struct R_inpstream_st * in = reinterpret_cast<struct R_inpstream_st*>(_args);
    SEXP output = R_Unserialize(in);
    reinterpret_cast<block_compress_reader*>(in->data)->finish();
    return output;
}

#endif

// typedef struct R_inpstream_st *R_inpstream_t;
// #define R_CODESET_MAX 63
// struct R_inpstream_st {
//   R_pstream_data_t data;                 // arbitrary data structure, void *
//   R_pstream_format_t type;               // R_pstream_any_format
//   int (*InChar)(R_inpstream_t);          // unused for binary type
//   void (*InBytes)(R_inpstream_t, void *, int); // this structure pointer, outgoing buffer, length of outgoing
//   SEXP (*InPersistHookFunc)(SEXP, SEXP); // Unclear what this does, just use NULL
//   SEXP InPersistHookData;                // use NULL
//   char native_encoding[R_CODESET_MAX + 1]; // set within R_initInPStream
//   void *nat2nat_obj;                       // NULL, set within R_initInPStream
//   void *nat2utf8_obj;                      // NULL, set within R_initInPStream
// };


//// description of relevant structures
// struct R_outpstream_st {
//   R_pstream_data_t data;                   // arbitrary data structure (e.g. membuf_st) cast to contain output, R_pstream_data_t is actually just typedef as void *
//   R_pstream_format_t type;                 // binary
//   int version;                             // 3
//   void (*OutChar)(R_outpstream_t, int);    // not used for binary or XDR
//   void (*OutBytes)(R_outpstream_t, void *, int); // this structure pointer, incoming buffer, length of incoming
//   SEXP (*OutPersistHookFunc)(SEXP, SEXP);  // Does NOTHING in serialize.c
//   SEXP OutPersistHookData;                 // Does NOTHIHG in serialize.c
// };
//// R_outpstream_t is a pointer to a R_outpstream_st

// typedef struct membuf_st {
//   R_size_t size; // initialized to 0, reserved size (as in std::vector)
//   R_size_t count; // initialized to 0, size of final raw vector
//   unsigned char *buf; // initialized to NULL, realloc on a NULL is the same as malloc
// } *membuf_t;

