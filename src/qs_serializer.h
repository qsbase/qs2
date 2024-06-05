#ifndef _QS_SERIALIZER_H_
#define _QS_SERIALIZER_H_

#include <Rcpp.h>
#include <tbb/global_control.h>
#include "qx_file_headers.h"
  
using namespace Rcpp;

#define FILE_SAVE_ERR_MSG "Failed to open for writing. Does the directory exist? Do you have file permissions? Is the file name long? (>255 chars)"

struct qsSaveImplArgs {
    SEXP object;
    R_outpstream_t out;
};

template<typename block_compress_writer>
void qs_save_out_bytes(R_outpstream_t stream, void * buf, int length) {
    block_compress_writer * writer = reinterpret_cast<block_compress_writer*>(stream->data);
    writer->push_data( reinterpret_cast<char*>(buf), static_cast<uint64_t>(length) );
}

template<typename block_compress_writer>
void qs_save_out_char(R_outpstream_t stream, int length) {
    char c = static_cast<char>(length);
    qs_save_out_bytes<block_compress_writer>(stream, &c, 1);
}

template<typename block_compress_writer>
void R_SerializeInit(R_outpstream_t stream, block_compress_writer & writer) {
    R_InitOutPStream(stream, 
                     (R_pstream_data_t)(&writer), // member: data, void *
                     R_pstream_binary_format, // format
                     3, // version
                     qs_save_out_char<block_compress_writer>,
                     qs_save_out_bytes<block_compress_writer>,
                     NULL,        // phook
                     R_NilValue); // pdata
}

template<typename block_compress_writer>
SEXP qs_save_impl(void * _args) {
    qsSaveImplArgs * args = reinterpret_cast<qsSaveImplArgs*>(_args);
    R_Serialize(args->object, args->out);
    block_compress_writer* writer = reinterpret_cast<block_compress_writer*>(args->out->data);
    uint64_t hash = writer->finish();

    // it would be more consistent with the layered module approach to write outside of this implementation
    // but its easier to do here, as we don't have to smuggle the hash out, since we are forced to return a SEXP
    write_qx_hash(writer->myFile, hash);
    return R_NilValue;
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

