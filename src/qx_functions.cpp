#include <RcppParallel.h>

#include "io/block_module.h"
#include "io/cvector_module.h"
#include "io/filestream_module.h"
#include "io/xxhash_module.h"
#include "io/zstd_module.h"
#ifndef RCPP_PARALLEL_USE_TBB
#define RCPP_PARALLEL_USE_TBB 0
#endif
#if RCPP_PARALLEL_USE_TBB
#include "io/multithreaded_block_module.h"
#endif

#include "ascii_encoding/base85.h"
#include "ascii_encoding/base91.h"
#include "qoptions.h" // global default parameters
#include "qd_deserializer.h"
#include "qd_serializer.h"
#include "qs_deserializer.h"
#include "qs_serializer.h"
#include "qs_unwind_protect.h"
#include "qx_dump.h"

// qs2 format functions
// [[Rcpp::export(rng = false, invisible = true, signature = {object, file, compress_level = qopt("compress_level"), shuffle = qopt("shuffle"), nthreads = qopt("nthreads")})]]
SEXP qs_save(SEXP object, const std::string& file, const int compress_level, const bool shuffle, const int nthreads);
CVectorOut qs_serialize_impl(SEXP object, const int compress_level, const bool shuffle, const int nthreads);
// [[Rcpp::export(rng = false, invisible = true, signature = {object, compress_level = qopt("compress_level"), shuffle = qopt("shuffle"), nthreads = qopt("nthreads")})]]
SEXP qs_serialize(SEXP object, const int compress_level, const bool shuffle, const int nthreads);
unsigned char* c_qs_serialize(SEXP object, size_t* len, const int compress_level, const bool shuffle, const int nthreads);
bool c_qs_free(void* ptr);
// [[Rcpp::export(rng = false, signature = {file, validate_checksum = qopt("validate_checksum"), nthreads = qopt("nthreads")})]]
SEXP qs_read(const std::string& file, const bool validate_checksum, const int nthreads);
SEXP qs_deserialize_impl(CVectorIn& myFile, const bool validate_checksum, const int nthreads);
// [[Rcpp::export(rng = false, signature = {input, validate_checksum = qopt("validate_checksum"), nthreads = qopt("nthreads")})]]
SEXP qs_deserialize(SEXP input, const bool validate_checksum, const int nthreads);
SEXP c_qs_deserialize(const unsigned char* buffer, const size_t len, const bool validate_checksum, const int nthreads);

// qdata format functions
// [[Rcpp::export(rng = false, invisible = true, signature = {object, file, compress_level = qopt("compress_level"), shuffle = qopt("shuffle"), warn_unsupported_types = qopt("warn_unsupported_types"), nthreads = qopt("nthreads")})]]
SEXP qd_save(SEXP object, const std::string& file, const int compress_level, const bool shuffle, const bool warn_unsupported_types, const int nthreads);
CVectorOut qd_serialize_impl(SEXP object, const int compress_level, const bool shuffle, const bool warn_unsupported_types, const int nthreads);
// [[Rcpp::export(rng = false, signature = {object, compress_level = qopt("compress_level"), shuffle = qopt("shuffle"), warn_unsupported_types = qopt("warn_unsupported_types"), nthreads = qopt("nthreads")})]]
SEXP qd_serialize(SEXP object, const int compress_level, const bool shuffle, const bool warn_unsupported_types, const int nthreads);
unsigned char* c_qd_serialize(SEXP object, size_t* len, const int compress_level, const bool shuffle, const bool warn_unsupported_types, const int nthreads);
bool c_qd_free(void* ptr);
// [[Rcpp::export(rng = false, signature = {file, use_alt_rep = qopt("use_alt_rep"), validate_checksum = qopt("validate_checksum"), nthreads = qopt("nthreads")})]]
SEXP qd_read(const std::string& file, const bool use_alt_rep, const bool validate_checksum, const int nthreads);
SEXP qd_deserialize_impl(CVectorIn& myFile, const bool use_alt_rep, const bool validate_checksum, const int nthreads);
// [[Rcpp::export(rng = false, signature = {input, use_alt_rep = qopt("use_alt_rep"), validate_checksum = qopt("validate_checksum"), nthreads = qopt("nthreads")})]]
SEXP qd_deserialize(SEXP input, const bool use_alt_rep, const bool validate_checksum, const int nthreads);
SEXP c_qd_deserialize(const unsigned char* buffer, const size_t len, const bool use_alt_rep, const bool validate_checksum, const int nthreads);

// qx utility functions
// [[Rcpp::export(rng = false)]]
List qx_dump(const std::string& file);
// [[Rcpp::export(rng = false)]]
std::string check_SIMD();
// [[Rcpp::export(rng = false)]]
bool check_TBB();
// [[Rcpp::export(rng = false)]]
int check_internal_blocksize();
// [[Rcpp::export(rng = false)]]
int internal_set_blocksize(int size);
// [[Rcpp::export(rng = false)]]
int internal_is_utf8_locale(int size);

// standalone utility functions
// [[Rcpp::export(rng = false, signature = {data, compress_level = qopt("compress_level")})]]
std::vector<unsigned char> zstd_compress_raw(SEXP const data, int compress_level);
// [[Rcpp::export(rng = false)]]
RawVector zstd_decompress_raw(SEXP const data);
// [[Rcpp::export(rng = false)]]
int zstd_compress_bound(int size);
// [[Rcpp::export(rng = false)]]
std::vector<unsigned char> blosc_shuffle_raw(SEXP const data, int bytesofsize);
// [[Rcpp::export(rng = false)]]
std::vector<unsigned char> blosc_unshuffle_raw(SEXP const data, int bytesofsize);
// [[Rcpp::export(rng = false)]]
std::string xxhash_raw(SEXP const data);
// [[Rcpp::export(rng = false)]]
std::string base85_encode(const RawVector& rawdata);
// [[Rcpp::export(rng = false)]]
RawVector base85_decode(const std::string& encoded_string);
// [[Rcpp::export(rng = false)]]
std::string c_base91_encode(const RawVector& rawdata);
// [[Rcpp::export(rng = false)]]
RawVector c_base91_decode(const std::string& encoded_string);

// exported functions
void qx_export_functions(DllInfo* dll);

#define NTHREADS_ERR_MSG "nthreads > 1 requires TBB, see the readme and vignette for details."
#define COMPRESS_LEVEL_ERR_MSG "compress_level must be an integer between " + std::to_string(ZSTD_minCLevel()) + " and " + std::to_string(ZSTD_maxCLevel()) + "."
#define FILE_SAVE_ERR_MSG "For file " + file + ": " + "Failed to open for writing. Does the directory exist? Do you have file permissions? Is the file name long? (>255 chars)"
#define FILE_READ_ERR_MSG "For file " + file + ": " + "Failed to open for reading. Does the file exist? Do you have file permissions? Is the file name long? (>255 chars)"
#define NO_HASH_ERR_MSG "For file " + file + ": hash not stored, save file may be incomplete."
#define HASH_MISMATCH_ERR_MSG "For file " + file + ": hash mismatch, file may be corrupted."
#define IN_MEMORY_NO_HASH_ERR_MSG "Hash not stored, data may be incomplete."
#define IN_MEMORY_HASH_MISMATCH_ERR_MSG "Hash mismatch, data may be corrupted."
#define IN_MEMORY_RAW_VECTOR_INPUT_ERR_MSG "Input must be a raw vector."

///////////////////////////////////////////////////////////////////////////////
/* qs2 format functions */

#define DO_QS_SAVE(_STREAM_WRITER_, _BASE_CLASS_, _COMPRESSOR_, _HASHER_)                                              \
    _BASE_CLASS_<_STREAM_WRITER_, _COMPRESSOR_, _HASHER_, ErrorType::r_error, false> block_io(myFile, compress_level); \
    R_SerializeInit(&out, block_io);                                                                                   \
    qsSaveImplArgs args = {object, hash, &out};                                                                        \
    DO_JMPBUF_QS_SAVE();                                                                                               \
    DO_UNWIND_PROTECT_QS_SAVE(qs_save_impl, decltype(block_io), args);

SEXP qs_save(SEXP object, const std::string& file, const int compress_level, const bool shuffle, const int nthreads) {
#if RCPP_PARALLEL_USE_TBB == 0
    if (nthreads > 1) throw_error<ErrorType::r_error>(NTHREADS_ERR_MSG);
#endif

    if (compress_level > ZSTD_maxCLevel() || compress_level < ZSTD_minCLevel()) {
        throw_error<ErrorType::r_error>(COMPRESS_LEVEL_ERR_MSG);
    }

    OfStreamWriter myFile(R_ExpandFileName(file.c_str()));
    if (!myFile.isValid()) {
        throw_error<ErrorType::r_error>(FILE_SAVE_ERR_MSG);
    }
    write_qs2_header(myFile, shuffle);

    UNWIND_PROTECT_BEGIN()
    struct R_outpstream_st out;
    uint64_t hash = 0;
    if (nthreads > 1) {
#if RCPP_PARALLEL_USE_TBB
        tbb::global_control gc(tbb::global_control::parameter::max_allowed_parallelism, nthreads);
        if (shuffle) {
            DO_QS_SAVE(OfStreamWriter, BlockCompressWriterMT, ZstdShuffleCompressor, xxHashEnv);
        } else {
            DO_QS_SAVE(OfStreamWriter, BlockCompressWriterMT, ZstdCompressor, xxHashEnv);
        }
#endif
    } else {
        if (shuffle) {
            DO_QS_SAVE(OfStreamWriter, BlockCompressWriter, ZstdShuffleCompressor, xxHashEnv);
        } else {
            DO_QS_SAVE(OfStreamWriter, BlockCompressWriter, ZstdCompressor, xxHashEnv);
        }
    }
    write_qx_hash(myFile, hash);
    return R_NilValue;
    UNWIND_PROTECT_END();
    return R_NilValue;
}

CVectorOut qs_serialize_impl(SEXP object, const int compress_level, const bool shuffle, const int nthreads) {
#if RCPP_PARALLEL_USE_TBB == 0
    if (nthreads > 1) throw_error<ErrorType::r_error>(NTHREADS_ERR_MSG);
#endif

    if (compress_level > ZSTD_maxCLevel() || compress_level < ZSTD_minCLevel()) {
        throw_error<ErrorType::r_error>(COMPRESS_LEVEL_ERR_MSG);
    }

    CVectorOut myFile;
    write_qs2_header(myFile, shuffle);

    UNWIND_PROTECT_BEGIN()
    struct R_outpstream_st out;
    uint64_t hash = 0;
    if (nthreads > 1) {
#if RCPP_PARALLEL_USE_TBB
        tbb::global_control gc(tbb::global_control::parameter::max_allowed_parallelism, nthreads);
        if (shuffle) {
            DO_QS_SAVE(CVectorOut, BlockCompressWriterMT, ZstdShuffleCompressor, xxHashEnv);
        } else {
            DO_QS_SAVE(CVectorOut, BlockCompressWriterMT, ZstdCompressor, xxHashEnv);
        }
#endif
    } else {
        if (shuffle) {
            DO_QS_SAVE(CVectorOut, BlockCompressWriter, ZstdShuffleCompressor, xxHashEnv);
        } else {
            DO_QS_SAVE(CVectorOut, BlockCompressWriter, ZstdCompressor, xxHashEnv);
        }
    }
    uint64_t len = myFile.tellp();
    write_qx_hash(myFile, hash);  // must be done after getting length (position) from tellp
    myFile.seekp(len);
    return myFile;
    UNWIND_PROTECT_END();
    return CVectorOut{};
}

SEXP qs_serialize(SEXP object, const int compress_level, const bool shuffle, const int nthreads) {
    CVectorOut result = qs_serialize_impl(object, compress_level, shuffle, nthreads);
    SEXP output = Rf_allocVector(RAWSXP, result.tellp());
    std::memcpy(RAW(output), result.data(), result.tellp());
    return output;
}

unsigned char* c_qs_serialize(SEXP object, size_t* len, const int compress_level, const bool shuffle, const int nthreads) {
    if(len == nullptr) return nullptr;
    CVectorOut result = qs_serialize_impl(object, compress_level, shuffle, nthreads);
    *len = static_cast<size_t>(result.tellp());
    return reinterpret_cast<unsigned char*>(result.release());  // give up ownership of buffer
}

bool c_qs_free(void *buffer) {
    if (buffer == nullptr) { return false; }
    free(buffer);
    return true;
}

// DO_UNWIND_PROTECT macro assigns SEXP output
// return output within macro (within inner if-else) to avoid false positive RCHK warning
#define DO_QS_READ(_STREAM_READER_, _BASE_CLASS_, _DECOMPRESSOR_)                                                             \
    _BASE_CLASS_<_STREAM_READER_, _DECOMPRESSOR_, ErrorType::r_error> block_io(myFile);                                       \
    R_UnserializeInit<_BASE_CLASS_<_STREAM_READER_, _DECOMPRESSOR_, ErrorType::r_error>>(&in, (R_pstream_data_t)(&block_io)); \
    DO_JMPBUF_QS_READ();                                                                                                      \
    DO_UNWIND_PROTECT_QS_READ(qs_read_impl, decltype(block_io), in); \
    return output;

SEXP qs_read(const std::string& file, const bool validate_checksum, const int nthreads) {
#if RCPP_PARALLEL_USE_TBB == 0
    if (nthreads > 1) throw_error<ErrorType::r_error>(NTHREADS_ERR_MSG);
#endif

    IfStreamReader myFile(R_ExpandFileName(file.c_str()));
    if (!myFile.isValid()) {
        throw_error<ErrorType::r_error>(FILE_READ_ERR_MSG);
    }

    bool shuffle;
    uint64_t stored_hash;
    read_qs2_header(myFile, shuffle, stored_hash);
    if (stored_hash == 0) {
        throw_error<ErrorType::r_error>(NO_HASH_ERR_MSG);
    }

    if (validate_checksum) {
        uint64_t computed_hash = read_qx_hash(myFile);
        if (computed_hash != stored_hash) {
            throw_error<ErrorType::r_error>(HASH_MISMATCH_ERR_MSG);
        }
    }

    UNWIND_PROTECT_BEGIN()
    struct R_inpstream_st in;
    SEXP output = R_NilValue;
    if (nthreads > 1) {
#if RCPP_PARALLEL_USE_TBB != 0
        tbb::global_control gc(tbb::global_control::parameter::max_allowed_parallelism, nthreads);
        if (shuffle) {
            DO_QS_READ(IfStreamReader, BlockCompressReaderMT, ZstdShuffleDecompressor);
        } else {
            DO_QS_READ(IfStreamReader, BlockCompressReaderMT, ZstdDecompressor);
        }
#endif
    } else {
        if (shuffle) {
            DO_QS_READ(IfStreamReader, BlockCompressReader, ZstdShuffleDecompressor);
        } else {
            DO_QS_READ(IfStreamReader, BlockCompressReader, ZstdDecompressor);
        }
    }
    UNWIND_PROTECT_END();
    return R_NilValue;
}

SEXP qs_deserialize_impl(CVectorIn& myFile, const bool validate_checksum, const int nthreads) {
#if RCPP_PARALLEL_USE_TBB == 0
    if (nthreads > 1) throw_error<ErrorType::r_error>(NTHREADS_ERR_MSG);
#endif

    bool shuffle;
    uint64_t stored_hash;
    read_qs2_header(myFile, shuffle, stored_hash);
    if (stored_hash == 0) {
        throw_error<ErrorType::r_error>(IN_MEMORY_NO_HASH_ERR_MSG);
    }

    if (validate_checksum) {
        uint64_t computed_hash = read_qx_hash(myFile);
        if (computed_hash != stored_hash) {
            throw_error<ErrorType::r_error>(IN_MEMORY_HASH_MISMATCH_ERR_MSG);
        }
    }

    UNWIND_PROTECT_BEGIN()
    struct R_inpstream_st in;
    SEXP output = R_NilValue;
    if (nthreads > 1) {
#if RCPP_PARALLEL_USE_TBB != 0
        tbb::global_control gc(tbb::global_control::parameter::max_allowed_parallelism, nthreads);
        if (shuffle) {
            DO_QS_READ(CVectorIn, BlockCompressReaderMT, ZstdShuffleDecompressor);
        } else {
            DO_QS_READ(CVectorIn, BlockCompressReaderMT, ZstdDecompressor);
        }
#endif
    } else {
        if (shuffle) {
            DO_QS_READ(CVectorIn, BlockCompressReader, ZstdShuffleDecompressor);
        } else {
            DO_QS_READ(CVectorIn, BlockCompressReader, ZstdDecompressor);
        }
    }
    UNWIND_PROTECT_END();
    return R_NilValue;
}

SEXP qs_deserialize(SEXP input, const bool validate_checksum, const int nthreads) {
    if (TYPEOF(input) != RAWSXP) {
        throw_error<ErrorType::r_error>(IN_MEMORY_RAW_VECTOR_INPUT_ERR_MSG);
    }
    CVectorIn myFile(reinterpret_cast<char*>(RAW(input)), Rf_xlength(input));
    return qs_deserialize_impl(myFile, validate_checksum, nthreads);
}

SEXP c_qs_deserialize(const unsigned char* buffer, const size_t len, const bool validate_checksum, const int nthreads) {
    CVectorIn myFile(reinterpret_cast<const char*>(buffer), static_cast<const uint64_t>(len));
    return qs_deserialize_impl(myFile, validate_checksum, nthreads);
}

#define DO_QD_SAVE(_STREAM_WRITER_, _BASE_CLASS_, _COMPRESSOR_, _HASHER_)                                                                          \
    _BASE_CLASS_<_STREAM_WRITER_, _COMPRESSOR_, _HASHER_, ErrorType::cpp_error, true> writer(myFile, compress_level);                              \
    QdataSerializer<_BASE_CLASS_<_STREAM_WRITER_, _COMPRESSOR_, _HASHER_, ErrorType::cpp_error, true>> serializer(writer, warn_unsupported_types); \
    serializer.write_object(object);                                                                                                               \
    serializer.write_object_data();                                                                                                                \
    hash = writer.finish();

///////////////////////////////////////////////////////////////////////////////
/* qdata format functions */

SEXP qd_save(SEXP object, const std::string& file, const int compress_level, const bool shuffle, const bool warn_unsupported_types, const int nthreads) {
#if RCPP_PARALLEL_USE_TBB == 0
    if (nthreads > 1) throw std::runtime_error(NTHREADS_ERR_MSG);
#endif

    if (compress_level > ZSTD_maxCLevel() || compress_level < ZSTD_minCLevel()) {
        throw_error<ErrorType::r_error>(COMPRESS_LEVEL_ERR_MSG);
    }

    OfStreamWriter myFile(R_ExpandFileName(file.c_str()));
    if (!myFile.isValid()) {
        throw std::runtime_error(FILE_SAVE_ERR_MSG);
    }
    write_qdata_header(myFile, shuffle);
    uint64_t hash = 0;
    if (nthreads > 1) {
#if RCPP_PARALLEL_USE_TBB
        tbb::global_control gc(tbb::global_control::parameter::max_allowed_parallelism, nthreads);
        if (shuffle) {
            DO_QD_SAVE(OfStreamWriter, BlockCompressWriterMT, ZstdShuffleCompressor, xxHashEnv);
        } else {
            DO_QD_SAVE(OfStreamWriter, BlockCompressWriterMT, ZstdCompressor, xxHashEnv);
        }
#endif
    } else {
        if (shuffle) {
            DO_QD_SAVE(OfStreamWriter, BlockCompressWriter, ZstdShuffleCompressor, xxHashEnv);
        } else {
            DO_QD_SAVE(OfStreamWriter, BlockCompressWriter, ZstdCompressor, xxHashEnv);
        }
    }
    write_qx_hash(myFile, hash);
    return R_NilValue;
}

CVectorOut qd_serialize_impl(SEXP object, const int compress_level, const bool shuffle, const bool warn_unsupported_types, const int nthreads) {
#if RCPP_PARALLEL_USE_TBB == 0
    if (nthreads > 1) throw std::runtime_error(NTHREADS_ERR_MSG);
#endif

    if (compress_level > ZSTD_maxCLevel() || compress_level < ZSTD_minCLevel()) {
        throw_error<ErrorType::r_error>(COMPRESS_LEVEL_ERR_MSG);
    }

    CVectorOut myFile;
    write_qdata_header(myFile, shuffle);
    uint64_t hash = 0;
    if (nthreads > 1) {
#if RCPP_PARALLEL_USE_TBB
        tbb::global_control gc(tbb::global_control::parameter::max_allowed_parallelism, nthreads);
        if (shuffle) {
            DO_QD_SAVE(CVectorOut, BlockCompressWriterMT, ZstdShuffleCompressor, xxHashEnv);
        } else {
            DO_QD_SAVE(CVectorOut, BlockCompressWriterMT, ZstdCompressor, xxHashEnv);
        }
#endif
    } else {
        if (shuffle) {
            DO_QD_SAVE(CVectorOut, BlockCompressWriter, ZstdShuffleCompressor, xxHashEnv);
        } else {
            DO_QD_SAVE(CVectorOut, BlockCompressWriter, ZstdCompressor, xxHashEnv);
        }
    }
    uint64_t len = myFile.tellp();
    write_qx_hash(myFile, hash);  // must be done after getting length (position) from tellp
    myFile.seekp(len);
    return myFile;
}

SEXP qd_serialize(SEXP object, const int compress_level, const bool shuffle, const bool warn_unsupported_types, const int nthreads) {
    CVectorOut result = qd_serialize_impl(object, compress_level, shuffle, warn_unsupported_types, nthreads);
    SEXP output = Rf_allocVector(RAWSXP, result.tellp());
    std::memcpy(RAW(output), result.data(), result.tellp());
    return output;
}

unsigned char* c_qd_serialize(SEXP object, size_t* len, const int compress_level, const bool shuffle, const bool warn_unsupported_types, const int nthreads) {
    if(len == nullptr) { return nullptr; }
    CVectorOut result = qd_serialize_impl(object, compress_level, shuffle, warn_unsupported_types, nthreads);
    *len = static_cast<size_t>(result.tellp());
    return reinterpret_cast<unsigned char*>(result.release());  // give up ownership of buffer
}

bool c_qd_free(void *buffer) {
    if (buffer == nullptr) { return false; }
    free(buffer);
    return true;
}

// return output within macro (within inner if-else) to avoid false positive RCHK warning
#define DO_QD_READ(_STREAM_READER_, _BASE_CLASS_, _DECOMPRESSOR_)                                                             \
    _BASE_CLASS_<_STREAM_READER_, _DECOMPRESSOR_, ErrorType::cpp_error> reader(myFile);                                       \
    QdataDeserializer<_BASE_CLASS_<_STREAM_READER_, _DECOMPRESSOR_, ErrorType::cpp_error>> deserializer(reader, use_alt_rep); \
    output = PROTECT(deserializer.read_object());                                                                             \
    deserializer.read_object_data();                                                                                          \
    reader.finish();                                                                                                          \
    UNPROTECT(1);                                                                                                             \
    return output;

SEXP qd_read(const std::string& file, const bool use_alt_rep, const bool validate_checksum, const int nthreads) {
#if RCPP_PARALLEL_USE_TBB == 0
    if (nthreads > 1) throw std::runtime_error(NTHREADS_ERR_MSG);
#endif

    IfStreamReader myFile(R_ExpandFileName(file.c_str()));
    if (!myFile.isValid()) {
        throw std::runtime_error(FILE_READ_ERR_MSG);
    }
    bool shuffle;
    uint64_t stored_hash;
    read_qdata_header(myFile, shuffle, stored_hash);
    if (stored_hash == 0) {
        throw std::runtime_error(NO_HASH_ERR_MSG);
    }

    if (validate_checksum) {
        uint64_t computed_hash = read_qx_hash(myFile);
        if (computed_hash != stored_hash) {
            throw_error<ErrorType::r_error>(HASH_MISMATCH_ERR_MSG);
        }
    }

    SEXP output = R_NilValue;
    if (nthreads > 1) {
#if RCPP_PARALLEL_USE_TBB != 0
        tbb::global_control gc(tbb::global_control::parameter::max_allowed_parallelism, nthreads);
        if (shuffle) {
            DO_QD_READ(IfStreamReader, BlockCompressReaderMT, ZstdShuffleDecompressor);
        } else {
            DO_QD_READ(IfStreamReader, BlockCompressReaderMT, ZstdDecompressor);
        }
#endif
    } else {
        if (shuffle) {
            DO_QD_READ(IfStreamReader, BlockCompressReader, ZstdShuffleDecompressor);
        } else {
            DO_QD_READ(IfStreamReader, BlockCompressReader, ZstdDecompressor);
        }
    }
    return R_NilValue; // unreachable
}

SEXP qd_deserialize_impl(CVectorIn& myFile, const bool use_alt_rep, const bool validate_checksum, const int nthreads) {
#if RCPP_PARALLEL_USE_TBB == 0
    if (nthreads > 1) throw std::runtime_error(NTHREADS_ERR_MSG);
#endif

    bool shuffle;
    uint64_t stored_hash;
    read_qdata_header(myFile, shuffle, stored_hash);
    if (stored_hash == 0) {
        throw std::runtime_error(IN_MEMORY_NO_HASH_ERR_MSG);
    }

    if (validate_checksum) {
        uint64_t computed_hash = read_qx_hash(myFile);
        if (computed_hash != stored_hash) {
            throw_error<ErrorType::r_error>(IN_MEMORY_HASH_MISMATCH_ERR_MSG);
        }
    }

    SEXP output = R_NilValue;
    if (nthreads > 1) {
#if RCPP_PARALLEL_USE_TBB != 0
        tbb::global_control gc(tbb::global_control::parameter::max_allowed_parallelism, nthreads);
        if (shuffle) {
            DO_QD_READ(CVectorIn, BlockCompressReaderMT, ZstdShuffleDecompressor);
        } else {
            DO_QD_READ(CVectorIn, BlockCompressReaderMT, ZstdDecompressor);
        }
#endif
    } else {
        if (shuffle) {
            DO_QD_READ(CVectorIn, BlockCompressReader, ZstdShuffleDecompressor);
        } else {
            DO_QD_READ(CVectorIn, BlockCompressReader, ZstdDecompressor);
        }
    }
    return R_NilValue; // unreachable
}

SEXP qd_deserialize(SEXP input, const bool use_alt_rep, const bool validate_checksum, const int nthreads) {
    if (TYPEOF(input) != RAWSXP) {
        throw_error<ErrorType::r_error>(IN_MEMORY_RAW_VECTOR_INPUT_ERR_MSG);
    }
    CVectorIn myFile(reinterpret_cast<char*>(RAW(input)), Rf_xlength(input));
    return qd_deserialize_impl(myFile, use_alt_rep, validate_checksum, nthreads);
}

SEXP c_qd_deserialize(const unsigned char* buffer, const size_t len, const bool use_alt_rep, const bool validate_checksum, const int nthreads) {
    CVectorIn myFile(reinterpret_cast<const char*>(buffer), static_cast<const uint64_t>(len));
    return qd_deserialize_impl(myFile, use_alt_rep, validate_checksum, nthreads);
}


///////////////////////////////////////////////////////////////////////////////
/* qx utility functions */

List qx_dump(const std::string& file) {
    IfStreamReader myFile(R_ExpandFileName(file.c_str()));
    if (!myFile.isValid()) {
        throw std::runtime_error(FILE_READ_ERR_MSG);
    }
    qxHeaderInfo header_info = read_qx_header(myFile);

    std::tuple<std::vector<std::vector<unsigned char>>, std::vector<std::vector<unsigned char>>, std::vector<int>, std::string> output;
    if (header_info.shuffle) {
        output = qx_dump_impl<IfStreamReader, ZstdShuffleDecompressor>(myFile);
    } else {
        output = qx_dump_impl<IfStreamReader, ZstdDecompressor>(myFile);
    }

    return List::create(
        _["format"] = header_info.format,
        _["format_version"] = header_info.format_version,
        _["compression"] = header_info.compression,
        _["shuffle"] = header_info.shuffle,
        _["file_endian"] = header_info.file_endian,
        _["stored_hash"] = header_info.stored_hash,
        _["computed_hash"] = std::get<3>(output),
        _["zblocks"] = std::get<0>(output),
        _["blocks"] = std::get<1>(output),
        _["block_shuffled"] = std::get<2>(output)
    );
}

std::string check_SIMD() {
#if defined(__AVX2__)
    return "AVX2";
#elif defined(__SSE2__)
    return "SSE2";
#else
    return "no SIMD";
#endif
}

bool check_TBB() {
    return RCPP_PARALLEL_USE_TBB;
}

int check_internal_blocksize() {
    return MAX_BLOCKSIZE;
}

int internal_set_blocksize(const int size) {
#ifdef QS2_DYNAMIC_BLOCKSIZE
    int old = MAX_BLOCKSIZE;
    MAX_BLOCKSIZE = size;
    MIN_BLOCKSIZE = MAX_BLOCKSIZE - BLOCK_RESERVE;
    MAX_ZBLOCKSIZE = ZSTD_compressBound(MAX_BLOCKSIZE);
    return old;
#else
    throw std::runtime_error("dynamic blocksize compile option not enabled");
#endif
}

int internal_is_utf8_locale(const int size) {
    return IS_UTF8_LOCALE;
}

///////////////////////////////////////////////////////////////////////////////
/* standalone utility functions */

std::vector<unsigned char> zstd_compress_raw(SEXP const data, const int compress_level) {
    if (TYPEOF(data) != RAWSXP) {
        throw_error<ErrorType::r_error>(IN_MEMORY_RAW_VECTOR_INPUT_ERR_MSG);
    }
    if (compress_level > ZSTD_maxCLevel() || compress_level < ZSTD_minCLevel()) {
        throw_error<ErrorType::r_error>(COMPRESS_LEVEL_ERR_MSG);
    }
    uint64_t xsize = Rf_xlength(data);
    uint64_t zsize = ZSTD_compressBound(xsize);
    char* xdata = reinterpret_cast<char*>(RAW(data));
    std::vector<unsigned char> ret(zsize);
    char* retdata = reinterpret_cast<char*>(ret.data());
    zsize = ZSTD_compress(retdata, zsize, xdata, xsize, compress_level);
    ret.resize(zsize);
    return ret;
}
RawVector zstd_decompress_raw(SEXP const data) {
    if (TYPEOF(data) != RAWSXP) {
        throw_error<ErrorType::r_error>(IN_MEMORY_RAW_VECTOR_INPUT_ERR_MSG);
    }
    uint64_t zsize = Rf_xlength(data);
    char* xdata = reinterpret_cast<char*>(RAW(data));
    uint64_t retsize = ZSTD_getFrameContentSize(xdata, zsize);
    RawVector ret(retsize);
    char* retdata = reinterpret_cast<char*>(RAW(ret));
    ZSTD_decompress(retdata, retsize, xdata, zsize);
    return ret;
}

int zstd_compress_bound(const int size) { return ZSTD_compressBound(size); }

std::vector<unsigned char> blosc_shuffle_raw(SEXP const data, int bytesofsize) {
    if (bytesofsize != 4 && bytesofsize != 8) throw std::runtime_error("bytesofsize must be 4 or 8");
    uint64_t blocksize = Rf_xlength(data);
    uint8_t* xdata = reinterpret_cast<uint8_t*>(RAW(data));
    std::vector<uint8_t> xshuf(blocksize);
    blosc_shuffle(xdata, xshuf.data(), blocksize, bytesofsize);
    uint64_t remainder = blocksize % bytesofsize;
    uint64_t vectorizablebytes = blocksize - remainder;
    std::memcpy(xshuf.data() + vectorizablebytes, xdata + vectorizablebytes, remainder);
    return xshuf;
}

std::vector<unsigned char> blosc_unshuffle_raw(SEXP const data, int bytesofsize) {
    if (TYPEOF(data) != RAWSXP) {
        throw_error<ErrorType::r_error>(IN_MEMORY_RAW_VECTOR_INPUT_ERR_MSG);
    }
    if (bytesofsize != 4 && bytesofsize != 8) throw std::runtime_error("bytesofsize must be 4 or 8");
    uint64_t blocksize = Rf_xlength(data);
    uint8_t* xdata = reinterpret_cast<uint8_t*>(RAW(data));
    std::vector<uint8_t> xshuf(blocksize);
    blosc_unshuffle(xdata, xshuf.data(), blocksize, bytesofsize);
    uint64_t remainder = blocksize % bytesofsize;
    uint64_t vectorizablebytes = blocksize - remainder;
    std::memcpy(xshuf.data() + vectorizablebytes, xdata + vectorizablebytes, remainder);
    return xshuf;
}

std::string xxhash_raw(SEXP const data) {
    if (TYPEOF(data) != RAWSXP) {
        throw_error<ErrorType::r_error>(IN_MEMORY_RAW_VECTOR_INPUT_ERR_MSG);
    }
    uint64_t xsize = Rf_xlength(data);
    uint8_t* xdata = reinterpret_cast<uint8_t*>(RAW(data));
    xxHashEnv xenv = xxHashEnv();
    xenv.update(xdata, xsize);
    return std::to_string(xenv.digest());
}

std::string base85_encode(const RawVector& rawdata) {
    size_t size = Rf_xlength(rawdata);
    uint8_t* data = reinterpret_cast<uint8_t*>(RAW(rawdata));
    size_t size_partial = (size / 4) * 4;
    size_t encoded_size_partial = (size / 4) * 5;
    size_t encoded_size = encoded_size_partial + (size % 4 != 0 ? size % 4 + 1 : 0);
    std::string encoded_string(encoded_size, '\0');
    uint8_t* encoded = reinterpret_cast<uint8_t*>(const_cast<char*>(encoded_string.c_str()));

    size_t dbyte = 0;
    size_t ebyte = 0;
    while (dbyte < size_partial) {
        uint32_t value = 16777216UL * data[dbyte] + 65536UL * data[dbyte + 1] + 256UL * data[dbyte + 2] + data[dbyte + 3];
        encoded[ebyte] = base85_encoder_ring[value / 52200625UL];
        encoded[ebyte + 1] = base85_encoder_ring[value / 614125UL % 85];
        encoded[ebyte + 2] = base85_encoder_ring[value / 7225UL % 85];
        encoded[ebyte + 3] = base85_encoder_ring[value / 85UL % 85];
        encoded[ebyte + 4] = base85_encoder_ring[value % 85];
        dbyte += 4;
        ebyte += 5;
    }

    size_t leftover_bytes = size - size_partial;
    if (leftover_bytes == 1) {
        uint32_t value = data[dbyte];
        encoded[ebyte] = base85_encoder_ring[value / 85UL % 85];
        encoded[ebyte + 1] = base85_encoder_ring[value % 85];
    } else if (leftover_bytes == 2) {
        uint32_t value = 256UL * data[dbyte] + data[dbyte + 1];
        encoded[ebyte] = base85_encoder_ring[value / 7225UL];
        encoded[ebyte + 1] = base85_encoder_ring[value / 85UL % 85];
        encoded[ebyte + 2] = base85_encoder_ring[value % 85];
    } else if (leftover_bytes == 3) {
        uint32_t value = 65536UL * data[dbyte] + 256UL * data[dbyte + 1] + data[dbyte + 2];
        encoded[ebyte] = base85_encoder_ring[value / 614125UL % 85];
        encoded[ebyte + 1] = base85_encoder_ring[value / 7225UL % 85];
        encoded[ebyte + 2] = base85_encoder_ring[value / 85UL % 85];
        encoded[ebyte + 3] = base85_encoder_ring[value % 85];
    }
    return encoded_string;
}

RawVector base85_decode(const std::string& encoded_string) {
    size_t size = encoded_string.size();
    size_t size_partial = (size / 5) * 5;
    size_t leftover_bytes = size - size_partial;
    if (leftover_bytes == 1) throw std::runtime_error("base85_decode: corrupted input data, incorrect input size");
    uint8_t* data = reinterpret_cast<uint8_t*>(const_cast<char*>(encoded_string.data()));
    size_t decoded_size_partial = (size / 5) * 4;
    size_t decoded_size = decoded_size_partial + (size % 5 != 0 ? size % 5 - 1 : 0);
    RawVector decoded_vector(decoded_size);
    uint8_t* decoded = reinterpret_cast<uint8_t*>(RAW(decoded_vector));

    size_t dbyte = 0;
    size_t ebyte = 0;
    while (ebyte < size_partial) {
        base85_check_byte(data[ebyte]);
        base85_check_byte(data[ebyte + 1]);
        base85_check_byte(data[ebyte + 2]);
        base85_check_byte(data[ebyte + 3]);
        base85_check_byte(data[ebyte + 4]);
        uint64_t value_of = 52200625ULL * base85_decoder_ring[data[ebyte] - 32] + 614125ULL * base85_decoder_ring[data[ebyte + 1] - 32];
        value_of += 7225ULL * base85_decoder_ring[data[ebyte + 2] - 32] + 85ULL * base85_decoder_ring[data[ebyte + 3] - 32];
        value_of += base85_decoder_ring[data[ebyte + 4] - 32];

        // is there a better way to detect overflow?
        if (value_of > 4294967296ULL) throw std::runtime_error("base85_decode: corrupted input data, decoded block overflow");
        uint32_t value = static_cast<uint32_t>(value_of);
        decoded[dbyte] = value / 16777216UL;
        decoded[dbyte + 1] = value / 65536UL % 256;
        decoded[dbyte + 2] = value / 256UL % 256;
        decoded[dbyte + 3] = value % 256;
        ebyte += 5;
        dbyte += 4;
    }

    if (leftover_bytes == 2) {
        base85_check_byte(data[ebyte]);
        base85_check_byte(data[ebyte + 1]);
        uint32_t value = 85UL * base85_decoder_ring[data[ebyte] - 32] + base85_decoder_ring[data[ebyte + 1] - 32];
        if (value > 256) throw std::runtime_error("base85_decode: corrupted input data, decoded block overflow");
        decoded[dbyte] = value;
    } else if (leftover_bytes == 3) {
        base85_check_byte(data[ebyte]);
        base85_check_byte(data[ebyte + 1]);
        base85_check_byte(data[ebyte + 2]);
        uint32_t value = 7225UL * base85_decoder_ring[data[ebyte] - 32] + 85UL * base85_decoder_ring[data[ebyte + 1] - 32];
        value += base85_decoder_ring[data[ebyte + 2] - 32];
        if (value > 65536) throw std::runtime_error("base85_decode: corrupted input data, decoded block overflow");
        decoded[dbyte] = value / 256UL;
        decoded[dbyte + 1] = value % 256;
    } else if (leftover_bytes == 4) {
        base85_check_byte(data[ebyte]);
        base85_check_byte(data[ebyte + 1]);
        base85_check_byte(data[ebyte + 2]);
        base85_check_byte(data[ebyte + 3]);
        uint32_t value = 614125UL * base85_decoder_ring[data[ebyte] - 32] + 7225UL * base85_decoder_ring[data[ebyte + 1] - 32];
        value += 85UL * base85_decoder_ring[data[ebyte + 2] - 32] + base85_decoder_ring[data[ebyte + 3] - 32];
        if (value > 16777216) throw std::runtime_error("base85_decode: corrupted input data, decoded block overflow");
        decoded[dbyte] = value / 65536UL;
        decoded[dbyte + 1] = value / 256UL % 256;
        decoded[dbyte + 2] = value % 256;
    }
    return decoded_vector;
}

std::string c_base91_encode(const RawVector& rawdata) {
    basE91 b = basE91();
    size_t size = Rf_xlength(rawdata);
    size_t outsize = basE91_encode_bound(size);
    std::string output(outsize, '\0');
    size_t nb_encoded = basE91_encode_internal(&b, RAW(rawdata), size, const_cast<char*>(output.data()), outsize);
    nb_encoded += basE91_encode_end(&b, const_cast<char*>(output.data()) + nb_encoded, outsize - nb_encoded);
    output.resize(nb_encoded);
    return output;
}

RawVector c_base91_decode(const std::string& encoded_string) {
    basE91 b = basE91();
    size_t size = encoded_string.size();
    size_t outsize = basE91_decode_bound(size);
    std::vector<uint8_t> output(outsize);
    size_t nb_decoded = basE91_decode_internal(&b, encoded_string.data(), size, output.data(), outsize);
    nb_decoded += basE91_decode_end(&b, output.data() + nb_decoded, outsize - nb_decoded);
    output.resize(nb_decoded);
    return RawVector(output.begin(), output.end());
}

// [[Rcpp::init]]
void qx_export_functions(DllInfo* dll) {
    R_RegisterCCallable("qs2", "c_qs_serialize", (DL_FUNC)&c_qs_serialize);
    R_RegisterCCallable("qs2", "c_qs_deserialize", (DL_FUNC)&c_qs_deserialize);
    R_RegisterCCallable("qs2", "c_qs_free", (DL_FUNC)&c_qs_free);
    R_RegisterCCallable("qs2", "c_qd_serialize", (DL_FUNC)&c_qd_serialize);
    R_RegisterCCallable("qs2", "c_qd_deserialize", (DL_FUNC)&c_qd_deserialize);
    R_RegisterCCallable("qs2", "c_qd_free", (DL_FUNC)&c_qd_free);

    // from qoptions.h
    R_RegisterCCallable("qs2", "qs2_get_compress_level", (DL_FUNC)&qs2_get_compress_level);
    R_RegisterCCallable("qs2", "qs2_set_compress_level", (DL_FUNC)&qs2_set_compress_level);
    R_RegisterCCallable("qs2", "qs2_get_shuffle", (DL_FUNC)&qs2_get_shuffle);
    R_RegisterCCallable("qs2", "qs2_set_shuffle", (DL_FUNC)&qs2_set_shuffle);
    R_RegisterCCallable("qs2", "qs2_get_nthreads", (DL_FUNC)&qs2_get_nthreads);
    R_RegisterCCallable("qs2", "qs2_set_nthreads", (DL_FUNC)&qs2_set_nthreads);
    R_RegisterCCallable("qs2", "qs2_get_validate_checksum", (DL_FUNC)&qs2_get_validate_checksum);
    R_RegisterCCallable("qs2", "qs2_set_validate_checksum", (DL_FUNC)&qs2_set_validate_checksum);
    R_RegisterCCallable("qs2", "qs2_get_warn_unsupported_types", (DL_FUNC)&qs2_get_warn_unsupported_types);
    R_RegisterCCallable("qs2", "qs2_set_warn_unsupported_types", (DL_FUNC)&qs2_set_warn_unsupported_types);
    R_RegisterCCallable("qs2", "qs2_get_use_alt_rep", (DL_FUNC)&qs2_get_use_alt_rep);
    R_RegisterCCallable("qs2", "qs2_set_use_alt_rep", (DL_FUNC)&qs2_set_use_alt_rep);
}
