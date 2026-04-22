#include <RcppParallel.h>
#include <cmath>
#include <cstring>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <vector>

#include "io/block_module.h"
#include "io/filestream_module.h"
#include "io/xxhash_module.h"
#include "io/zstd_module.h"
#ifndef RCPP_PARALLEL_USE_TBB
#define RCPP_PARALLEL_USE_TBB 0
#endif
#if RCPP_PARALLEL_USE_TBB
#include <tbb/global_control.h>
#include "io/multithreaded_block_module.h"
#endif

#include "ascii_encoding/base85.h"
#include "ascii_encoding/base91.h"
#include "qoptions.h" // global default parameters
#include "qdata_format/detail/memory_stream.h"
#include "qdata_cpp_external_api.h"
#include "qd_deserializer.h"
#include "qd_serializer.h"
#include "r_error_policy.h"
#include "qs_deserializer.h"
#include "qs_serializer.h"
#include "qx_unwind_protect.h"
#include "qx_dump.h"
#include "zstd_file_functions.h"

using MemoryBuffer = std::vector<unsigned char>;
using MemoryReader = qdata::detail::memory_reader;
using MemoryWriter = qdata::detail::memory_writer<MemoryBuffer>;

// qs2 format functions
// [[Rcpp::export(rng = false, invisible = true, signature = {object, file, compress_level = qopt("compress_level"), shuffle = qopt("shuffle"), nthreads = qopt("nthreads")})]]
SEXP qs_save(SEXP object, const std::string& file, const int compress_level, const bool shuffle, int nthreads);
MemoryBuffer qs_serialize_impl(SEXP object, const int compress_level, const bool shuffle, int nthreads);
// [[Rcpp::export(rng = false, invisible = true, signature = {object, compress_level = qopt("compress_level"), shuffle = qopt("shuffle"), nthreads = qopt("nthreads")})]]
SEXP qs_serialize(SEXP object, const int compress_level, const bool shuffle, int nthreads);
// [[Rcpp::export(rng = false, signature = {file, validate_checksum = qopt("validate_checksum"), nthreads = qopt("nthreads")})]]
SEXP qs_read(const std::string& file, const bool validate_checksum, int nthreads);
SEXP qs_deserialize_impl(MemoryReader& myFile, const bool validate_checksum, int nthreads);
// [[Rcpp::export(rng = false, signature = {input, validate_checksum = qopt("validate_checksum"), nthreads = qopt("nthreads")})]]
SEXP qs_deserialize(SEXP input, const bool validate_checksum, int nthreads);

// qdata format functions
// [[Rcpp::export(rng = false, invisible = true, signature = {object, file, compress_level = qopt("compress_level"), shuffle = qopt("shuffle"), warn_unsupported_types = qopt("warn_unsupported_types"), nthreads = qopt("nthreads")})]]
SEXP qd_save(SEXP object, const std::string& file, const int compress_level, const bool shuffle, const bool warn_unsupported_types, int nthreads);
MemoryBuffer qd_serialize_impl(SEXP object, const int compress_level, const bool shuffle, const bool warn_unsupported_types, int nthreads);
// [[Rcpp::export(rng = false, signature = {object, compress_level = qopt("compress_level"), shuffle = qopt("shuffle"), warn_unsupported_types = qopt("warn_unsupported_types"), nthreads = qopt("nthreads")})]]
SEXP qd_serialize(SEXP object, const int compress_level, const bool shuffle, const bool warn_unsupported_types, int nthreads);
// [[Rcpp::export(rng = false, signature = {file, use_alt_rep = qopt("use_alt_rep"), validate_checksum = qopt("validate_checksum"), nthreads = qopt("nthreads")})]]
SEXP qd_read(const std::string& file, const bool use_alt_rep, const bool validate_checksum, int nthreads);
SEXP qd_deserialize_impl(MemoryReader& myFile, const bool validate_checksum, int nthreads);
// [[Rcpp::export(rng = false, signature = {input, use_alt_rep = qopt("use_alt_rep"), validate_checksum = qopt("validate_checksum"), nthreads = qopt("nthreads")})]]
SEXP qd_deserialize(SEXP input, const bool use_alt_rep, const bool validate_checksum, int nthreads);

// qx utility functions
// [[Rcpp::export(rng = false)]]
List qx_dump(const std::string& file);
// [[Rcpp::export(rng = false)]]
std::string check_SIMD();
// [[Rcpp::export(rng = false)]]
bool check_TBB();
// [[Rcpp::export(rng = false)]]
int internal_is_utf8_locale(int size);

// standalone utility functions
// [[Rcpp::export(rng = false, signature = {data, compress_level = qopt("compress_level")})]]
std::vector<unsigned char> zstd_compress_raw(SEXP const data, int compress_level);
// [[Rcpp::export(rng = false)]]
RawVector zstd_decompress_raw(SEXP const data);
// [[Rcpp::export(rng = false)]]
double zstd_compress_bound(SEXP const size);
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

#define NTHREADS_WARNING_MSG "nthreads > 1 requested but TBB not available; using nthreads = 1"
#define COMPRESS_LEVEL_ERR_MSG "compress_level must be an integer between " + std::to_string(ZSTD_minCLevel()) + " and " + std::to_string(ZSTD_maxCLevel()) + "."
#define FILE_SAVE_ERR_MSG "For file " + file + ": " + "Failed to open for writing. Does the directory exist? Do you have file permissions? Is the file name long? (>255 chars)"
#define FILE_READ_ERR_MSG "For file " + file + ": " + "Failed to open for reading. Does the file exist? Do you have file permissions? Is the file name long? (>255 chars)"
#define NO_HASH_ERR_MSG "For file " + file + ": hash not stored, save file may be incomplete."
#define HASH_MISMATCH_ERR_MSG "For file " + file + ": hash mismatch, file may be corrupted."
#define NO_HASH_WARN_MSG "For file " + file + ": hash not stored; object returned without checksum validation."
#define HASH_MISMATCH_WARN_MSG "For file " + file + ": hash mismatch after read; object returned but data may be corrupted."
#define IN_MEMORY_NO_HASH_ERR_MSG "Hash not stored, data may be incomplete."
#define IN_MEMORY_HASH_MISMATCH_ERR_MSG "Hash mismatch, data may be corrupted."
#define IN_MEMORY_NO_HASH_WARN_MSG "Hash not stored; object returned without checksum validation."
#define IN_MEMORY_HASH_MISMATCH_WARN_MSG "Hash mismatch after read; object returned but data may be corrupted."
#define IN_MEMORY_RAW_VECTOR_INPUT_ERR_MSG "Input must be a raw vector."
#define ALTREP_DISABLED_WARN_MSG "use_alt_rep is temporarily disabled for qdata; reading strings as ordinary character vectors."

namespace {

constexpr double MAX_EXACT_R_NUMERIC = 9007199254740991.0;

size_t parse_nonnegative_whole_size(SEXP const size, const char* arg_name) {
    if (Rf_xlength(size) != 1) {
        throw std::runtime_error(std::string(arg_name) + " must be a single non-negative whole number");
    }

    if (TYPEOF(size) == INTSXP) {
        int value = INTEGER(size)[0];
        if (value == NA_INTEGER || value < 0) {
            throw std::runtime_error(std::string(arg_name) + " must be a single non-negative whole number");
        }
        return static_cast<size_t>(value);
    }

    if (TYPEOF(size) == REALSXP) {
        double value = REAL(size)[0];
        if (!R_finite(value) || value < 0 || value > MAX_EXACT_R_NUMERIC || value != std::floor(value)) {
            throw std::runtime_error(std::string(arg_name) + " must be a single non-negative whole number");
        }
        if (value > static_cast<double>(std::numeric_limits<size_t>::max())) {
            throw std::runtime_error(std::string(arg_name) + " is too large for this platform");
        }
        return static_cast<size_t>(value);
    }

    throw std::runtime_error(std::string(arg_name) + " must be a single non-negative whole number");
}

}  // namespace

///////////////////////////////////////////////////////////////////////////////
/* qs2 format functions */

#define DO_QS_SAVE(_STREAM_WRITER_, _BASE_CLASS_, _COMPRESSOR_, _HASHER_)                                                  \
    _BASE_CLASS_<_STREAM_WRITER_, _COMPRESSOR_, _HASHER_, RErrorPolicy, false> block_io(myFile, compress_level);           \
    qx_with_unwind_cleanup(                                                                                                \
        block_io,                                                                                                          \
        [&]() -> SEXP {                                                                                                    \
            struct R_outpstream_st out;                                                                                    \
            R_SerializeInit(&out, block_io);                                                                               \
            qsSaveImplArgs args = {object, hash, &out};                                                                    \
            return qs_save_impl<decltype(block_io)>(static_cast<void*>(&args));                                             \
        },                                                                                                                 \
        "Object save interrupted, file may be incomplete");

SEXP qs_save(SEXP object, const std::string& file, const int compress_level, const bool shuffle, int nthreads) {
#if RCPP_PARALLEL_USE_TBB == 0
    if (nthreads > 1) {
        Rf_warning("%s", NTHREADS_WARNING_MSG);
        nthreads = 1;
    }
#endif

    if (compress_level > ZSTD_maxCLevel() || compress_level < ZSTD_minCLevel()) {
        throw_error<StdErrorPolicy>(COMPRESS_LEVEL_ERR_MSG);
    }

    OfStreamWriter myFile(R_ExpandFileName(file.c_str()));
    if (!myFile.isValid()) {
        throw_error<StdErrorPolicy>(FILE_SAVE_ERR_MSG);
    }
    write_qs2_header(myFile, shuffle);

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
}

MemoryBuffer qs_serialize_impl(SEXP object, const int compress_level, const bool shuffle, int nthreads) {
#if RCPP_PARALLEL_USE_TBB == 0
    if (nthreads > 1) {
        Rf_warning("%s", NTHREADS_WARNING_MSG);
        nthreads = 1;
    }
#endif

    if (compress_level > ZSTD_maxCLevel() || compress_level < ZSTD_minCLevel()) {
        throw_error<StdErrorPolicy>(COMPRESS_LEVEL_ERR_MSG);
    }

    MemoryWriter myFile;
    write_qs2_header(myFile, shuffle);

    uint64_t hash = 0;
    if (nthreads > 1) {
#if RCPP_PARALLEL_USE_TBB
        tbb::global_control gc(tbb::global_control::parameter::max_allowed_parallelism, nthreads);
        if (shuffle) {
            DO_QS_SAVE(MemoryWriter, BlockCompressWriterMT, ZstdShuffleCompressor, xxHashEnv);
        } else {
            DO_QS_SAVE(MemoryWriter, BlockCompressWriterMT, ZstdCompressor, xxHashEnv);
        }
#endif
    } else {
        if (shuffle) {
            DO_QS_SAVE(MemoryWriter, BlockCompressWriter, ZstdShuffleCompressor, xxHashEnv);
        } else {
            DO_QS_SAVE(MemoryWriter, BlockCompressWriter, ZstdCompressor, xxHashEnv);
        }
    }
    uint64_t len = myFile.tellp();
    write_qx_hash(myFile, hash);  // must be done after getting length (position) from tellp
    myFile.seekp(len);
    return myFile.take_bytes(static_cast<std::size_t>(len));
}

SEXP qs_serialize(SEXP object, const int compress_level, const bool shuffle, int nthreads) {
    MemoryBuffer result = qs_serialize_impl(object, compress_level, shuffle, nthreads);
    return RawVector(result.begin(), result.end());
}

#define DO_QS_READ(_STREAM_READER_, _BASE_CLASS_, _DECOMPRESSOR_, _RUNTIME_HASH_)                                             \
    _BASE_CLASS_<_STREAM_READER_, _DECOMPRESSOR_, RErrorPolicy> block_io(myFile);                                             \
    PROTECT(output = qx_with_unwind_cleanup(block_io, [&]() -> SEXP {                                                        \
        struct R_inpstream_st in;                                                                                             \
        R_UnserializeInit<_BASE_CLASS_<_STREAM_READER_, _DECOMPRESSOR_, RErrorPolicy>>(&in, (R_pstream_data_t)(&block_io)); \
        SEXP protected_output = qs_read_impl<decltype(block_io)>(static_cast<void*>(&in));                                   \
        _RUNTIME_HASH_ = block_io.get_hash_digest();                                                                          \
        return protected_output;                                                                                              \
    }));

SEXP qs_read(const std::string& file, const bool validate_checksum, int nthreads) {
#if RCPP_PARALLEL_USE_TBB == 0
    if (nthreads > 1) {
        Rf_warning("%s", NTHREADS_WARNING_MSG);
        nthreads = 1;
    }
#endif

    IfStreamReader myFile(R_ExpandFileName(file.c_str()));
    if (!myFile.isValid()) {
        throw_error<StdErrorPolicy>(FILE_READ_ERR_MSG);
    }

    bool shuffle;
    uint64_t stored_hash;
    read_qs2_header(myFile, shuffle, stored_hash);
    if (validate_checksum) {
        if (stored_hash == 0) {
            throw_error<StdErrorPolicy>(NO_HASH_ERR_MSG);
        }
        uint64_t computed_hash = read_qx_hash(myFile);
        if (computed_hash != stored_hash) {
            throw_error<StdErrorPolicy>(HASH_MISMATCH_ERR_MSG);
        }
    }

    SEXP output = R_NilValue;
    uint64_t runtime_hash = 0;
    if (nthreads > 1) {
#if RCPP_PARALLEL_USE_TBB != 0
        tbb::global_control gc(tbb::global_control::parameter::max_allowed_parallelism, nthreads);
        if (shuffle) {
            DO_QS_READ(IfStreamReader, BlockCompressReaderMT, ZstdShuffleDecompressor, runtime_hash);
        } else {
            DO_QS_READ(IfStreamReader, BlockCompressReaderMT, ZstdDecompressor, runtime_hash);
        }
#endif
    } else {
        if (shuffle) {
            DO_QS_READ(IfStreamReader, BlockCompressReader, ZstdShuffleDecompressor, runtime_hash);
        } else {
            DO_QS_READ(IfStreamReader, BlockCompressReader, ZstdDecompressor, runtime_hash);
        }
    }
    if (!validate_checksum) {
        if (stored_hash == 0) {
            Rf_warning("%s", (NO_HASH_WARN_MSG).c_str());
        } else if (runtime_hash != stored_hash) {
            Rf_warning("%s", (HASH_MISMATCH_WARN_MSG).c_str());
        }
    }
    UNPROTECT(1);
    return output;
}

SEXP qs_deserialize_impl(MemoryReader& myFile, const bool validate_checksum, int nthreads) {
#if RCPP_PARALLEL_USE_TBB == 0
    if (nthreads > 1) {
        Rf_warning("%s", NTHREADS_WARNING_MSG);
        nthreads = 1;
    }
#endif

    bool shuffle;
    uint64_t stored_hash;
    read_qs2_header(myFile, shuffle, stored_hash);
    if (validate_checksum) {
        if (stored_hash == 0) {
            throw_error<StdErrorPolicy>(IN_MEMORY_NO_HASH_ERR_MSG);
        }
        uint64_t computed_hash = read_qx_hash(myFile);
        if (computed_hash != stored_hash) {
            throw_error<StdErrorPolicy>(IN_MEMORY_HASH_MISMATCH_ERR_MSG);
        }
    }

    SEXP output = R_NilValue;
    uint64_t runtime_hash = 0;
    if (nthreads > 1) {
#if RCPP_PARALLEL_USE_TBB != 0
        tbb::global_control gc(tbb::global_control::parameter::max_allowed_parallelism, nthreads);
        if (shuffle) {
            DO_QS_READ(MemoryReader, BlockCompressReaderMT, ZstdShuffleDecompressor, runtime_hash);
        } else {
            DO_QS_READ(MemoryReader, BlockCompressReaderMT, ZstdDecompressor, runtime_hash);
        }
#endif
    } else {
        if (shuffle) {
            DO_QS_READ(MemoryReader, BlockCompressReader, ZstdShuffleDecompressor, runtime_hash);
        } else {
            DO_QS_READ(MemoryReader, BlockCompressReader, ZstdDecompressor, runtime_hash);
        }
    }
    if (!validate_checksum) {
        if (stored_hash == 0) {
            Rf_warning("%s", IN_MEMORY_NO_HASH_WARN_MSG);
        } else if (runtime_hash != stored_hash) {
            Rf_warning("%s", IN_MEMORY_HASH_MISMATCH_WARN_MSG);
        }
    }
    UNPROTECT(1);
    return output;
}

SEXP qs_deserialize(SEXP input, const bool validate_checksum, int nthreads) {
    if (TYPEOF(input) != RAWSXP) {
        throw_error<StdErrorPolicy>(IN_MEMORY_RAW_VECTOR_INPUT_ERR_MSG);
    }
    MemoryReader myFile(RAW(input), static_cast<const uint64_t>(Rf_xlength(input)));
    return qs_deserialize_impl(myFile, validate_checksum, nthreads);
}

inline void warn_if_qdata_altrep_requested(const bool use_alt_rep) {
    if (use_alt_rep) {
        Rf_warning("%s", ALTREP_DISABLED_WARN_MSG);
    }
}

#define DO_QD_SAVE(_STREAM_WRITER_, _BASE_CLASS_, _COMPRESSOR_, _HASHER_)                                                                          \
    _BASE_CLASS_<_STREAM_WRITER_, _COMPRESSOR_, _HASHER_, StdErrorPolicy, true> writer(myFile, compress_level);                                   \
    QdataSerializer<_BASE_CLASS_<_STREAM_WRITER_, _COMPRESSOR_, _HASHER_, StdErrorPolicy, true>> serializer(writer, warn_unsupported_types);      \
    qx_with_unwind_cleanup(                                                                                                                        \
        writer,                                                                                                                                     \
        [&]() -> SEXP {                                                                                                                             \
            serializer.write_object(object);                                                                                                        \
            serializer.write_object_data();                                                                                                         \
            hash = writer.finish();                                                                                                                 \
            return R_NilValue;                                                                                                                      \
        },                                                                                                                                          \
        "Object save interrupted, file may be incomplete");

///////////////////////////////////////////////////////////////////////////////
/* qdata format functions */

SEXP qd_save(SEXP object, const std::string& file, const int compress_level, const bool shuffle, const bool warn_unsupported_types, int nthreads) {
#if RCPP_PARALLEL_USE_TBB == 0
    if (nthreads > 1) {
        Rf_warning("%s", NTHREADS_WARNING_MSG);
        nthreads = 1;
    }
#endif

    if (compress_level > ZSTD_maxCLevel() || compress_level < ZSTD_minCLevel()) {
        throw_error<StdErrorPolicy>(COMPRESS_LEVEL_ERR_MSG);
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

MemoryBuffer qd_serialize_impl(SEXP object, const int compress_level, const bool shuffle, const bool warn_unsupported_types, int nthreads) {
#if RCPP_PARALLEL_USE_TBB == 0
    if (nthreads > 1) {
        Rf_warning("%s", NTHREADS_WARNING_MSG);
        nthreads = 1;
    }
#endif

    if (compress_level > ZSTD_maxCLevel() || compress_level < ZSTD_minCLevel()) {
        throw_error<StdErrorPolicy>(COMPRESS_LEVEL_ERR_MSG);
    }

    MemoryWriter myFile;
    write_qdata_header(myFile, shuffle);
    uint64_t hash = 0;
    if (nthreads > 1) {
#if RCPP_PARALLEL_USE_TBB
        tbb::global_control gc(tbb::global_control::parameter::max_allowed_parallelism, nthreads);
        if (shuffle) {
            DO_QD_SAVE(MemoryWriter, BlockCompressWriterMT, ZstdShuffleCompressor, xxHashEnv);
        } else {
            DO_QD_SAVE(MemoryWriter, BlockCompressWriterMT, ZstdCompressor, xxHashEnv);
        }
#endif
    } else {
        if (shuffle) {
            DO_QD_SAVE(MemoryWriter, BlockCompressWriter, ZstdShuffleCompressor, xxHashEnv);
        } else {
            DO_QD_SAVE(MemoryWriter, BlockCompressWriter, ZstdCompressor, xxHashEnv);
        }
    }
    uint64_t len = myFile.tellp();
    write_qx_hash(myFile, hash);  // must be done after getting length (position) from tellp
    myFile.seekp(len);
    return myFile.take_bytes(static_cast<std::size_t>(len));
}

SEXP qd_serialize(SEXP object, const int compress_level, const bool shuffle, const bool warn_unsupported_types, int nthreads) {
    MemoryBuffer result = qd_serialize_impl(object, compress_level, shuffle, warn_unsupported_types, nthreads);
    return RawVector(result.begin(), result.end());
}

// DO_QD_READ macro assigns SEXP output
#define DO_QD_READ(_STREAM_READER_, _BASE_CLASS_, _DECOMPRESSOR_, _RUNTIME_HASH_)                                             \
    _BASE_CLASS_<_STREAM_READER_, _DECOMPRESSOR_, StdErrorPolicy> reader(myFile);                                             \
    QdataDeserializer<_BASE_CLASS_<_STREAM_READER_, _DECOMPRESSOR_, StdErrorPolicy>> deserializer(reader);                    \
    PROTECT(output = qx_with_unwind_cleanup(reader, [&]() -> SEXP {                                                          \
        return deserializer.read_root_object(_RUNTIME_HASH_);                                                                \
    }));

SEXP qd_read(const std::string& file, const bool use_alt_rep, const bool validate_checksum, int nthreads) {
#if RCPP_PARALLEL_USE_TBB == 0
    if (nthreads > 1) {
        Rf_warning("%s", NTHREADS_WARNING_MSG);
        nthreads = 1;
    }
#endif

    warn_if_qdata_altrep_requested(use_alt_rep);

    IfStreamReader myFile(R_ExpandFileName(file.c_str()));
    if (!myFile.isValid()) {
        throw std::runtime_error(FILE_READ_ERR_MSG);
    }
    bool shuffle;
    uint64_t stored_hash;
    read_qdata_header(myFile, shuffle, stored_hash);
    if (validate_checksum) {
        if (stored_hash == 0) {
            throw std::runtime_error(NO_HASH_ERR_MSG);
        }
        uint64_t computed_hash = read_qx_hash(myFile);
        if (computed_hash != stored_hash) {
            throw_error<StdErrorPolicy>(HASH_MISMATCH_ERR_MSG);
        }
    }

    SEXP output = R_NilValue;
    uint64_t runtime_hash = 0;
    if (nthreads > 1) {
#if RCPP_PARALLEL_USE_TBB != 0
        tbb::global_control gc(tbb::global_control::parameter::max_allowed_parallelism, nthreads);
        if (shuffle) {
            DO_QD_READ(IfStreamReader, BlockCompressReaderMT, ZstdShuffleDecompressor, runtime_hash);
        } else {
            DO_QD_READ(IfStreamReader, BlockCompressReaderMT, ZstdDecompressor, runtime_hash);
        }
#endif
    } else {
        if (shuffle) {
            DO_QD_READ(IfStreamReader, BlockCompressReader, ZstdShuffleDecompressor, runtime_hash);
        } else {
            DO_QD_READ(IfStreamReader, BlockCompressReader, ZstdDecompressor, runtime_hash);
        }
    }
    if (!validate_checksum) {
        if (stored_hash == 0) {
            Rf_warning("%s", (NO_HASH_WARN_MSG).c_str());
        } else if (runtime_hash != stored_hash) {
            Rf_warning("%s", (HASH_MISMATCH_WARN_MSG).c_str());
        }
    }
    UNPROTECT(1);
    return output;
}

SEXP qd_deserialize_impl(MemoryReader& myFile, const bool validate_checksum, int nthreads) {
#if RCPP_PARALLEL_USE_TBB == 0
    if (nthreads > 1) {
        Rf_warning("%s", NTHREADS_WARNING_MSG);
        nthreads = 1;
    }
#endif

    bool shuffle;
    uint64_t stored_hash;
    read_qdata_header(myFile, shuffle, stored_hash);
    if (validate_checksum) {
        if (stored_hash == 0) {
            throw std::runtime_error(IN_MEMORY_NO_HASH_ERR_MSG);
        }
        uint64_t computed_hash = read_qx_hash(myFile);
        if (computed_hash != stored_hash) {
            throw_error<StdErrorPolicy>(IN_MEMORY_HASH_MISMATCH_ERR_MSG);
        }
    }

    SEXP output = R_NilValue;
    uint64_t runtime_hash = 0;
    if (nthreads > 1) {
#if RCPP_PARALLEL_USE_TBB != 0
        tbb::global_control gc(tbb::global_control::parameter::max_allowed_parallelism, nthreads);
        if (shuffle) {
            DO_QD_READ(MemoryReader, BlockCompressReaderMT, ZstdShuffleDecompressor, runtime_hash);
        } else {
            DO_QD_READ(MemoryReader, BlockCompressReaderMT, ZstdDecompressor, runtime_hash);
        }
#endif
    } else {
        if (shuffle) {
            DO_QD_READ(MemoryReader, BlockCompressReader, ZstdShuffleDecompressor, runtime_hash);
        } else {
            DO_QD_READ(MemoryReader, BlockCompressReader, ZstdDecompressor, runtime_hash);
        }
    }
    if (!validate_checksum) {
        if (stored_hash == 0) {
            Rf_warning("%s", IN_MEMORY_NO_HASH_WARN_MSG);
        } else if (runtime_hash != stored_hash) {
            Rf_warning("%s", IN_MEMORY_HASH_MISMATCH_WARN_MSG);
        }
    }
    UNPROTECT(1);
    return output;
}

SEXP qd_deserialize(SEXP input, const bool use_alt_rep, const bool validate_checksum, int nthreads) {
    warn_if_qdata_altrep_requested(use_alt_rep);
    if (TYPEOF(input) != RAWSXP) {
        throw_error<StdErrorPolicy>(IN_MEMORY_RAW_VECTOR_INPUT_ERR_MSG);
    }
    MemoryReader myFile(RAW(input), static_cast<const uint64_t>(Rf_xlength(input)));
    return qd_deserialize_impl(myFile, validate_checksum, nthreads);
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

int internal_is_utf8_locale(const int size) {
    return IS_UTF8_LOCALE;
}

// [[Rcpp::export(rng = false)]]
std::string internal_compute_qx_hash(const std::string& file) {
    IfStreamReader myFile(R_ExpandFileName(file.c_str()));
    if (!myFile.isValid()) {
        throw_error<StdErrorPolicy>(FILE_READ_ERR_MSG);
    }
    myFile.seekg(24);
    return std::to_string(read_qx_hash(myFile));
}

// [[Rcpp::export(rng = false, invisible = true)]]
SEXP internal_write_qx_hash(const std::string& file, const std::string& hash_string) {
    if (hash_string.empty()) {
        throw_error<StdErrorPolicy>("internal_write_qx_hash: empty hash string");
    }

    size_t parsed_chars = 0;
    uint64_t hash_value = 0;
    try {
        hash_value = std::stoull(hash_string, &parsed_chars, 10);
    } catch (...) {
        throw_error<StdErrorPolicy>("internal_write_qx_hash: hash string is not a valid unsigned integer");
    }
    if (parsed_chars != hash_string.size()) {
        throw_error<StdErrorPolicy>("internal_write_qx_hash: hash string is not a valid unsigned integer");
    }

    std::fstream out(R_ExpandFileName(file.c_str()), std::ios::in | std::ios::out | std::ios::binary);
    if (!out.is_open()) {
        throw_error<StdErrorPolicy>(FILE_SAVE_ERR_MSG);
    }
    out.seekp(HEADER_HASH_POSITION);
    out.write(reinterpret_cast<const char*>(&hash_value), sizeof(hash_value));
    if (!out) {
        throw_error<StdErrorPolicy>("internal_write_qx_hash: failed to write hash");
    }
    return R_NilValue;
}

///////////////////////////////////////////////////////////////////////////////
/* standalone utility functions */

std::vector<unsigned char> zstd_compress_raw(SEXP const data, const int compress_level) {
    if (compress_level > ZSTD_maxCLevel() || compress_level < ZSTD_minCLevel()) {
        throw_error<StdErrorPolicy>(COMPRESS_LEVEL_ERR_MSG);
    }
    if (TYPEOF(data) != RAWSXP) throw std::runtime_error(IN_MEMORY_RAW_VECTOR_INPUT_ERR_MSG);
    size_t xsize = static_cast<size_t>(Rf_xlength(data));
    size_t zbound = ZSTD_compressBound(xsize);
    if (ZSTD_isError(zbound)) {
        throw_error<StdErrorPolicy>(std::string("zstd_compress_raw failed: ") + ZSTD_getErrorName(zbound));
    }
    const char* xdata = reinterpret_cast<const char*>(RAW(data));
    std::vector<unsigned char> ret(zbound);
    char* retdata = reinterpret_cast<char*>(ret.data());
    size_t zsize = ZSTD_compress(retdata, zbound, xdata, xsize, compress_level);
    if (ZSTD_isError(zsize)) {
        throw_error<StdErrorPolicy>(std::string("zstd_compress_raw failed: ") + ZSTD_getErrorName(zsize));
    }
    ret.resize(zsize);
    return ret;
}
RawVector zstd_decompress_raw(SEXP const data) {
    if (TYPEOF(data) != RAWSXP) throw std::runtime_error(IN_MEMORY_RAW_VECTOR_INPUT_ERR_MSG);
    size_t zsize = static_cast<size_t>(Rf_xlength(data));
    const char* xdata = reinterpret_cast<const char*>(RAW(data));
    unsigned long long retsize = ZSTD_getFrameContentSize(xdata, zsize);
    if (retsize == ZSTD_CONTENTSIZE_ERROR) {
        throw_error<StdErrorPolicy>("zstd_decompress_raw failed: input is not a valid zstd frame");
    }
    if (retsize == ZSTD_CONTENTSIZE_UNKNOWN) {
        throw_error<StdErrorPolicy>("zstd_decompress_raw failed: frame size is unknown");
    }
    if (retsize > static_cast<unsigned long long>(R_XLEN_T_MAX)) {
        throw_error<StdErrorPolicy>("zstd_decompress_raw failed: output is too large for an R raw vector");
    }
    RawVector ret(static_cast<R_xlen_t>(retsize));
    char* retdata = reinterpret_cast<char*>(RAW(ret));
    size_t decoded_size = ZSTD_decompress(retdata, static_cast<size_t>(retsize), xdata, zsize);
    if (ZSTD_isError(decoded_size)) {
        throw_error<StdErrorPolicy>(std::string("zstd_decompress_raw failed: ") + ZSTD_getErrorName(decoded_size));
    }
    if (decoded_size != static_cast<size_t>(retsize)) {
        throw_error<StdErrorPolicy>("zstd_decompress_raw failed: decoded size mismatch");
    }
    return ret;
}

double zstd_compress_bound(SEXP const size) {
    size_t parsed_size = parse_nonnegative_whole_size(size, "size");
    size_t bound = ZSTD_compressBound(parsed_size);
    if (bound > MAX_EXACT_R_NUMERIC) {
        throw_error<StdErrorPolicy>("zstd_compress_bound result is too large to represent exactly in R");
    }
    return static_cast<double>(bound);
}

std::vector<unsigned char> blosc_shuffle_raw(SEXP const data, int bytesofsize) {
    if (TYPEOF(data) != RAWSXP) throw std::runtime_error(IN_MEMORY_RAW_VECTOR_INPUT_ERR_MSG);
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
    if (TYPEOF(data) != RAWSXP) throw std::runtime_error(IN_MEMORY_RAW_VECTOR_INPUT_ERR_MSG);
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
    if (TYPEOF(data) != RAWSXP) throw std::runtime_error(IN_MEMORY_RAW_VECTOR_INPUT_ERR_MSG);
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
    uint8_t* encoded = reinterpret_cast<uint8_t*>(encoded_string.data());

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
    const uint8_t* data = reinterpret_cast<const uint8_t*>(encoded_string.data());
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
        if (value_of >= 4294967296ULL) throw std::runtime_error("base85_decode: corrupted input data, decoded block overflow");
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
        if (value >= 256) throw std::runtime_error("base85_decode: corrupted input data, decoded block overflow");
        decoded[dbyte] = value;
    } else if (leftover_bytes == 3) {
        base85_check_byte(data[ebyte]);
        base85_check_byte(data[ebyte + 1]);
        base85_check_byte(data[ebyte + 2]);
        uint32_t value = 7225UL * base85_decoder_ring[data[ebyte] - 32] + 85UL * base85_decoder_ring[data[ebyte + 1] - 32];
        value += base85_decoder_ring[data[ebyte + 2] - 32];
        if (value >= 65536) throw std::runtime_error("base85_decode: corrupted input data, decoded block overflow");
        decoded[dbyte] = value / 256UL;
        decoded[dbyte + 1] = value % 256;
    } else if (leftover_bytes == 4) {
        base85_check_byte(data[ebyte]);
        base85_check_byte(data[ebyte + 1]);
        base85_check_byte(data[ebyte + 2]);
        base85_check_byte(data[ebyte + 3]);
        uint32_t value = 614125UL * base85_decoder_ring[data[ebyte] - 32] + 7225UL * base85_decoder_ring[data[ebyte + 1] - 32];
        value += 85UL * base85_decoder_ring[data[ebyte + 2] - 32] + base85_decoder_ring[data[ebyte + 3] - 32];
        if (value >= 16777216) throw std::runtime_error("base85_decode: corrupted input data, decoded block overflow");
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
    for (const char byte : encoded_string) {
        basE91_decode_value(static_cast<unsigned char>(byte));
    }
    size_t outsize = basE91_decode_bound(size);
    std::vector<uint8_t> output(outsize);
    size_t nb_decoded = basE91_decode_internal(&b, encoded_string.data(), size, output.data(), outsize);
    nb_decoded += basE91_decode_end(&b, output.data() + nb_decoded, outsize - nb_decoded);
    output.resize(nb_decoded);
    return RawVector(output.begin(), output.end());
}

// [[Rcpp::init]]
void qx_export_functions(DllInfo* dll) {
    R_RegisterCCallable("qs2", "qs_save", (DL_FUNC)&qs_save);
    R_RegisterCCallable("qs2", "qs_serialize", (DL_FUNC)&qs_serialize);
    R_RegisterCCallable("qs2", "qs_read", (DL_FUNC)&qs_read);
    R_RegisterCCallable("qs2", "qs_deserialize", (DL_FUNC)&qs_deserialize);
    R_RegisterCCallable("qs2", "qd_save", (DL_FUNC)&qd_save);
    R_RegisterCCallable("qs2", "qd_serialize", (DL_FUNC)&qd_serialize);
    R_RegisterCCallable("qs2", "qd_read", (DL_FUNC)&qd_read);
    R_RegisterCCallable("qs2", "qd_deserialize", (DL_FUNC)&qd_deserialize);
    register_qdata_cpp_external_callables();

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
