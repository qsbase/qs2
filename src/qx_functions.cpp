#include "io/filestream_module.h"
#include "io/cvector_module.h"
#include "io/zstd_module.h"
#include "io/xxhash_module.h"
#include "io/block_module.h"

#include <RcppParallel.h>
#ifndef RCPP_PARALLEL_USE_TBB
    #define RCPP_PARALLEL_USE_TBB 0
#endif
#if RCPP_PARALLEL_USE_TBB
    #include "io/multithreaded_block_module.h"
#endif


#include "qs_serializer.h"
#include "qs_deserializer.h"
#include "qs_unwind_protect.h"
#include "qd_serializer.h"
#include "qd_deserializer.h"
#include "qx_dump.h"

SEXP qs_save(SEXP object, const std::string & file, const int compress_level, const bool shuffle, const int nthreads);
CVectorOut qs_serialize_impl(SEXP object, const int compress_level, const bool shuffle, const int nthreads);
SEXP qs_serialize(SEXP object, const int compress_level, const bool shuffle, const int nthreads);
char * c_qs_serialize(SEXP object, uint64_t * len, const int compress_level, const bool shuffle, const int nthreads);
SEXP qs_read(const std::string & file, const bool validate_checksum, const int nthreads);
SEXP qs_deserialize_impl(CVectorIn & myFile, const bool validate_checksum, const int nthreads);
SEXP qs_deserialize(SEXP input, const bool validate_checksum, const int nthreads);
SEXP c_qs_deserialize(const char * buffer, const uint64_t len, const bool validate_checksum, const int nthreads);
SEXP qd_save(SEXP object, const std::string & file, const int compress_level, const bool shuffle, const bool warn_unsupported_types, const int nthreads);
CVectorOut qd_serialize_impl(SEXP object, const int compress_level, const bool shuffle, const bool warn_unsupported_types, const int nthreads);
SEXP qd_serialize(SEXP object, const int compress_level, const bool shuffle, const bool warn_unsupported_types, const int nthreads);
char * c_qd_serialize(SEXP object, uint64_t * len, const int compress_level, const bool shuffle, const bool warn_unsupported_types, const int nthreads);
SEXP qd_read(const std::string & file, const bool use_alt_rep, const bool validate_checksum, const int nthreads);
SEXP qd_deserialize_impl(CVectorIn & myFile, const bool use_alt_rep, const bool validate_checksum, const int nthreads);
SEXP qd_deserialize(SEXP input, const bool use_alt_rep, const bool validate_checksum, const int nthreads);
SEXP c_qd_deserialize(const char * buffer, const uint64_t len, const bool use_alt_rep, const bool validate_checksum, const int nthreads);


#define NTHREADS_ERROR_MSG "nthreads > 1 requires TBB, see the readme and vignette for details"

#define DO_QS_SAVE(_STREAM_WRITER_, _BASE_CLASS_, _COMPRESSOR_, _HASHER_) \
    _BASE_CLASS_ <_STREAM_WRITER_, _COMPRESSOR_, _HASHER_, ErrorType::r_error, false> block_io(myFile, compress_level); \
    R_SerializeInit(&out, block_io); \
    qsSaveImplArgs args = {object, hash, &out}; \
    DO_JMPBUF_QS_SAVE(); \
    DO_UNWIND_PROTECT(qs_save_impl, decltype(block_io), args);

// [[Rcpp::export(rng = false, invisible = true)]]
SEXP qs_save(SEXP object, const std::string & file, const int compress_level = 3, const bool shuffle = true, const int nthreads = 1) {
    #if RCPP_PARALLEL_USE_TBB == 0
    if(nthreads > 1) throw_error<ErrorType::r_error>(NTHREADS_ERROR_MSG);
    #endif

    if(compress_level > ZSTD_maxCLevel() || compress_level < ZSTD_minCLevel()) {
        throw_error<ErrorType::r_error>("compress_level must be an integer between " + 
            std::to_string(ZSTD_minCLevel()) + 
            " and " + std::to_string(ZSTD_maxCLevel()));
    }

    OfStreamWriter myFile(R_ExpandFileName(file.c_str()));
    if(! myFile.isValid()) {
        throw_error<ErrorType::r_error>("For file " + file + ": " + FILE_SAVE_ERR_MSG);
    }
    write_qs2_header(myFile, shuffle);

    UNWIND_PROTECT_BEGIN()
    struct R_outpstream_st out;
    SEXP output = R_NilValue;
    uint64_t hash = 0;
    if(nthreads > 1) {
        #if RCPP_PARALLEL_USE_TBB
        tbb::global_control gc(tbb::global_control::parameter::max_allowed_parallelism, nthreads);
        if(shuffle) {
            DO_QS_SAVE(OfStreamWriter, BlockCompressWriterMT, ZstdShuffleCompressor, xxHashEnv);
        } else {
            DO_QS_SAVE(OfStreamWriter, BlockCompressWriterMT, ZstdCompressor, xxHashEnv);
        }
        #endif
    } else {
        if(shuffle) {
            DO_QS_SAVE(OfStreamWriter, BlockCompressWriter, ZstdShuffleCompressor, xxHashEnv);
        } else {
            DO_QS_SAVE(OfStreamWriter, BlockCompressWriter, ZstdCompressor, xxHashEnv);
        }
    }
    write_qx_hash(myFile, hash);
    return output;
    UNWIND_PROTECT_END();
    return R_NilValue;
}

CVectorOut qs_serialize_impl(SEXP object, const int compress_level = 3, const bool shuffle = true, const int nthreads = 1) {
    #if RCPP_PARALLEL_USE_TBB == 0
    if(nthreads > 1) throw_error<ErrorType::r_error>(NTHREADS_ERROR_MSG);
    #endif

    if(compress_level > ZSTD_maxCLevel() || compress_level < ZSTD_minCLevel()) {
        throw_error<ErrorType::r_error>("compress_level must be an integer between " + 
            std::to_string(ZSTD_minCLevel()) + 
            " and " + std::to_string(ZSTD_maxCLevel()));
    }

    CVectorOut myFile;
    write_qs2_header(myFile, shuffle);

    UNWIND_PROTECT_BEGIN()
    struct R_outpstream_st out;
    SEXP output = R_NilValue;
    uint64_t hash = 0;
    if(nthreads > 1) {
        #if RCPP_PARALLEL_USE_TBB
        tbb::global_control gc(tbb::global_control::parameter::max_allowed_parallelism, nthreads);
        if(shuffle) {
            DO_QS_SAVE(CVectorOut, BlockCompressWriterMT, ZstdShuffleCompressor, xxHashEnv);
        } else {
            DO_QS_SAVE(CVectorOut, BlockCompressWriterMT, ZstdCompressor, xxHashEnv);
        }
        #endif
    } else {
        if(shuffle) {
            DO_QS_SAVE(CVectorOut, BlockCompressWriter, ZstdShuffleCompressor, xxHashEnv);
        } else {
            DO_QS_SAVE(CVectorOut, BlockCompressWriter, ZstdCompressor, xxHashEnv);
        }
    }
    uint64_t len = myFile.tellp();
    write_qx_hash(myFile, hash); // must be done after getting length (position) from tellp
    myFile.seekp(len);
    return myFile;
    UNWIND_PROTECT_END();
    return CVectorOut{};
}

// [[Rcpp::export(rng = false, invisible = true)]]
SEXP qs_serialize(SEXP object, const int compress_level = 3, const bool shuffle = true, const int nthreads = 1) {
    CVectorOut result = qs_serialize_impl(object, compress_level, shuffle, nthreads);
    SEXP output = Rf_allocVector(RAWSXP, result.tellp());
    std::memcpy(RAW(output), result.data(), result.tellp());
    return output;
}

char * c_qs_serialize(SEXP object, uint64_t * len, const int compress_level = 3, const bool shuffle = true, const int nthreads = 1) {
    CVectorOut result = qs_serialize_impl(object, compress_level, shuffle, nthreads);
    *len = result.tellp();
    return result.release(); // give up ownership of buffer
}

// DO_UNWIND_PROTECT macro assigns SEXP output
#define DO_QS_READ(_STREAM_READER_, _BASE_CLASS_, _DECOMPRESSOR_) \
    _BASE_CLASS_ <_STREAM_READER_, _DECOMPRESSOR_, ErrorType::r_error> block_io(myFile); \
    R_UnserializeInit< _BASE_CLASS_ <_STREAM_READER_, _DECOMPRESSOR_, ErrorType::r_error>>(&in, (R_pstream_data_t)(&block_io)); \
    DO_JMPBUF_QS_READ(); \
    DO_UNWIND_PROTECT(qs_read_impl, decltype(block_io), in);

// [[Rcpp::export(rng = false)]]
SEXP qs_read(const std::string & file, const bool validate_checksum = false, const int nthreads = 1) {
    #if RCPP_PARALLEL_USE_TBB == 0
    if(nthreads > 1) throw_error<ErrorType::r_error>(NTHREADS_ERROR_MSG);
    #endif
    
    IfStreamReader myFile(R_ExpandFileName(file.c_str()));
    if(! myFile.isValid()) {
        throw_error<ErrorType::r_error>("For file " + file + ": " + FILE_READ_ERR_MSG);
    }

    bool shuffle; uint64_t stored_hash;
    read_qs2_header(myFile, shuffle, stored_hash);
    if(stored_hash == 0) {
        throw_error<ErrorType::r_error>("For file " + file + ": hash not stored, save file may be incomplete");
    }

    if(validate_checksum) {
        uint64_t computed_hash = read_qx_hash(myFile);
        if(computed_hash != stored_hash) {
            throw_error<ErrorType::r_error>("For file " + file + ": hash mismatch");
        }
    }

    UNWIND_PROTECT_BEGIN()
    struct R_inpstream_st in;
    SEXP output = R_NilValue;
    if(nthreads > 1) {
        #if RCPP_PARALLEL_USE_TBB
        tbb::global_control gc(tbb::global_control::parameter::max_allowed_parallelism, nthreads);
        if(shuffle) {
            DO_QS_READ(IfStreamReader, BlockCompressReaderMT, ZstdShuffleDecompressor);
        } else {
            DO_QS_READ(IfStreamReader, BlockCompressReaderMT, ZstdDecompressor);
        }
        #endif
    } else {
        if(shuffle) {
            DO_QS_READ(IfStreamReader, BlockCompressReader, ZstdShuffleDecompressor);
        } else {
            DO_QS_READ(IfStreamReader, BlockCompressReader, ZstdDecompressor);
        }
    }
    return output;
    UNWIND_PROTECT_END();
    return R_NilValue;
}

SEXP qs_deserialize_impl(CVectorIn & myFile, const bool validate_checksum = false, const int nthreads = 1) {
    #if RCPP_PARALLEL_USE_TBB == 0
    if(nthreads > 1) throw_error<ErrorType::r_error>(NTHREADS_ERROR_MSG);
    #endif

    bool shuffle; uint64_t stored_hash;
    read_qs2_header(myFile, shuffle, stored_hash);
    if(stored_hash == 0) {
        throw_error<ErrorType::r_error>("Hash not stored, save may be incomplete");
    }

    if(validate_checksum) {
        uint64_t computed_hash = read_qx_hash(myFile);
        if(computed_hash != stored_hash) {
            throw_error<ErrorType::r_error>("Hash mismatch");
        }
    }

    UNWIND_PROTECT_BEGIN()
    struct R_inpstream_st in;
    SEXP output = R_NilValue;
    if(nthreads > 1) {
        #if RCPP_PARALLEL_USE_TBB
        tbb::global_control gc(tbb::global_control::parameter::max_allowed_parallelism, nthreads);
        if(shuffle) {
            DO_QS_READ(CVectorIn, BlockCompressReaderMT, ZstdShuffleDecompressor);
        } else {
            DO_QS_READ(CVectorIn, BlockCompressReaderMT, ZstdDecompressor);
        }
        #endif
    } else {
        if(shuffle) {
            DO_QS_READ(CVectorIn, BlockCompressReader, ZstdShuffleDecompressor);
        } else {
            DO_QS_READ(CVectorIn, BlockCompressReader, ZstdDecompressor);
        }
    }
    return output;
    UNWIND_PROTECT_END();
    return R_NilValue;
}

// [[Rcpp::export(rng = false)]]
SEXP qs_deserialize(SEXP input, const bool validate_checksum = false, const int nthreads = 1) {
    if(TYPEOF(input) != RAWSXP) {
        throw_error<ErrorType::r_error>("input must be a raw vector");
    }
    CVectorIn myFile(reinterpret_cast<char*>(RAW(input)), Rf_xlength(input));
    return qs_deserialize_impl(myFile, validate_checksum, nthreads);
}

SEXP c_qs_deserialize(const char * buffer, const uint64_t len, const bool validate_checksum = false, const int nthreads = 1) {
    CVectorIn myFile(buffer, len);
    return qs_deserialize_impl(myFile, validate_checksum, nthreads);
}

#define DO_QD_SAVE(_STREAM_WRITER_, _BASE_CLASS_, _COMPRESSOR_, _HASHER_) \
    _BASE_CLASS_ <_STREAM_WRITER_, _COMPRESSOR_, _HASHER_, ErrorType::cpp_error, true> writer(myFile, compress_level); \
    QdataSerializer<_BASE_CLASS_<_STREAM_WRITER_, _COMPRESSOR_, _HASHER_, ErrorType::cpp_error, true>> serializer(writer, warn_unsupported_types); \
    serializer.write_object(object); \
    serializer.write_object_data(); \
    hash = writer.finish();

// [[Rcpp::export(rng = false, invisible = true)]]
SEXP qd_save(SEXP object, const std::string & file, const int compress_level = 3, const bool shuffle = true, const bool warn_unsupported_types = true, const int nthreads = 1) {
    #if RCPP_PARALLEL_USE_TBB == 0
    if(nthreads > 1) throw std::runtime_error(NTHREADS_ERROR_MSG);
    #endif

    if(compress_level > ZSTD_maxCLevel() || compress_level < ZSTD_minCLevel()) {
    throw_error<ErrorType::r_error>("compress_level must be an integer between " + 
        std::to_string(ZSTD_minCLevel()) + 
        " and " + std::to_string(ZSTD_maxCLevel()));
    }

    OfStreamWriter myFile(R_ExpandFileName(file.c_str()));
    if(! myFile.isValid()) {
        throw std::runtime_error("For file " + file + ": " + FILE_SAVE_ERR_MSG);
    }
    write_qdata_header(myFile, shuffle);
    uint64_t hash = 0;
    if(nthreads > 1) {
        #if RCPP_PARALLEL_USE_TBB
        tbb::global_control gc(tbb::global_control::parameter::max_allowed_parallelism, nthreads);
        if(shuffle) {
            DO_QD_SAVE(OfStreamWriter, BlockCompressWriterMT, ZstdShuffleCompressor, xxHashEnv);
        } else {
            DO_QD_SAVE(OfStreamWriter, BlockCompressWriterMT, ZstdCompressor, xxHashEnv);
        }
        #endif
    } else {
        if(shuffle) {
            DO_QD_SAVE(OfStreamWriter, BlockCompressWriter, ZstdShuffleCompressor, xxHashEnv);
        } else {
            DO_QD_SAVE(OfStreamWriter, BlockCompressWriter, ZstdCompressor, xxHashEnv);
        }
    }
    write_qx_hash(myFile, hash);
    return R_NilValue;
}

CVectorOut qd_serialize_impl(SEXP object, const int compress_level = 3, const bool shuffle = true, const bool warn_unsupported_types = true, const int nthreads = 1) {
    #if RCPP_PARALLEL_USE_TBB == 0
    if(nthreads > 1) throw std::runtime_error(NTHREADS_ERROR_MSG);
    #endif

    if(compress_level > ZSTD_maxCLevel() || compress_level < ZSTD_minCLevel()) {
    throw_error<ErrorType::r_error>("compress_level must be an integer between " + 
        std::to_string(ZSTD_minCLevel()) + 
        " and " + std::to_string(ZSTD_maxCLevel()));
    }

    CVectorOut myFile;
    write_qdata_header(myFile, shuffle);
    uint64_t hash = 0;
    if(nthreads > 1) {
        #if RCPP_PARALLEL_USE_TBB
        tbb::global_control gc(tbb::global_control::parameter::max_allowed_parallelism, nthreads);
        if(shuffle) {
            DO_QD_SAVE(CVectorOut, BlockCompressWriterMT, ZstdShuffleCompressor, xxHashEnv);
        } else {
            DO_QD_SAVE(CVectorOut, BlockCompressWriterMT, ZstdCompressor, xxHashEnv);
        }
        #endif
    } else {
        if(shuffle) {
            DO_QD_SAVE(CVectorOut, BlockCompressWriter, ZstdShuffleCompressor, xxHashEnv);
        } else {
            DO_QD_SAVE(CVectorOut, BlockCompressWriter, ZstdCompressor, xxHashEnv);
        }
    }
    uint64_t len = myFile.tellp();
    write_qx_hash(myFile, hash); // must be done after getting length (position) from tellp
    myFile.seekp(len);
    return myFile;
}

// [[Rcpp::export(rng = false)]]
SEXP qd_serialize(SEXP object, const int compress_level = 3, const bool shuffle = true, const bool warn_unsupported_types = true, const int nthreads = 1) {
    CVectorOut result = qd_serialize_impl(object, compress_level, shuffle, warn_unsupported_types, nthreads);
    SEXP output = Rf_allocVector(RAWSXP, result.tellp());
    std::memcpy(RAW(output), result.data(), result.tellp());
    return output;
}

char * c_qd_serialize(SEXP object, uint64_t * len, const int compress_level = 3, const bool shuffle = true, const bool warn_unsupported_types = true, const int nthreads = 1) {
    CVectorOut result = qd_serialize_impl(object, compress_level, shuffle, warn_unsupported_types, nthreads);
    *len = result.tellp();
    return result.release(); // give up ownership of buffer
}

#define DO_QD_READ(_STREAM_READER_, _BASE_CLASS_, _DECOMPRESSOR_) \
    _BASE_CLASS_ <_STREAM_READER_, _DECOMPRESSOR_, ErrorType::cpp_error> reader(myFile); \
    QdataDeserializer<_BASE_CLASS_<_STREAM_READER_, _DECOMPRESSOR_, ErrorType::cpp_error>> deserializer(reader, use_alt_rep); \
    output = PROTECT(deserializer.read_object()); \
    deserializer.read_object_data(); \
    reader.finish(); \
    UNPROTECT(1);


// [[Rcpp::export(rng = false)]]
SEXP qd_read(const std::string & file, const bool use_alt_rep = false, const bool validate_checksum = false, const int nthreads = 1) {
    #if RCPP_PARALLEL_USE_TBB == 0
    if(nthreads > 1) throw std::runtime_error(NTHREADS_ERROR_MSG);
    #endif

    IfStreamReader myFile(R_ExpandFileName(file.c_str()));
    if(! myFile.isValid()) {
        throw std::runtime_error("For file " + file + ": " + FILE_READ_ERR_MSG);
    }
    bool shuffle;
    uint64_t stored_hash;
    read_qdata_header(myFile, shuffle, stored_hash);
    if(stored_hash == 0) {
        throw std::runtime_error("For file " + file + ": hash not stored, save file may be incomplete");
    }

    if(validate_checksum) {
        uint64_t computed_hash = read_qx_hash(myFile);
        if(computed_hash != stored_hash) {
            throw_error<ErrorType::r_error>("For file " + file + ": hash mismatch");
        }
    }

    SEXP output = R_NilValue;
    if(nthreads > 1) {
        #if RCPP_PARALLEL_USE_TBB
        tbb::global_control gc(tbb::global_control::parameter::max_allowed_parallelism, nthreads);
        if(shuffle) {
            DO_QD_READ(IfStreamReader, BlockCompressReaderMT, ZstdShuffleDecompressor);
        } else {
            DO_QD_READ(IfStreamReader, BlockCompressReaderMT, ZstdDecompressor);
        }
        #endif
    } else {
        if(shuffle) {
            DO_QD_READ(IfStreamReader, BlockCompressReader, ZstdShuffleDecompressor);
        } else {
            DO_QD_READ(IfStreamReader, BlockCompressReader, ZstdDecompressor);
        }
    }
    return output;
}

SEXP qd_deserialize_impl(CVectorIn & myFile, const bool use_alt_rep = false, const bool validate_checksum = false, const int nthreads = 1) {
    #if RCPP_PARALLEL_USE_TBB == 0
    if(nthreads > 1) throw std::runtime_error(NTHREADS_ERROR_MSG);
    #endif

    bool shuffle;
    uint64_t stored_hash;
    read_qdata_header(myFile, shuffle, stored_hash);
    if(stored_hash == 0) {
        throw std::runtime_error("Hash not stored, save file may be incomplete");
    }

    if(validate_checksum) {
        uint64_t computed_hash = read_qx_hash(myFile);
        if(computed_hash != stored_hash) {
            throw_error<ErrorType::r_error>("Hash mismatch");
        }
    }

    SEXP output = R_NilValue;
    if(nthreads > 1) {
        #if RCPP_PARALLEL_USE_TBB
        tbb::global_control gc(tbb::global_control::parameter::max_allowed_parallelism, nthreads);
        if(shuffle) {
            DO_QD_READ(CVectorIn, BlockCompressReaderMT, ZstdShuffleDecompressor);
        } else {
            DO_QD_READ(CVectorIn, BlockCompressReaderMT, ZstdDecompressor);
        }
        #endif
    } else {
        if(shuffle) {
            DO_QD_READ(CVectorIn, BlockCompressReader, ZstdShuffleDecompressor);
        } else {
            DO_QD_READ(CVectorIn, BlockCompressReader, ZstdDecompressor);
        }
    }
    return output;
}

// [[Rcpp::export(rng = false)]]
SEXP qd_deserialize(SEXP input, const bool use_alt_rep = false, const bool validate_checksum = false, const int nthreads = 1) {
    if(TYPEOF(input) != RAWSXP) {
        throw_error<ErrorType::r_error>("input must be a raw vector");
    }
    CVectorIn myFile(reinterpret_cast<char*>(RAW(input)), Rf_xlength(input));
    return qd_deserialize_impl(myFile, use_alt_rep, validate_checksum, nthreads);
}

SEXP c_qd_deserialize(const char * buffer, const uint64_t len, const bool use_alt_rep = false, const bool validate_checksum = false, const int nthreads = 1) {
    CVectorIn myFile(buffer, len);
    return qd_deserialize_impl(myFile, use_alt_rep, validate_checksum, nthreads);
}


// [[Rcpp::export(rng = false)]]
List qx_dump(const std::string & file) {
    IfStreamReader myFile(R_ExpandFileName(file.c_str()));
    if(! myFile.isValid()) {
        throw std::runtime_error("For file " + file + ": " + FILE_READ_ERR_MSG);
    }
    qxHeaderInfo header_info = read_qx_header(myFile);

    std::tuple<std::vector<std::vector<unsigned char>>, std::vector<std::vector<unsigned char>>, std::string> output;
    if(header_info.shuffle) {
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
        _["computed_hash"] = std::get<2>(output),
        _["zblocks"] = std::get<0>(output),
        _["blocks"] = std::get<1>(output));
}

// [[Rcpp::export(rng = false)]]
std::string check_SIMD() {
#if defined (__AVX2__)
  return "AVX2";
#elif defined (__SSE2__)
  return "SSE2";
#else
  return "no SIMD";
#endif
}

// [[Rcpp::export(rng = false)]]
bool check_TBB() {
    return RCPP_PARALLEL_USE_TBB;
}

// [[Rcpp::export(rng = false)]]
int check_internal_blocksize() {
    return MAX_BLOCKSIZE;
}

// [[Rcpp::export(rng = false)]]
std::vector<unsigned char> zstd_compress_raw(SEXP const data, const int compress_level) {
    if(compress_level > ZSTD_maxCLevel() || compress_level < ZSTD_minCLevel()) {
        throw_error<ErrorType::r_error>("compress_level must be an integer between " + 
            std::to_string(ZSTD_minCLevel()) + 
            " and " + std::to_string(ZSTD_maxCLevel()));
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

// [[Rcpp::export(rng = false)]]
RawVector zstd_decompress_raw(SEXP const data) {
  uint64_t zsize = Rf_xlength(data);
  char* xdata = reinterpret_cast<char*>(RAW(data));
  uint64_t retsize = ZSTD_getFrameContentSize(xdata, zsize);
  RawVector ret(retsize);
  char* retdata = reinterpret_cast<char*>(RAW(ret));
  ZSTD_decompress(retdata, retsize, xdata, zsize);
  return ret;
}

// [[Rcpp::export(rng = false)]]
int zstd_compress_bound(const int size) {
  return ZSTD_compressBound(size);
}

// [[Rcpp::export(rng = false)]]
std::vector<unsigned char> blosc_shuffle_raw(SEXP const data, int bytesofsize) {
  if(bytesofsize != 4 && bytesofsize != 8) throw std::runtime_error("bytesofsize must be 4 or 8");
  uint64_t blocksize = Rf_xlength(data);
  uint8_t* xdata = reinterpret_cast<uint8_t*>(RAW(data));
  std::vector<uint8_t> xshuf(blocksize);
  blosc_shuffle(xdata, xshuf.data(), blocksize, bytesofsize);
  uint64_t remainder = blocksize % bytesofsize;
  uint64_t vectorizablebytes = blocksize - remainder;
  std::memcpy(xshuf.data() + vectorizablebytes, xdata + vectorizablebytes, remainder);
  return xshuf;
}

// [[Rcpp::export(rng = false)]]
std::vector<unsigned char> blosc_unshuffle_raw(SEXP const data, int bytesofsize) {
  if(bytesofsize != 4 && bytesofsize != 8) throw std::runtime_error("bytesofsize must be 4 or 8");
  uint64_t blocksize = Rf_xlength(data);
  uint8_t* xdata = reinterpret_cast<uint8_t*>(RAW(data));
  std::vector<uint8_t> xshuf(blocksize);
  blosc_unshuffle(xdata, xshuf.data(), blocksize, bytesofsize);
  uint64_t remainder = blocksize % bytesofsize;
  uint64_t vectorizablebytes = blocksize - remainder;
  std::memcpy(xshuf.data() + vectorizablebytes, xdata + vectorizablebytes, remainder);
  return xshuf;
}

// [[Rcpp::export(rng = false)]]
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

// [[Rcpp::export(rng = false)]]
int internal_is_utf8_locale(const int size) {
    return IS_UTF8_LOCALE;
}

// [[Rcpp::init]]
void qx_export_functions(DllInfo* dll) {
  R_RegisterCCallable("qs2", "c_qs_serialize", (DL_FUNC) &c_qs_serialize);
  R_RegisterCCallable("qs2", "c_qs_deserialize", (DL_FUNC) &c_qs_deserialize);
  R_RegisterCCallable("qs2", "c_qd_serialize", (DL_FUNC) &c_qd_serialize);
  R_RegisterCCallable("qs2", "c_qd_deserialize", (DL_FUNC) &c_qd_deserialize);
}
