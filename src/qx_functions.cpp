// low level IO functions/classes
#include "io/filestream_module.h"
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
#include "qx_read_hash.h"

#define DO_QS_SAVE(_BASE_CLASS_, _COMPRESSOR_, _HASHER_) \
    _BASE_CLASS_ <OfStreamWriter, _COMPRESSOR_, _HASHER_, ErrorType::r_error> block_io(myFile, compress_level); \
    R_SerializeInit(&out, block_io); \
    qsSaveImplArgs args = {object, &out}; \
    DO_JMPBUF(); \
    DO_UNWIND_PROTECT(qs_save_impl, decltype(block_io), args)

// [[Rcpp::export(rng = false, invisible = true)]]
SEXP qs_save(SEXP object, const std::string & file, const int compress_level = 3, const bool shuffle = true, const bool store_checksum = true, const int nthreads = 1) {
    UNWIND_PROTECT_BEGIN()

    #if RCPP_PARALLEL_USE_TBB == 0
    if(nthreads > 1) throw_error<ErrorType::r_error>("nthreads > 1 requires TBB");
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
    struct R_outpstream_st out;
    if(nthreads > 1) {
        #if RCPP_PARALLEL_USE_TBB
        tbb::global_control gc(tbb::global_control::parameter::max_allowed_parallelism, nthreads);
        if(shuffle) {
            if(store_checksum) {
                DO_QS_SAVE(BlockCompressWriterMT, ZstdShuffleCompressor, xxHashEnv);
            } else {
                DO_QS_SAVE(BlockCompressWriterMT, ZstdShuffleCompressor, noHashEnv);
            }
        } else {
            if(store_checksum) {
                DO_QS_SAVE(BlockCompressWriterMT, ZstdCompressor, xxHashEnv);
            } else {
                DO_QS_SAVE(BlockCompressWriterMT, ZstdCompressor, noHashEnv);
            }
        }
        #endif
    } else {
        if(shuffle) {
            if(store_checksum) {
                DO_QS_SAVE(BlockCompressWriter, ZstdShuffleCompressor, xxHashEnv);
            } else {
                DO_QS_SAVE(BlockCompressWriter, ZstdShuffleCompressor, noHashEnv);
            }

        } else {
            if(store_checksum) {
                DO_QS_SAVE(BlockCompressWriter, ZstdCompressor, xxHashEnv);
            } else {
                DO_QS_SAVE(BlockCompressWriter, ZstdCompressor, noHashEnv);
            }
        }
    }
    UNWIND_PROTECT_END();
}

// DO_UNWIND_PROTECT macro assigns SEXP output
#define DO_QS_READ(_BASE_CLASS_, _DECOMPRESSOR_) \
    _BASE_CLASS_ <IfStreamReader, _DECOMPRESSOR_, ErrorType::r_error> block_io(myFile); \
    R_UnserializeInit< _BASE_CLASS_ <IfStreamReader, _DECOMPRESSOR_, ErrorType::r_error>>(&in, (R_pstream_data_t)(&block_io)); \
    DO_JMPBUF(); \
    DO_UNWIND_PROTECT(qs_read_impl, decltype(block_io), in);

// [[Rcpp::export(rng = false)]]
SEXP qs_read(const std::string & file, const bool validate_checksum = true, const int nthreads = 1) {
    UNWIND_PROTECT_BEGIN()

    #if RCPP_PARALLEL_USE_TBB == 0
    if(nthreads > 1) throw_error<ErrorType::r_error>("nthreads > 1 requires TBB");
    #endif

    IfStreamReader myFile(R_ExpandFileName(file.c_str()));
    if(! myFile.isValid()) {
        throw_error<ErrorType::r_error>("For file " + file + ": " + FILE_READ_ERR_MSG);
    }

    bool shuffle; uint64_t stored_hash;
    read_qs2_header(myFile, shuffle, stored_hash);
    if(validate_checksum) {
        if(stored_hash == 0) throw_error<ErrorType::r_error>("For file " + file + ": hash not stored");
        uint64_t computed_hash = read_qx_hash(myFile);
        if(computed_hash != stored_hash) {
            throw_error<ErrorType::r_error>("For file " + file + ": hash mismatch");
        }
    }

    struct R_inpstream_st in;
    if(nthreads > 1) {
        #if RCPP_PARALLEL_USE_TBB
        tbb::global_control gc(tbb::global_control::parameter::max_allowed_parallelism, nthreads);
        if(shuffle) {
            DO_QS_READ(BlockCompressReaderMT, ZstdShuffleDecompressor);
        } else {
            DO_QS_READ(BlockCompressReaderMT, ZstdDecompressor);
        }
        #endif
    } else {
        if(shuffle) {
            DO_QS_READ(BlockCompressReader, ZstdShuffleDecompressor);
        } else {
            DO_QS_READ(BlockCompressReader, ZstdDecompressor);
        }
    }
    UNWIND_PROTECT_END();
}

#define DO_QD_SAVE(_BASE_CLASS_, _COMPRESSOR_, _HASHER_) \
    _BASE_CLASS_ <OfStreamWriter, _COMPRESSOR_, _HASHER_, ErrorType::cpp_error> writer(myFile, compress_level); \
    QdataSerializer<_BASE_CLASS_<OfStreamWriter, _COMPRESSOR_, _HASHER_, ErrorType::cpp_error>> serializer(writer, warn_unsupported_types); \
    serializer.write_object(object); \
    uint64_t hash = writer.finish(); \
    write_qx_hash(myFile, hash); \
    return R_NilValue

// [[Rcpp::export(rng = false, invisible = true)]]
SEXP qd_save(SEXP object, const std::string & file, const int compress_level = 3, const bool shuffle = true, const bool store_checksum = true, const bool warn_unsupported_types = true, const int nthreads = 1) {

    #if RCPP_PARALLEL_USE_TBB == 0
    if(nthreads > 1) throw std::runtime_error("nthreads > 1 requires TBB");
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
    if(nthreads > 1) {
        #if RCPP_PARALLEL_USE_TBB
        tbb::global_control gc(tbb::global_control::parameter::max_allowed_parallelism, nthreads);
        if(shuffle) {
            if(store_checksum) {
                DO_QD_SAVE(BlockCompressWriterMT, ZstdShuffleCompressor, xxHashEnv);
            } else {
                DO_QD_SAVE(BlockCompressWriterMT, ZstdShuffleCompressor, noHashEnv);
            }
        } else {
            if(store_checksum) {
                DO_QD_SAVE(BlockCompressWriterMT, ZstdCompressor, xxHashEnv);
            } else {
                DO_QD_SAVE(BlockCompressWriterMT, ZstdCompressor, noHashEnv);
            }
        }
        #endif
    } else {
        if(shuffle) {
            if(store_checksum) {
                DO_QD_SAVE(BlockCompressWriter, ZstdShuffleCompressor, xxHashEnv);
            } else {
                DO_QD_SAVE(BlockCompressWriter, ZstdShuffleCompressor, noHashEnv);
            }
        } else {
            if(store_checksum) {
                DO_QD_SAVE(BlockCompressWriter, ZstdCompressor, xxHashEnv);
            } else {
                DO_QD_SAVE(BlockCompressWriter, ZstdCompressor, noHashEnv);
            }
        }
    }
    return R_NilValue; // unreachable
}

#define DO_QD_READ(_BASE_CLASS_, _DECOMPRESSOR_) \
    _BASE_CLASS_ <IfStreamReader, _DECOMPRESSOR_, ErrorType::cpp_error> reader(myFile); \
    QdataDeserializer<_BASE_CLASS_<IfStreamReader, _DECOMPRESSOR_, ErrorType::cpp_error>> deserializer(reader, use_alt_rep); \
    SEXP output = deserializer.read_object(); \
    reader.finish(); \
    return output

// [[Rcpp::export(rng = false)]]
SEXP qd_read(const std::string & file, const bool use_alt_rep = false, const bool validate_checksum = true, const int nthreads = 1) {

    #if RCPP_PARALLEL_USE_TBB == 0
    if(nthreads > 1) throw std::runtime_error("nthreads > 1 requires TBB");
    #endif

    IfStreamReader myFile(R_ExpandFileName(file.c_str()));
    if(! myFile.isValid()) {
        throw std::runtime_error("For file " + file + ": " + FILE_READ_ERR_MSG);
    }
    bool shuffle;
    uint64_t stored_hash;
    read_qdata_header(myFile, shuffle, stored_hash);

    if(validate_checksum) {
        if(stored_hash == 0) throw_error<ErrorType::r_error>("For file " + file + ": hash not stored");
        uint64_t computed_hash = read_qx_hash(myFile);
        if(computed_hash != stored_hash) {
            throw_error<ErrorType::r_error>("For file " + file + ": hash mismatch");
        }
    }

    if(nthreads > 1) {
        #if RCPP_PARALLEL_USE_TBB
        tbb::global_control gc(tbb::global_control::parameter::max_allowed_parallelism, nthreads);
        if(shuffle) {
            DO_QD_READ(BlockCompressReaderMT, ZstdShuffleDecompressor);
        } else {
            DO_QD_READ(BlockCompressReaderMT, ZstdDecompressor);
        }
        #endif
    } else {
        if(shuffle) {
            DO_QD_READ(BlockCompressReader, ZstdShuffleDecompressor);
        } else {
            DO_QD_READ(BlockCompressReader, ZstdDecompressor);
        }
    }
    return R_NilValue; // unreachable
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