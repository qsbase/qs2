#ifndef _QS2_ZSTD_FILE_FUNCTIONS_H
#define _QS2_ZSTD_FILE_FUNCTIONS_H

#include <algorithm>
#include <array>
#include <cmath>
#include <cerrno>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <memory>
#include <Rcpp.h>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include <zstd.h>

#include "io/error_policy.h"

using namespace Rcpp;

static constexpr std::array<uint8_t, 4> ZSTD_MAGIC{{0x28, 0xb5, 0x2f, 0xfd}};
static constexpr double MAX_SAFE_R_DOUBLE_INTEGER = 9007199254740991.0; // 2^53 - 1

template <size_t N>
inline void ensure_magic_prefix(const std::string& path,
                                const std::array<uint8_t, N>& magic,
                                const char* format_name) {
    std::ifstream in(path, std::ios::binary);
    if(!in) {
        throw std::runtime_error(std::string("Failed to open ") + format_name + " file: " + path);
    }
    std::array<char, N> buf{};
    in.read(buf.data(), static_cast<std::streamsize>(N));
    if(in.gcount() < static_cast<std::streamsize>(N)) {
        throw std::runtime_error(std::string("File too short to be valid ") + format_name + ": " + path);
    }
    for(size_t i = 0; i < N; ++i) {
        if(static_cast<uint8_t>(buf[i]) != magic[i]) {
            throw std::runtime_error(std::string("File does not appear to be valid ") + format_name + ": " + path);
        }
    }
}

struct IWriter {
    virtual ~IWriter() = default;
    virtual void write(const char* data, size_t len) = 0;
    virtual void writeLine(const std::string& line) = 0;
    virtual void flush() = 0;
    virtual bool has_pending() const { return false; }
    virtual void close() {}
};

struct IReader {
    virtual ~IReader() = default;

    virtual size_t read(char* dst, size_t cnt) = 0;
    virtual bool readLine(std::string& out) = 0;
};

struct PartialOutputGuard {
    std::string path;
    bool keep{false};

    explicit PartialOutputGuard(std::string output_path) : path(std::move(output_path)) {}

    void release() {
        keep = true;
    }

    ~PartialOutputGuard() {
        if(!keep) {
            std::remove(path.c_str());
        }
    }
};

inline bool parse_max_output_bytes(SEXP max_output_bytes, uint64_t& parsed_max_output_bytes) {
    if(Rf_isNull(max_output_bytes)) {
        return false;
    }
    if(Rf_xlength(max_output_bytes) != 1) {
        throw std::runtime_error("max_output_bytes must be NULL or a single non-negative whole number");
    }
    const double value = Rf_asReal(max_output_bytes);
    if(!R_finite(value) || value < 0 || std::floor(value) != value) {
        throw std::runtime_error("max_output_bytes must be NULL or a single non-negative whole number");
    }
    if(value > MAX_SAFE_R_DOUBLE_INTEGER) {
        throw std::runtime_error("max_output_bytes exceeds the largest exactly representable R numeric integer (2^53 - 1)");
    }
    parsed_max_output_bytes = static_cast<uint64_t>(value);
    return true;
}

struct ZstdWriter : IWriter {
    FILE* _f;
    ZSTD_CCtx* _cctx;
    std::vector<char> _inBuf;
    std::vector<char> _outBuf;
    size_t _bufSize;
    size_t _inPos{0};

    explicit ZstdWriter(const std::string& path,
                        int compress_level,
                        size_t bufSize = 64 * 1024) :
    _f(std::fopen(path.c_str(), "wb")),
    _cctx(ZSTD_createCCtx()),
    _inBuf(bufSize),
    _outBuf(ZSTD_CStreamOutSize()),
    _bufSize(bufSize) {
        if(!_f) {
            throw std::runtime_error("Failed to open " + path);
        }
        if(!_cctx) {
            std::fclose(_f);
            throw std::runtime_error("ZSTD_createCCtx failed");
        }
        size_t init = ZSTD_initCStream(_cctx, compress_level);
        if(ZSTD_isError(init)) {
            ZSTD_freeCCtx(_cctx);
            std::fclose(_f);
            throw std::runtime_error(std::string("ZSTD_initCStream failed: ") + ZSTD_getErrorName(init));
        }
    }

    ~ZstdWriter() override {
        try {
            close();
        } catch(...) {}
    }

    void write(const char* data, size_t len) override {
        size_t done = 0;
        while(done < len) {
            size_t space = _bufSize - _inPos;
            size_t chunk = std::min(space, len - done);
            std::memcpy(_inBuf.data() + _inPos, data + done, chunk);
            _inPos += chunk;
            done += chunk;
            if(_inPos == _bufSize) {
                compressChunk();
            }
        }
    }

    void writeLine(const std::string& line) override {
        write(line.data(), line.size());
        static const char nl = '\n';
        write(&nl, 1);
    }

    void flush() override {
        if(_inPos > 0) {
            compressChunk();
        }

        ZSTD_outBuffer out = {_outBuf.data(), _outBuf.size(), 0};
        size_t ret;
        do {
            ret = ZSTD_endStream(_cctx, &out);
            if(ZSTD_isError(ret)) {
                throw std::runtime_error(std::string("ZSTD_endStream failed: ") + ZSTD_getErrorName(ret));
            }
            if(out.pos > 0) {
                size_t written = std::fwrite(_outBuf.data(), 1, out.pos, _f);
                if(written != out.pos) {
                    throw std::runtime_error("fwrite failed");
                }
                out.pos = 0;
            }
        } while(ret > 0);

        std::fflush(_f);
    }

    void close() override {
        if(!_cctx && !_f) {
            return;
        }
        flush();
        if(_cctx) {
            ZSTD_freeCCtx(_cctx);
            _cctx = nullptr;
        }
        if(_f) {
            std::fclose(_f);
            _f = nullptr;
        }
    }

private:
    void compressChunk() {
        ZSTD_inBuffer in = {_inBuf.data(), _inPos, 0};
        ZSTD_outBuffer out = {_outBuf.data(), _outBuf.size(), 0};

        while(in.pos < in.size) {
            size_t ret = ZSTD_compressStream(_cctx, &out, &in);
            if(ZSTD_isError(ret)) {
                throw std::runtime_error(std::string("ZSTD_compressStream failed: ") + ZSTD_getErrorName(ret));
            }
            if(out.pos > 0) {
                size_t written = std::fwrite(_outBuf.data(), 1, out.pos, _f);
                if(written != out.pos) {
                    throw std::runtime_error("fwrite failed");
                }
                out.pos = 0;
            }
        }
        _inPos = 0;
    }
};

struct ZstdReader : IReader {
    FILE* _f;
    ZSTD_DCtx* _dctx;
    std::vector<char> _inBuf;
    std::vector<char> _outBuf;
    size_t _inPos;
    size_t _inSize;
    size_t _outPos;
    size_t _outSize;
    size_t _pendingFrameBytes;

    explicit ZstdReader(const std::string& path, size_t bufSize) :
    _f(nullptr),
    _dctx(nullptr),
    _inBuf(bufSize),
    _outBuf(bufSize),
    _inPos(0),
    _inSize(0),
    _outPos(0),
    _outSize(0),
    _pendingFrameBytes(0) {
        ensure_magic_prefix(path, ZSTD_MAGIC, "zstd");
        _f = std::fopen(path.c_str(), "rb");
        if(!_f) {
            throw std::runtime_error("zstd open failed: " + path);
        }

        _dctx = ZSTD_createDCtx();
        if(!_dctx) {
            std::fclose(_f);
            throw std::runtime_error("ZSTD_createDCtx failed");
        }
        size_t init = ZSTD_initDStream(_dctx);
        if(ZSTD_isError(init)) {
            ZSTD_freeDCtx(_dctx);
            std::fclose(_f);
            throw std::runtime_error("ZSTD_initDStream failed: " + std::string(ZSTD_getErrorName(init)));
        }
    }

    ~ZstdReader() override {
        if(_dctx) {
            ZSTD_freeDCtx(_dctx);
        }
        if(_f) {
            std::fclose(_f);
        }
    }

    size_t read(char* dst, size_t cnt) override {
        size_t tot = 0;
        while(tot < cnt) {
            if(_outPos == _outSize) {
                if(_inPos == _inSize) {
                    _inSize = std::fread(_inBuf.data(), 1, _inBuf.size(), _f);
                    _inPos = 0;
                    if(_inSize == 0) {
                        break;
                    }
                }
                ZSTD_inBuffer in = {_inBuf.data() + _inPos, _inSize - _inPos, 0};
                ZSTD_outBuffer out = {_outBuf.data(), _outBuf.size(), 0};
                size_t ret = ZSTD_decompressStream(_dctx, &out, &in);
                if(ZSTD_isError(ret)) {
                    throw std::runtime_error("ZSTD_decompressStream failed: " + std::string(ZSTD_getErrorName(ret)));
                }
                _pendingFrameBytes = ret;
                _inPos += in.pos;
                _outSize = out.pos;
                _outPos = 0;
                if(_outSize == 0) {
                    continue;
                }
            }
            size_t chunk = std::min(cnt - tot, _outSize - _outPos);
            std::memcpy(dst + tot, _outBuf.data() + _outPos, chunk);
            tot += chunk;
            _outPos += chunk;
        }
        if(tot < cnt && _inSize == 0 && _outSize == 0 && _pendingFrameBytes > 0) {
            throw std::runtime_error("zstd input truncated before end of frame");
        }
        return tot;
    }

    bool readLine(std::string& out) override {
        out.clear();
        char c;
        while(read(&c, 1) == 1) {
            if(c == '\n') {
                return true;
            }
            out.push_back(c);
        }
        return !out.empty();
    }
};

// [[Rcpp::export(rng = false, invisible = true, signature = {input_file, output_file, compress_level = qopt("compress_level")})]]
SEXP zstd_compress_file(const std::string& input_file, const std::string& output_file, const int compress_level) {
    if (compress_level > ZSTD_maxCLevel() || compress_level < ZSTD_minCLevel()) {
        throw_error<StdErrorPolicy>("compress_level out of range for zstd");
    }

    const std::string input_path = R_ExpandFileName(input_file.c_str());
    const std::string output_path = R_ExpandFileName(output_file.c_str());

    std::ifstream in(input_path, std::ios::binary);
    if (!in) {
        throw_error<StdErrorPolicy>(std::string("Failed to open input file: ") + input_file);
    }

    ZstdWriter writer(output_path, compress_level);
    std::vector<char> buffer(1 << 20);
    while (in) {
        in.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
        std::streamsize got = in.gcount();
        if (got > 0) {
            writer.write(buffer.data(), static_cast<size_t>(got));
        }
    }
    if (!in.eof()) {
        throw_error<StdErrorPolicy>(std::string("Error while reading input file: ") + input_file);
    }
    writer.close();
    return R_NilValue;
}

// [[Rcpp::export(rng = false, invisible = true, signature = {input_file, output_file, max_output_bytes = NULL})]]
SEXP zstd_decompress_file(const std::string& input_file, const std::string& output_file, SEXP max_output_bytes) {
    const std::string input_path = R_ExpandFileName(input_file.c_str());
    const std::string output_path = R_ExpandFileName(output_file.c_str());
    uint64_t parsed_max_output_bytes = 0;
    const bool has_max_output_bytes = parse_max_output_bytes(max_output_bytes, parsed_max_output_bytes);

    ZstdReader reader(input_path, 1 << 20);
    std::ofstream out(output_path, std::ios::binary);
    if (!out) {
        throw_error<StdErrorPolicy>(std::string("Failed to open output file for writing: ") + output_file);
    }
    PartialOutputGuard output_guard(output_path);

    try {
        std::vector<char> buffer(1 << 20);
        uint64_t decoded_bytes = 0;
        size_t got = reader.read(buffer.data(), buffer.size());
        while (got > 0) {
            if (has_max_output_bytes && static_cast<uint64_t>(got) > (parsed_max_output_bytes - decoded_bytes)) {
                throw_error<StdErrorPolicy>("zstd_decompress_file failed: decompressed output exceeds max_output_bytes");
            }
            decoded_bytes += static_cast<uint64_t>(got);
            out.write(buffer.data(), static_cast<std::streamsize>(got));
            if (!out) {
                throw_error<StdErrorPolicy>(std::string("Error while writing output file: ") + output_file);
            }
            got = reader.read(buffer.data(), buffer.size());
        }
        out.close();
        if (!out) {
            throw_error<StdErrorPolicy>(std::string("Error while closing output file: ") + output_file);
        }
        output_guard.release();
    } catch(...) {
        out.close();
        throw;
    }
    return R_NilValue;
}

#endif
