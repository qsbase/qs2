// This header is not used in qs2/qd formats, but for utility functions

#ifndef _QS2_ZSTD_FILE_H
#define _QS2_ZSTD_FILE_H

#include <cstddef>
#include <string>
#include <vector>
#include <memory>
#include <fstream>
#include <stdexcept>
#include <algorithm>
#include <cstdio>
#include <cerrno>
#include <array>
#include <zstd.h>

// magic bytes used to test ZSTD format
static constexpr std::array<uint8_t, 4> ZSTD_MAGIC{{0x28, 0xb5, 0x2f, 0xfd}};

template <size_t N>
inline void ensure_magic_prefix(const std::string& path,
                                const std::array<uint8_t, N>& magic,
                                const char* format_name) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw std::runtime_error(std::string("Failed to open ") + format_name + " file: " + path);
    }
    std::array<char, N> buf{};
    in.read(buf.data(), static_cast<std::streamsize>(N));
    if (in.gcount() < static_cast<std::streamsize>(N)) {
        throw std::runtime_error(std::string("File too short to be valid ") + format_name + ": " + path);
    }
    for (size_t i = 0; i < N; ++i) {
        if (static_cast<uint8_t>(buf[i]) != magic[i]) {
            throw std::runtime_error(std::string("File does not appear to be valid ") + format_name + ": " + path);
        }
    }
}

struct IWriter {
    virtual ~IWriter() = default;
    virtual void write(const char *data, size_t len) = 0;
    virtual void writeLine(const std::string &line) = 0;
    virtual void flush() = 0;
    virtual bool has_pending() const { return false; }
    virtual void close() {}
};


struct IReader {
    virtual ~IReader() = default;

    virtual size_t read(char* dst, size_t cnt) = 0;
    virtual bool   readLine(std::string& out) = 0;
};

// zstd writer
struct ZstdWriter : IWriter {
    FILE*              _f;
    ZSTD_CCtx*         _cctx;
    std::vector<char>  _inBuf;
    std::vector<char>  _outBuf;
    size_t             _bufSize;
    size_t             _inPos{0};

    explicit ZstdWriter(const std::string &path,
                        int compress_level,
                        size_t bufSize = 64*1024)
      : _f(std::fopen(path.c_str(), "wb"))
      , _cctx(ZSTD_createCCtx())
      , _inBuf(bufSize)
      , _outBuf(ZSTD_CStreamOutSize())
      , _bufSize(bufSize)
    {
        if (!_f) {
            throw std::runtime_error("Failed to open " + path);
        }
        if (!_cctx) {
            std::fclose(_f);
            throw std::runtime_error("ZSTD_createCCtx failed");
        }
        size_t init = ZSTD_initCStream(_cctx, compress_level);
        if (ZSTD_isError(init)) {
            ZSTD_freeCCtx(_cctx);
            std::fclose(_f);
            throw std::runtime_error(std::string("ZSTD_initCStream failed: ")
                                     + ZSTD_getErrorName(init));
        }
    }

    ~ZstdWriter() override {
        try { close(); } catch(...) {}
    }

    void write(const char* data, size_t len) override {
        size_t done = 0;
        while (done < len) {
            size_t space = _bufSize - _inPos;
            size_t chunk = std::min(space, len - done);
            std::memcpy(_inBuf.data() + _inPos, data + done, chunk);
            _inPos += chunk;
            done   += chunk;
            if (_inPos == _bufSize) {
                compressChunk(); // compress full input buffer
            }
        }
    }

    void writeLine(const std::string &line) override {
        write(line.data(), line.size());
        static const char nl = '\n';
        write(&nl, 1);
    }

    void flush() override {
        if (_inPos > 0) {
            compressChunk(); // push remaining input
        }

        // finish the frame
        ZSTD_outBuffer out = { _outBuf.data(), _outBuf.size(), 0 };
        size_t ret;
        do {
            ret = ZSTD_endStream(_cctx, &out);
            if (ZSTD_isError(ret)) {
                throw std::runtime_error(std::string("ZSTD_endStream failed: ")
                                         + ZSTD_getErrorName(ret));
            }
            if (out.pos > 0) {
                size_t written = std::fwrite(_outBuf.data(), 1, out.pos, _f);
                if (written != out.pos) {
                    throw std::runtime_error("fwrite failed");
                }
                out.pos = 0;
            }
        } while (ret > 0);

        std::fflush(_f);
    }

    void close() override {
        if (!_cctx && !_f) return;
        flush();
        if (_cctx) {
            ZSTD_freeCCtx(_cctx);
            _cctx = nullptr;
        }
        if (_f) {
            std::fclose(_f);
            _f = nullptr;
        }
    }

private:
    void compressChunk() {
        ZSTD_inBuffer  in  = { _inBuf.data(), _inPos, 0 };
        ZSTD_outBuffer out = { _outBuf.data(), _outBuf.size(), 0 };

        while (in.pos < in.size) {
            size_t ret = ZSTD_compressStream(_cctx, &out, &in);
            if (ZSTD_isError(ret)) {
                throw std::runtime_error(std::string("ZSTD_compressStream failed: ")
                                         + ZSTD_getErrorName(ret));
            }
            if (out.pos > 0) {
                size_t written = std::fwrite(_outBuf.data(), 1, out.pos, _f);
                if (written != out.pos) {
                    throw std::runtime_error("fwrite failed");
                }
                out.pos = 0; // reset for next iteration
            }
        }
        _inPos = 0; // input buffer fully consumed
    }
};

// zstd reader
struct ZstdReader : IReader {
    FILE*              _f;
    ZSTD_DCtx*         _dctx;
    std::vector<char>  _inBuf;
    std::vector<char>  _outBuf;
    size_t             _inPos;
    size_t             _inSize;
    size_t             _outPos;
    size_t             _outSize;
    size_t             _pendingFrameBytes;

    explicit ZstdReader(const std::string& path, size_t bufSize)
        : _f(nullptr),
          _dctx(nullptr),
          _inBuf(bufSize),
          _outBuf(bufSize),
          _inPos(0),
          _inSize(0),
          _outPos(0),
          _outSize(0),
          _pendingFrameBytes(0)
    {
        ensure_magic_prefix(path, ZSTD_MAGIC, "zstd");
        _f = std::fopen(path.c_str(), "rb");
        if (!_f) throw std::runtime_error("zstd open failed: " + path);

        _dctx = ZSTD_createDCtx();
        if (!_dctx) {
            std::fclose(_f);
            throw std::runtime_error("ZSTD_createDCtx failed");
        }
        size_t init = ZSTD_initDStream(_dctx);
        if (ZSTD_isError(init)) {
            ZSTD_freeDCtx(_dctx);
            std::fclose(_f);
            throw std::runtime_error("ZSTD_initDStream failed: " +
                                     std::string(ZSTD_getErrorName(init)));
        }
    }

    ~ZstdReader() override {
        if (_dctx) ZSTD_freeDCtx(_dctx);
        if (_f)    std::fclose(_f);
    }

    size_t read(char* dst, size_t cnt) override {
        size_t tot = 0;
        while (tot < cnt) {
            if (_outPos == _outSize) {
                if (_inPos == _inSize) {
                    _inSize = std::fread(_inBuf.data(), 1,
                                         _inBuf.size(), _f);
                    _inPos = 0;
                    if (_inSize == 0) break;
                }
                ZSTD_inBuffer  in  = {_inBuf.data() + _inPos, _inSize - _inPos, 0};
                ZSTD_outBuffer out = {_outBuf.data(), _outBuf.size(), 0};
                size_t ret = ZSTD_decompressStream(_dctx, &out, &in);
                if (ZSTD_isError(ret))
                    throw std::runtime_error("ZSTD_decompressStream failed: " +
                                             std::string(ZSTD_getErrorName(ret)));
                _pendingFrameBytes = ret;
                _inPos   += in.pos;
                _outSize = out.pos;
                _outPos   = 0;
                if (_outSize == 0) continue;
            }
            size_t chunk = std::min(cnt - tot, _outSize - _outPos);
            std::memcpy(dst + tot, _outBuf.data() + _outPos, chunk);
            tot     += chunk;
            _outPos += chunk;
        }
        if (tot < cnt && _inSize == 0 && _outSize == 0 && _pendingFrameBytes > 0) {
            throw std::runtime_error("zstd input truncated before end of frame");
        }
        return tot;
    }

    bool readLine(std::string& out) override {
        out.clear();
        char c;
        while (read(&c, 1) == 1) {
            if (c == '\n') return true;
            out.push_back(c);
        }
        return !out.empty();
    }
};

#endif
