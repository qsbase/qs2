// include guard
#ifndef _QS2_FILESTREAM_MODULE_H
#define _QS2_FILESTREAM_MODULE_H

#include "io/io_common.h"

// in binary mode, seek/tell should be byte offsets from beginning of the file
// libstdc++ uses file descriptors under the hood for std::fstream:
// https://stackoverflow.com/a/5253726/2723734
// On posix, this gives byte offsets: https://stackoverflow.com/a/9138149/2723734

struct IfStreamReader {
    std::ifstream con;
    IfStreamReader(const char * const path) : con(path, std::ios::in | std::ios::binary) {}
    bool isValid() { return con.is_open(); }
    uint32_t read(char * const ptr, const uint32_t count) {
        con.read(ptr, count);
        return con.gcount();
    }
    // return whether enough bytes were read to fill the value
    template <typename T> bool readInteger(T & value) {
        return read(reinterpret_cast<char*>(&value), sizeof(T)) == sizeof(T);

    }
    bool isSeekable() const { return true; }
    void seekg(const uint32_t pos) {
        // https://stackoverflow.com/q/16364301/2723734
        // If EOF is reached, need to call clear() before seekg()
        // since realistically we are only seeking at EOF, it's fine to unconditionally call clear
        con.clear();
        con.seekg(pos);
    }
    uint32_t tellg() { return con.tellg(); }
};

struct OfStreamWriter {
    std::ofstream con;
    OfStreamWriter(const char * const path) : con(path, std::ios::out | std::ios::binary) {}
    bool isValid() { return con.is_open(); }
    uint32_t write(const char * const ptr, const uint32_t count) {
        con.write(ptr, count);
        return count;
    }
    template <typename T> void writeInteger(const T value) {
        con.write(reinterpret_cast<const char*>(&value), sizeof(T));
    }
    bool isSeekable() const { return true; }
    void seekp(const uint32_t pos) { con.seekp(pos); }
    uint32_t tellp() { return con.tellp(); }
};

#endif