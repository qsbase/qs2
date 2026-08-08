#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include "qdata.h"

namespace {

std::vector<std::byte> read_bytes(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if(!in) {
        throw std::runtime_error("failed to open file for reading: " + path);
    }

    in.seekg(0, std::ios::end);
    const auto size = static_cast<std::size_t>(in.tellg());
    in.seekg(0, std::ios::beg);

    std::vector<std::byte> out(size);
    if(size > 0) {
        in.read(reinterpret_cast<char*>(out.data()), static_cast<std::streamsize>(size));
        if(!in) {
            throw std::runtime_error("failed to read full input buffer");
        }
    }
    return out;
}

template <class Buffer>
void write_bytes(const std::string& path, const Buffer& data) {
    std::ofstream out(path, std::ios::binary);
    if(!out) {
        throw std::runtime_error("failed to open file for writing: " + path);
    }
    if(data.size() > 0) {
        out.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(data.size()));
        if(!out) {
            throw std::runtime_error("failed to write full output buffer");
        }
    }
}

} // namespace

int main(int argc, char** argv) {
    if(argc != 3 && argc != 7 && argc != 8) {
        std::cerr << "usage: roundtrip_memory <input.bin> <output.bin> [compress_level shuffle validate_checksum nthreads [--timings]]\n";
        return 1;
    }

    try {
        int compress_level = 3;
        bool shuffle = true;
        bool validate_checksum = false;
        int nthreads = 1;
        bool emit_timings = false;

        if(argc >= 7) {
            compress_level = std::atoi(argv[3]);
            shuffle = std::atoi(argv[4]) != 0;
            validate_checksum = std::atoi(argv[5]) != 0;
            nthreads = std::atoi(argv[6]);
        }
        if(argc == 8) {
            emit_timings = std::string(argv[7]) == "--timings";
            if(!emit_timings) {
                std::cerr << "expected optional flag --timings\n";
                return 1;
            }
        }

        const auto input = read_bytes(argv[1]);
        const auto deserialize_start = std::chrono::steady_clock::now();
        auto obj = qdata::deserialize(input, validate_checksum, nthreads);
        const auto deserialize_seconds = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - deserialize_start
        ).count();

        const auto serialize_start = std::chrono::steady_clock::now();
        auto output = qdata::serialize<std::vector<char>>(obj, compress_level, shuffle, nthreads);
        const auto serialize_seconds = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - serialize_start
        ).count();

        write_bytes(argv[2], output);
        if(emit_timings) {
            std::cout << std::setprecision(17)
                      << "QDATA_TIMINGS qdata-cpp-deserialize=" << deserialize_seconds
                      << " qdata-cpp-serialize=" << serialize_seconds << '\n';
        }
        return 0;
    } catch(const std::exception& e) {
        std::cerr << e.what() << "\n";
        return 1;
    }
}
