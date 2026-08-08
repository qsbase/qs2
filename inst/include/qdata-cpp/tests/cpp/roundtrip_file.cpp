#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <string>

#include "qdata.h"

int main(int argc, char** argv) {
    if(argc != 3 && argc != 7 && argc != 8) {
        std::cerr << "usage: roundtrip_file <input.qdata> <output.qdata> [compress_level shuffle validate_checksum nthreads [--timings]]\n";
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

        const auto read_start = std::chrono::steady_clock::now();
        auto obj = qdata::read(argv[1], validate_checksum, nthreads);
        const auto read_seconds = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - read_start
        ).count();

        const auto save_start = std::chrono::steady_clock::now();
        qdata::save(argv[2], obj, compress_level, shuffle, nthreads);
        const auto save_seconds = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - save_start
        ).count();

        if(emit_timings) {
            std::cout << std::setprecision(17)
                      << "QDATA_TIMINGS qdata-cpp-read=" << read_seconds
                      << " qdata-cpp-save=" << save_seconds << '\n';
        }
        return 0;
    } catch(const std::exception& e) {
        std::cerr << e.what() << "\n";
        return 1;
    }
}
