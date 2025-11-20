#ifndef _QS2_ZSTD_FILE_FUNCTIONS_H
#define _QS2_ZSTD_FILE_FUNCTIONS_H

#include <Rcpp.h>
#include "io/zstd_file.h"

using namespace Rcpp;

// [[Rcpp::export(rng = false, invisible = true, signature = {input_file, output_file, compress_level = qopt("compress_level")})]]
SEXP zstd_compress_file(const std::string& input_file, const std::string& output_file, const int compress_level) {
    if (compress_level > ZSTD_maxCLevel() || compress_level < ZSTD_minCLevel()) {
        throw_error<ErrorType::r_error>("compress_level out of range for zstd");
    }

    const std::string input_path = R_ExpandFileName(input_file.c_str());
    const std::string output_path = R_ExpandFileName(output_file.c_str());

    std::ifstream in(input_path, std::ios::binary);
    if (!in) {
        throw_error<ErrorType::r_error>(std::string("Failed to open input file: ") + input_file);
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
        throw_error<ErrorType::r_error>(std::string("Error while reading input file: ") + input_file);
    }
    writer.close();
    return R_NilValue;
}

// [[Rcpp::export(rng = false, invisible = true, signature = {input_file, output_file})]]
SEXP zstd_decompress_file(const std::string& input_file, const std::string& output_file) {
    const std::string input_path = R_ExpandFileName(input_file.c_str());
    const std::string output_path = R_ExpandFileName(output_file.c_str());

    ZstdReader reader(input_path, 1 << 20);
    std::ofstream out(output_path, std::ios::binary);
    if (!out) {
        throw_error<ErrorType::r_error>(std::string("Failed to open output file for writing: ") + output_file);
    }

    std::vector<char> buffer(1 << 20);
    size_t got = reader.read(buffer.data(), buffer.size());
    while (got > 0) {
        out.write(buffer.data(), static_cast<std::streamsize>(got));
        if (!out) {
            throw_error<ErrorType::r_error>(std::string("Error while writing output file: ") + output_file);
        }
        got = reader.read(buffer.data(), buffer.size());
    }
    return R_NilValue;
}

#endif
