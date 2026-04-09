#ifndef _QS2_QX_DUMP_H_
#define _QS2_QX_DUMP_H_

#include <Rcpp.h>
#include "qx_file_headers.h"

using namespace Rcpp;

template <typename stream_reader, typename decompressor>
std::tuple<std::vector<std::vector<unsigned char>>, std::vector<std::vector<unsigned char>>, std::vector<int>, std::string> 
qx_dump_impl(stream_reader & myFile) {
    decompressor dp;
    xxHashEnv env;
    std::tuple<std::vector<std::vector<unsigned char>>, std::vector<std::vector<unsigned char>>, std::vector<int>, std::string> output;
    while(true) {
        std::vector<unsigned char> zblock(MAX_ZBLOCKSIZE); // use unsigned char to auto-convert to Rcpp::RawVector
        std::vector<unsigned char> block(MAX_BLOCKSIZE);

        uint32_t zsize;
        uint32_t size_bytes_read = myFile.read(reinterpret_cast<char*>(&zsize), sizeof(zsize));
        if(size_bytes_read == 0) {
            break;
        }
        if(size_bytes_read != sizeof(zsize)) {
            throw std::runtime_error("Unexpected end of file while reading next block size");
        }

        const uint32_t zbytes = compressed_block_size(zsize);
        if(!compressed_block_size_fits_buffer(zsize)) {
            throw std::runtime_error("Compressed block size exceeds internal maximum");
        }

        uint32_t bytes_read = myFile.read(reinterpret_cast<char*>(zblock.data()), zbytes);
        if(bytes_read != zbytes) {
            throw std::runtime_error("Unexpected end of file while reading next block");
        }

        env.update(zsize);
        env.update(reinterpret_cast<char*>(zblock.data()), bytes_read);

        int shuffled = (zsize & SHUFFLE_MASK) > 0 ? 1 : 0;

        uint32_t blocksize = dp.decompress(reinterpret_cast<char*>(block.data()), MAX_BLOCKSIZE,
                                           reinterpret_cast<char*>(zblock.data()), zsize);
        if(decompressor::is_error(blocksize)) {
            throw std::runtime_error("Decompression error");
        }
        
        zblock.resize(bytes_read);
        block.resize(blocksize);

        std::get<0>(output).push_back(std::move(zblock));
        std::get<1>(output).push_back(std::move(block));
        std::get<2>(output).push_back(shuffled);
    }
    std::get<3>(output) = std::to_string(env.digest());
    return output;
}

#endif
