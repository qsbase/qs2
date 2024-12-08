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
        bool ok = myFile.readInteger(zsize);
        if(!ok) {
            break;
        }

        uint32_t bytes_read = myFile.read(reinterpret_cast<char*>(zblock.data()), zsize & (~BLOCK_METADATA));
        if(bytes_read != (zsize & (~BLOCK_METADATA))) {
            break;
        }

        env.update(zsize);
        env.update(reinterpret_cast<char*>(zblock.data()), bytes_read);

        int shuffled = (zsize & SHUFFLE_MASK) > 0 ? 1 : 0;

        uint32_t blocksize = dp.decompress(reinterpret_cast<char*>(block.data()), MAX_BLOCKSIZE,
                                           reinterpret_cast<char*>(zblock.data()), zsize);
        
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