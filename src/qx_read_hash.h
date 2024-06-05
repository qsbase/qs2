#ifndef _QS2_QX_READ_HASH_H_
#define _QS2_QX_READ_HASH_H_

#include "qx_file_headers.h"

// get file hash from remaining bytes from the current position
// reset seek position to current position
template <class stream_reader>
uint64_t read_qx_hash(stream_reader & reader) {
    auto current_position = reader.tellg();
    xxHashEnv env;
    std::unique_ptr<char[]> zblock(MAKE_UNIQUE_BLOCK(MAX_ZBLOCKSIZE));
    uint64_t bytes_read = 0;
    while( (bytes_read = reader.read(zblock.get(), MAX_ZBLOCKSIZE)) ) {
        env.update(zblock.get(), bytes_read);
    }
    reader.seekg(current_position);
    return env.digest();
}

#endif