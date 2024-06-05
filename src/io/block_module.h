#ifndef _QS2_BLOCK_MODULE_H
#define _QS2_BLOCK_MODULE_H

#include "io/io_common.h"

template <class stream_writer, class compressor, class hasher, ErrorType E>
struct BlockCompressWriter {
    stream_writer & myFile;
    compressor cp;
    hasher hp;
    std::unique_ptr<char[]> block;
    std::unique_ptr<char[]> zblock;
    uint64_t current_blocksize;
    const int compress_level;
    BlockCompressWriter(stream_writer & f, int compress_level) : 
        myFile(f),
        cp(),
        hp(),
        block(MAKE_UNIQUE_BLOCK(MAX_BLOCKSIZE)),
        zblock(MAKE_UNIQUE_BLOCK(MAX_ZBLOCKSIZE)),
        current_blocksize(0),
        compress_level(compress_level) {}
    private:
    void write_and_update(const char * const inbuffer, const uint64_t len) {
        myFile.write(inbuffer, len);
        hp.update(inbuffer, len);
    }
    template <typename POD>
    void write_and_update(const POD value) {
        myFile.writeInteger(value);
        hp.update(value);
    }
    void flush() {
        if(current_blocksize > 0) {
            uint64_t zsize = cp.compress(zblock.get(), MAX_ZBLOCKSIZE, block.get(), current_blocksize, compress_level);
            write_and_update(static_cast<uint32_t>(zsize));
            // zsize contains metadata, filter it out to get size of write
            write_and_update(zblock.get(), zsize & (~BLOCK_METADATA));
            current_blocksize = 0;
        }
    }
    public:
    uint64_t finish() {
        flush();
        return hp.digest();
    }
    void cleanup() {
        // nothing
    }
    void cleanup_and_throw(const std::string msg) {
        throw_error<E>(msg.c_str());
    }
    void push_data(const char * const inbuffer, const uint64_t len) {
        uint64_t current_pointer_consumed = 0;
        while(current_pointer_consumed < len) {
            if(current_blocksize >= MAX_BLOCKSIZE) { flush(); }
            
            if(current_blocksize == 0 && len - current_pointer_consumed >= MAX_BLOCKSIZE) {
                uint64_t zsize = cp.compress(zblock.get(), MAX_ZBLOCKSIZE, inbuffer + current_pointer_consumed, MAX_BLOCKSIZE, compress_level);
                write_and_update(static_cast<uint32_t>(zsize));
                // zsize contains metadata, filter it out to get size of write
                write_and_update(zblock.get(), zsize & (~BLOCK_METADATA));
                current_pointer_consumed += MAX_BLOCKSIZE;
                current_blocksize = 0;
            } else {
                uint64_t remaining_pointer_available = len - current_pointer_consumed;
                uint64_t add_length = remaining_pointer_available < (MAX_BLOCKSIZE - current_blocksize) ? remaining_pointer_available : MAX_BLOCKSIZE-current_blocksize;
                std::memcpy(block.get() + current_blocksize, inbuffer + current_pointer_consumed, add_length);
                current_blocksize += add_length;
                current_pointer_consumed += add_length;
            }
        }
    }
    template<typename POD> void push_pod(const POD pod) {
        // TOUT("push_pod ", (int)pod, " ", sizeof(POD));
        if(current_blocksize > MIN_BLOCKSIZE) { flush(); }
        const char * ptr = reinterpret_cast<const char*>(&pod);
        std::memcpy(block.get() + current_blocksize, ptr, sizeof(POD));
        current_blocksize += sizeof(POD);
    }

     // unconditionally append to block without checking current length
    template<typename POD> void push_pod_contiguous(const POD pod) {
        // TOUT("push_pod_contiguous ", (int)pod, " ", sizeof(POD));
        const char * ptr = reinterpret_cast<const char*>(&pod);
        memcpy(block.get() + current_blocksize, ptr, sizeof(POD));
        current_blocksize += sizeof(POD);
    }

    // runtime determination of contiguous
    template<typename POD> void push_pod(const POD pod, const bool contiguous) {
        if(contiguous) { push_pod_contiguous(pod); } else { push_pod(pod); }
    }
};

template <class stream_reader, class decompressor, ErrorType E> 
struct BlockCompressReader {
    stream_reader & myFile;
    decompressor dp;
    std::unique_ptr<char[]> block;
    std::unique_ptr<char[]> zblock;
    uint64_t current_blocksize;
    uint64_t data_offset;
    BlockCompressReader(stream_reader & f) : 
        myFile(f),
        dp(),
        block(MAKE_UNIQUE_BLOCK(MAX_BLOCKSIZE)),
        zblock(MAKE_UNIQUE_BLOCK(MAX_ZBLOCKSIZE)),
        current_blocksize(0), 
        data_offset(0) {}
    private:
    void decompress_block() {
        uint32_t zsize;
        bool ok = myFile.readInteger(zsize);
        TOUT("decompress_block ", zsize);
        if(!ok) {
            cleanup_and_throw("Unexpected end of file while reading next block size");
        }
        size_t bytes_read = myFile.read(zblock.get(), zsize & (~BLOCK_METADATA));
        if(bytes_read != (zsize & (~BLOCK_METADATA))) {
            cleanup_and_throw("Unexpected end of file while reading next block");
        }
        current_blocksize = dp.decompress(block.get(), MAX_BLOCKSIZE, zblock.get(), zsize);
        if(decompressor::is_error(current_blocksize)) { cleanup_and_throw("Decompression error"); }
        data_offset = 0;
        TOUT("blocksize ", current_blocksize);
    }
    void decompress_direct(char * outbuffer) {
        uint32_t zsize;
        bool ok = myFile.readInteger(zsize);
        TOUT("decompress_direct ", zsize);
        if(!ok) {
            cleanup_and_throw("Unexpected end of file while reading next block size");
        }
        size_t bytes_read = myFile.read(zblock.get(), zsize & (~BLOCK_METADATA));
        if(bytes_read != (zsize & (~BLOCK_METADATA))) {
            cleanup_and_throw("Unexpected end of file while reading next block");
        }
        current_blocksize = dp.decompress(outbuffer, MAX_BLOCKSIZE, zblock.get(), zsize);
        if(decompressor::is_error(current_blocksize)) { cleanup_and_throw("Decompression error"); }
        TOUT("blocksize ", current_blocksize);
    }
    public:
    void finish() {
        // nothing
    }
    void cleanup() {
        // nothing
    }
    void cleanup_and_throw(const std::string msg) {
        throw_error<E>(msg.c_str());
    }
    void get_data(char * outbuffer, const uint64_t len) {
        // TOUT("get_data ", len, " ", current_blocksize, " ", data_offset);
        if(current_blocksize - data_offset >= len) {
            std::memcpy(outbuffer, block.get()+data_offset, len);
            data_offset += len;
        } else {
            uint64_t bytes_accounted = current_blocksize - data_offset;
            std::memcpy(outbuffer, block.get()+data_offset, bytes_accounted);
            while(bytes_accounted < len) {
                if(len - bytes_accounted >= MAX_BLOCKSIZE) {
                    decompress_direct(outbuffer + bytes_accounted);
                    bytes_accounted += current_blocksize;
                    data_offset = current_blocksize;
                } else {
                    decompress_block();
                    // if not enough bytes in block then something went wrong, throw error
                    if(current_blocksize < len - bytes_accounted) {
                        cleanup_and_throw("Corrupted block data");
                    }
                    std::memcpy(outbuffer + bytes_accounted, block.get(), len - bytes_accounted);
                    data_offset = len - bytes_accounted;
                    bytes_accounted += data_offset;
                }
            }
        }
    }

    // A POD shall not be split across block boundaries
    // But data_offset may be at the end of a block, so we still need to check
    // Also we should guard against data corruption, by checking that there is
    // enough data left in the block
    template<typename POD> POD get_pod() {
        if(current_blocksize == data_offset) {
            decompress_block();
        }
        if(current_blocksize - data_offset < sizeof(POD)) {
            cleanup_and_throw("Corrupted block data");
        }
        POD pod;
        memcpy(&pod, block.get()+data_offset, sizeof(POD));
        data_offset += sizeof(POD);
        // TOUT("get_pod ", (int)pod, " ", sizeof(POD));
        return pod;
    }

    template<typename POD> POD get_pod_contiguous() {
        if(current_blocksize - data_offset < sizeof(POD)) {
            cleanup_and_throw("Corrupted block data");
        }
        POD pod;
        memcpy(&pod, block.get()+data_offset, sizeof(POD));
        data_offset += sizeof(POD);
        // TOUT("get_pod_contiguous ", (int)pod, " ", sizeof(POD));
        return pod;
    }
};

#endif
