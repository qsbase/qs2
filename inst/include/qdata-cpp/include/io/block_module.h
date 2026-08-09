#ifndef _QS2_BLOCK_MODULE_H
#define _QS2_BLOCK_MODULE_H

#include "io_common.h"
#include "xxhash_module.h"

// direct_mem switch does nothing, but is kept for parity with MT code
template <class stream_writer, class compressor, class hasher, class error_policy, bool direct_mem>
struct BlockCompressWriter {
    stream_writer & myFile;
    compressor cp;
    hasher hp;
    std::unique_ptr<char[]> block;
    std::unique_ptr<char[]> zblock;
    uint32_t current_blocksize;
    const int compress_level;
    BlockCompressWriter(stream_writer & f, const int compress_level) : 
        myFile(f),
        cp(),
        hp(),
        block(MAKE_UNIQUE_BLOCK(MAX_BLOCKSIZE)),
        zblock(MAKE_UNIQUE_BLOCK(MAX_ZBLOCKSIZE)),
        current_blocksize(0),
        compress_level(compress_level) {}
    private:
    // Once per block, so the guard is off the per-push path. The stream may
    // throw (ofstream failbit, or bad_alloc while a memory buffer grows) and
    // this can run underneath R's serializer, where an escaping exception would
    // cross R's C frames. Raise after the catch so the exception object is gone.
    void write_and_update(const char * const inbuffer, const uint64_t len) {
        bool ok = true;
        try { myFile.write(inbuffer, len); } catch(...) { ok = false; }
        if(!ok) cleanup_and_throw("Failed to write output");
        hp.update(inbuffer, len);
    }
    template <typename POD>
    void write_and_update(const POD value) {
        bool ok = true;
        try { myFile.writeInteger(value); } catch(...) { ok = false; }
        if(!ok) cleanup_and_throw("Failed to write output");
        hp.update(value);
    }
    void flush() {
        if(current_blocksize > 0) {
            uint32_t zsize = cp.compress(zblock.get(), MAX_ZBLOCKSIZE, block.get(), current_blocksize, compress_level);
            if(compressor::is_error(zsize)) {
                cleanup_and_throw("Compression error");
            }
            write_and_update(zsize);
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
    void cleanup() noexcept {
        // nothing
    }
    void cleanup_and_throw(const char * const msg) {
        throw_error<error_policy>(msg);
    }
    void push_data(const char * const inbuffer, const uint64_t len) {
        uint64_t current_pointer_consumed = 0;

        // if data exists in current_block, then append to it first
        if(current_blocksize >= MAX_BLOCKSIZE) { flush(); }
        if(current_blocksize > 0) {
            // append the minimum between remaining_len and remaining_block_space
            uint32_t add_length = std::min<uint64_t>(len - current_pointer_consumed, MAX_BLOCKSIZE - current_blocksize);
            std::memcpy(block.get() + current_blocksize, inbuffer + current_pointer_consumed, add_length);
            current_blocksize += add_length;
            current_pointer_consumed += add_length;
        }

        // after appending to non-empty block any additional data push assumes that current_blocksize is zero
        // True bc either inbuffer was fully consumed already (no more data to push) or block was filled and flushed (sets current_blocksize to zero)
        if(current_blocksize >= MAX_BLOCKSIZE) { flush(); }
        while(len - current_pointer_consumed >= MAX_BLOCKSIZE) {
            uint32_t zsize = cp.compress(zblock.get(), MAX_ZBLOCKSIZE, inbuffer + current_pointer_consumed, MAX_BLOCKSIZE, compress_level);
            if(compressor::is_error(zsize)) {
                cleanup_and_throw("Compression error");
            }
            write_and_update(zsize);
            // zsize contains metadata, filter it out to get size of write
            write_and_update(zblock.get(), zsize & (~BLOCK_METADATA));
            // current_blocksize = 0; // If we are in this loop, current_blocksize is already zero
            current_pointer_consumed += MAX_BLOCKSIZE;
        }

        // check if there is any remaining_len after appending full blocks
        if(len - current_pointer_consumed > 0) {
            uint32_t add_length = len - current_pointer_consumed;
            std::memcpy(block.get(), inbuffer + current_pointer_consumed, add_length);
            current_blocksize = add_length;
            // current_pointer_consumed += add_length; // unnecessary since we are returning now
        }
    }

    template<typename POD> void push_pod(const POD pod) {
        if(current_blocksize > MIN_BLOCKSIZE) { flush(); }
        const char * ptr = reinterpret_cast<const char*>(&pod);
        std::memcpy(block.get() + current_blocksize, ptr, sizeof(POD));
        current_blocksize += sizeof(POD);
    }

     // unconditionally append to block without checking current length
    template<typename POD> void push_pod_contiguous(const POD pod) {
        const char * ptr = reinterpret_cast<const char*>(&pod);
        memcpy(block.get() + current_blocksize, ptr, sizeof(POD));
        current_blocksize += sizeof(POD);
    }

    // runtime determination of contiguous
    template<typename POD> void push_pod(const POD pod, const bool contiguous) {
        if(contiguous) { push_pod_contiguous(pod); } else { push_pod(pod); }
    }
};

template <class stream_reader, class decompressor, class error_policy, class byte_copier = QioByteCopier>
struct BlockCompressReader {
    stream_reader & myFile;
    decompressor dp;
    xxHashEnv hp;
    std::unique_ptr<char[]> block;
    std::unique_ptr<char[]> zblock;
    uint32_t current_blocksize;
    uint32_t data_offset;
    BlockCompressReader(stream_reader & f) : 
        myFile(f),
        dp(),
        hp(),
        block(MAKE_UNIQUE_BLOCK(MAX_BLOCKSIZE)),
        zblock(MAKE_UNIQUE_BLOCK(MAX_ZBLOCKSIZE)),
        current_blocksize(0), 
        data_offset(0) {}
    private:
    void decompress_block() {
        uint32_t zsize;
        bool ok = myFile.readInteger(zsize);
        if(!ok) {
            cleanup_and_throw("Unexpected end of file while reading next block size");
        }
        const uint32_t zbytes = compressed_block_size(zsize);
        if(!compressed_block_size_fits_buffer(zsize)) {
            cleanup_and_throw("Compressed block size exceeds internal maximum");
        }
        hp.update(zsize);
        uint32_t bytes_read = myFile.read(zblock.get(), zbytes);
        if(bytes_read != zbytes) {
            cleanup_and_throw("Unexpected end of file while reading next block");
        }
        hp.update(zblock.get(), bytes_read);
        current_blocksize = dp.decompress(block.get(), MAX_BLOCKSIZE, zblock.get(), zsize);
        if(decompressor::is_error(current_blocksize)) { cleanup_and_throw("Decompression error"); }
    }
    void decompress_direct(char * outbuffer) {
        uint32_t zsize;
        bool ok = myFile.readInteger(zsize);
        if(!ok) {
            cleanup_and_throw("Unexpected end of file while reading next block size");
        }
        const uint32_t zbytes = compressed_block_size(zsize);
        if(!compressed_block_size_fits_buffer(zsize)) {
            cleanup_and_throw("Compressed block size exceeds internal maximum");
        }
        hp.update(zsize);
        uint32_t bytes_read = myFile.read(zblock.get(), zbytes);
        if(bytes_read != zbytes) {
            cleanup_and_throw("Unexpected end of file while reading next block");
        }
        hp.update(zblock.get(), bytes_read);
        current_blocksize = dp.decompress(outbuffer, MAX_BLOCKSIZE, zblock.get(), zsize);
        if(decompressor::is_error(current_blocksize)) { cleanup_and_throw("Decompression error"); }
    }
    public:
    void finish() {
        // nothing
    }
    void cleanup() noexcept {
        // nothing
    }
    // const char* only: under an error policy that longjmps, an owning
    // temporary here would never be destroyed
    void cleanup_and_throw(const char * const msg) {
        throw_error<error_policy>(msg);
    }
    uint64_t get_hash_digest() {
        return hp.digest();
    }
    const char * current_data() {
        if(current_blocksize == data_offset) {
            decompress_block();
            data_offset = 0;
        }
        return block.get() + data_offset;
    }
    uint32_t remaining_data() {
        if(current_blocksize == data_offset) {
            decompress_block();
            data_offset = 0;
        }
        return current_blocksize - data_offset;
    }
    void advance_data(const uint32_t len) {
        if(current_blocksize - data_offset < len) {
            cleanup_and_throw("Corrupted block data");
        }
        data_offset += len;
    }
    void get_data(char * outbuffer, const uint64_t len) {
        if(current_blocksize - data_offset >= len) {
            byte_copier::copy(outbuffer, block.get()+data_offset, len);
            data_offset += len;
        } else {
            // remainder of current block, may be zero
            uint64_t bytes_accounted = current_blocksize - data_offset;
            byte_copier::copy(outbuffer, block.get()+data_offset, bytes_accounted);
            while(len - bytes_accounted >= MAX_BLOCKSIZE) {
                decompress_direct(outbuffer + bytes_accounted);
                if(current_blocksize != MAX_BLOCKSIZE) {
                    cleanup_and_throw("Corrupted block data");
                }
                bytes_accounted += MAX_BLOCKSIZE;
                data_offset = MAX_BLOCKSIZE;
            }
            if(len - bytes_accounted > 0) { // but less than MAX_BLOCKSIZE
                decompress_block();
                if(current_blocksize < len - bytes_accounted) {
                    cleanup_and_throw("Corrupted block data");
                }
                byte_copier::copy(outbuffer + bytes_accounted, block.get(), len - bytes_accounted);
                data_offset = len - bytes_accounted;
                // bytes_accounted += data_offset; // no need to update since we are returning
            }
        }
    }

    const char * get_ptr(const uint64_t len) {
        if(current_blocksize - data_offset >= len) {
            const char * ptr = block.get() + data_offset;
            data_offset += len;
            return ptr;
        } else {
            // return nullptr, indicating len exceeds current block
            // copy data using get_data
            return nullptr;
        }
    }

    // A POD shall not be split across block boundaries
    // But data_offset may be at the end of a block, so we still need to check
    // Also we should guard against data corruption, by checking that there is
    // enough data left in the block
    template<typename POD> POD get_pod() {
        if(current_blocksize == data_offset) {
            decompress_block();
            data_offset = 0;
        }
        if(current_blocksize - data_offset < sizeof(POD)) {
            cleanup_and_throw("Corrupted block data");
        }
        POD pod;
        memcpy(&pod, block.get()+data_offset, sizeof(POD));
        data_offset += sizeof(POD);
        return pod;
    }

    template<typename POD> POD get_pod_contiguous() {
        if(current_blocksize - data_offset < sizeof(POD)) {
            cleanup_and_throw("Corrupted block data");
        }
        POD pod;
        memcpy(&pod, block.get()+data_offset, sizeof(POD));
        data_offset += sizeof(POD);
        return pod;
    }
};

#endif
