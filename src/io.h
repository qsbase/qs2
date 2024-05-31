// include guard
#ifndef _QS2_IO_H
#define _QS2_IO_H

#include <fstream>
#include <cstdint>
#include <memory>
#include <cstring>

#include "zstd.h"
#include "BLOSC/shuffle_routines.h"
#include "BLOSC/unshuffle_routines.h"

#ifndef QS2_BLOCKSIZE_TESTING_ONLY_DO_NOT_USE
static constexpr uint64_t MAX_BLOCKSIZE = 524288ULL; // 2^19 ... we save blocksize as uint32_t, so the last 13 MSBs can be used to store metadata
#else
static constexpr uint64_t MAX_BLOCKSIZE = QS2_BLOCKSIZE_TESTING_ONLY_DO_NOT_USE;
#endif

static constexpr uint64_t BLOCK_RESERVE = 64ULL;
static constexpr uint64_t MIN_BLOCKSIZE = MAX_BLOCKSIZE - BLOCK_RESERVE; // smallest allowable block size, except for last block
static const uint64_t MAX_ZBLOCKSIZE = ZSTD_compressBound(MAX_BLOCKSIZE);
static constexpr uint32_t BLOCK_METADATA = 0xFFF00000;  // 11111111 11110000 00000000 00000000 in binary, First 12 MSBs can be used for metadata in either zblock or block

static constexpr uint64_t SHUFFLE_ELEMSIZE = 8ULL;
static constexpr uint32_t SHUFFLE_MASK = (1ULL << 31);
static const int SHUFFLE_HEURISTIC_CLEVEL = -1; // compress with fast clevel to test whether shuffle is better
static constexpr uint64_t SHUFFLE_HEURISTIC_BLOCKSIZE = 16384ULL; // 524288 / 8 / 4
static constexpr float SHUFFLE_MIN_IMPROVEMENT_THRESHOLD = 1.07f; // shuffle must be at least 7% better to use

// MAKE_UNIQUE_BLOCK and MAKE_SHARED_BLOCK macros should be used ONLY in initializer lists
#if __cplusplus >= 201402L // Check for C++14 or above
    #define MAKE_UNIQUE_BLOCK(SIZE) std::make_unique<char[]>(SIZE)
#else
    #define MAKE_UNIQUE_BLOCK(SIZE) new char[SIZE]
#endif

// std::make_shared on an array requires c++20 (yes it is true)
// If you try to use make_shared on c++17 or lower it won't compile or will segfault.
#if __cplusplus >= 202002L  // Check for C++20 or above
    #define MAKE_SHARED_BLOCK(SIZE) std::make_shared<char[]>(SIZE)
#else
    #define MAKE_SHARED_BLOCK(SIZE) new char[SIZE]
#endif

#if __cplusplus >= 202002L  // Check for C++20 or above
    #define MAKE_SHARED_BLOCK_ASSIGNMENT(SIZE) std::make_shared<char[]>(SIZE)
#else
    #define MAKE_SHARED_BLOCK_ASSIGNMENT(SIZE) std::shared_ptr<char[]>(new char[SIZE])
#endif


// #define QS_MT_SERIALIZATION_DEBUG
#if defined(QS_MT_SERIALIZATION_DEBUG)
    #include <iostream>
    #include <sstream>
    #include <mutex>
    // multithreaded print statements for debugging
    // https://stackoverflow.com/a/53288135/2723734
    // Thread-safe std::ostream class.
    #define tout ThreadStream(std::cout)
    class ThreadStream : public std::ostringstream {
    public:
    ThreadStream(std::ostream& os) : os_(os)
    {
        imbue(os.getloc());
        precision(os.precision());
        width(os.width());
        setf(std::ios::fixed, std::ios::floatfield);
    }
    ~ThreadStream() {
        std::lock_guard<std::mutex> guard(_mutex_threadstream);
        os_ << this->str();
    }
    private:
    static std::mutex _mutex_threadstream;
    std::ostream& os_;
    };
    inline std::mutex ThreadStream::_mutex_threadstream{};
    template<typename ...Args>
    inline void TOUT(Args && ...args) {
        (tout << ... << args) << std::endl;
    }
#else
    #define TOUT(...)
#endif


// classes defined inline
// IfStreamReader - for reading from a file
// OfStreamWriter - for writing to a file
// ZstdCompressor - for compressing data
// ZstdDecompressor - for decompressing data
// ZstdShuffleCompressor - for compressing data in shuffle mode
// ZstdShuffleDecompressor - for decompressing data in shuffle mode
// BlockCompressReader - for reading compressed data from a templated stream_reader using a templated decompressor
// BlockCompressWriter - for writing compressed data to a templated stream_writer using a templated compressor

struct IfStreamReader {
    std::ifstream con;
    IfStreamReader(const char * const path) : con(path, std::ios::in | std::ios::binary) {}
    bool isValid() { return con.is_open(); }
    uint64_t read(char * const ptr, const uint64_t count) {
        con.read(ptr, count);
        return con.gcount();
    }
    // return whether enough bytes were read to fill the value
    template <typename T> bool readInteger(T & value) {
        return read(reinterpret_cast<char*>(&value), sizeof(T)) == sizeof(T);

    }
    bool isSeekable() { return true; }
};

struct OfStreamWriter {
    std::ofstream con;
    OfStreamWriter(const char * const path) : con(path, std::ios::out | std::ios::binary) {}
    bool isValid() { return con.is_open(); }
    uint64_t write(const char * const ptr, const uint64_t count) {
        con.write(ptr, count);
        return count;
    }
    template <typename T> void writeInteger(const T value) {
        con.write(reinterpret_cast<const char*>(&value), sizeof(T));
    }
};

struct ZstdCompressor {
    ZSTD_CCtx * cctx;
    ZstdCompressor() : cctx(ZSTD_createCCtx()) {}
    ~ZstdCompressor() {
        ZSTD_freeCCtx(cctx);
    }
    uint64_t compress(char * const dst, const uint64_t dstCapacity,
                      const char * const src, const uint64_t srcSize,
                      int compress_level) {
        return ZSTD_compressCCtx(cctx, dst, dstCapacity, src, srcSize, compress_level);
    }
};


// to do: investigate whether 16/32 byte alignment improves shuffle performance
// likely not a big deal as shuffle is already really fast
// NOT necessary since _mm_loadu_si128 does not require alignment
struct ZstdShuffleCompressor {
    ZSTD_CCtx * cctx;
    std::unique_ptr<char[]> shuffleblock;
    ZstdShuffleCompressor() : cctx(ZSTD_createCCtx()), shuffleblock(MAKE_UNIQUE_BLOCK(MAX_BLOCKSIZE)) {}
    ~ZstdShuffleCompressor() {
        ZSTD_freeCCtx(cctx);
    }

    // heuristic whether to use shuffle or not
    // using min compression level, compare compression of the first N bytes with or without shuffle using fast clevel
    // if shuffle improves compression by 7% or more, use shuffle
    bool use_shuffle_heuristic(char * const dst, const uint64_t dstCapacity, const char * const src, const uint64_t srcSize) {
        if(srcSize < SHUFFLE_HEURISTIC_BLOCKSIZE) {
            return false;
        };
        // copy from start of block
        blosc_shuffle(reinterpret_cast<const uint8_t*>(src), reinterpret_cast<uint8_t*>(shuffleblock.get()), SHUFFLE_HEURISTIC_BLOCKSIZE, SHUFFLE_ELEMSIZE);
        uint32_t shuffle_approx    = ZSTD_compressCCtx(cctx, dst, dstCapacity, shuffleblock.get(),
                                                       SHUFFLE_HEURISTIC_BLOCKSIZE, 
                                                       SHUFFLE_HEURISTIC_CLEVEL);
        uint32_t no_shuffle_approx = ZSTD_compressCCtx(cctx, dst, dstCapacity, src,
                                                       SHUFFLE_HEURISTIC_BLOCKSIZE,
                                                       SHUFFLE_HEURISTIC_CLEVEL);

        // copy from half way of block and add to estimate
        if(srcSize >= MAX_BLOCKSIZE/2 + SHUFFLE_HEURISTIC_BLOCKSIZE) {
            blosc_shuffle(reinterpret_cast<const uint8_t*>(src + MAX_BLOCKSIZE/2),
                          reinterpret_cast<uint8_t*>(shuffleblock.get()), SHUFFLE_HEURISTIC_BLOCKSIZE, SHUFFLE_ELEMSIZE);
        shuffle_approx += ZSTD_compressCCtx(cctx, dst, dstCapacity, shuffleblock.get(),
                                            SHUFFLE_HEURISTIC_BLOCKSIZE,
                                            SHUFFLE_HEURISTIC_CLEVEL);
        no_shuffle_approx += ZSTD_compressCCtx(cctx, dst, dstCapacity, src + MAX_BLOCKSIZE/2,
                                               SHUFFLE_HEURISTIC_BLOCKSIZE,
                                               SHUFFLE_HEURISTIC_CLEVEL);
        }
        return (static_cast<float>(no_shuffle_approx) / static_cast<float>(shuffle_approx)) > SHUFFLE_MIN_IMPROVEMENT_THRESHOLD;
    }
    uint64_t compress(char * const dst, const uint64_t dstCapacity,
                      const char * const src, const uint64_t srcSize,
                      int compress_level) {

        if(use_shuffle_heuristic(dst, dstCapacity, src, srcSize)) {
            uint64_t remainder = srcSize % SHUFFLE_ELEMSIZE;
            blosc_shuffle(reinterpret_cast<const uint8_t*>(src), reinterpret_cast<uint8_t*>(shuffleblock.get()), srcSize - remainder, SHUFFLE_ELEMSIZE);
            std::memcpy(shuffleblock.get() + srcSize - remainder, src + srcSize - remainder, remainder);
            uint32_t output_size = ZSTD_compressCCtx(cctx, dst, dstCapacity, shuffleblock.get(), srcSize, compress_level);
            output_size = output_size | SHUFFLE_MASK;
            return output_size;
        } else {
            uint32_t output_size = ZSTD_compressCCtx(cctx, dst, dstCapacity, src, srcSize, compress_level);
            return output_size;
        }
    }
};

struct ZstdDecompressor {
    ZSTD_DCtx * dctx;
    ZstdDecompressor() : dctx(ZSTD_createDCtx()) {}
    ~ZstdDecompressor() {
        ZSTD_freeDCtx(dctx);
    }
    uint64_t decompress(char * const dst, const uint64_t dstCapacity,
                        const char * const src, const uint64_t srcSize) {
        return ZSTD_decompressDCtx(dctx, dst, dstCapacity, src, srcSize);
    }
};

struct ZstdShuffleDecompressor {
    ZSTD_DCtx * dctx;
    std::unique_ptr<char[]> shuffleblock;
    ZstdShuffleDecompressor() : dctx(ZSTD_createDCtx()), shuffleblock(MAKE_UNIQUE_BLOCK(MAX_BLOCKSIZE)) {}
    ~ZstdShuffleDecompressor() {
        ZSTD_freeDCtx(dctx);
    }
    uint64_t decompress(char * const dst, const uint64_t dstCapacity,
                        const char * const src, uint64_t srcSize) { // srcSize modified by shuffle mask, so not const
        bool is_shuffled = srcSize & SHUFFLE_MASK;
        if(is_shuffled) {
            // set shuffle bit to zero, negate shuffle mask
            srcSize = srcSize & (~SHUFFLE_MASK);
            uint64_t output_blocksize = ZSTD_decompressDCtx(dctx, shuffleblock.get(), dstCapacity, src, srcSize);
            uint64_t remainder = output_blocksize % SHUFFLE_ELEMSIZE;
            blosc_unshuffle(reinterpret_cast<uint8_t*>(shuffleblock.get()), reinterpret_cast<uint8_t*>(dst), output_blocksize - remainder, SHUFFLE_ELEMSIZE);
            std::memcpy(dst + output_blocksize - remainder, shuffleblock.get() + output_blocksize - remainder, remainder);
            return output_blocksize;
        } else {
            return ZSTD_decompressDCtx(dctx, dst, dstCapacity, src, srcSize);
        }

    }
};

template <class stream_writer, class compressor>
struct BlockCompressWriter {
    stream_writer & myFile;
    compressor cp;
    std::unique_ptr<char[]> block;
    std::unique_ptr<char[]> zblock;
    uint64_t current_blocksize;
    const int compress_level;
    BlockCompressWriter(stream_writer & f, int compress_level) : 
        myFile(f),
        cp(),
        block(MAKE_UNIQUE_BLOCK(MAX_BLOCKSIZE)),
        zblock(MAKE_UNIQUE_BLOCK(MAX_ZBLOCKSIZE)),
        current_blocksize(0),
        compress_level(compress_level) {}
    void flush() {
        if(current_blocksize > 0) {
            uint64_t zsize = cp.compress(zblock.get(), MAX_ZBLOCKSIZE, block.get(), current_blocksize, compress_level);
            myFile.writeInteger(static_cast<uint32_t>(zsize));
            // zsize contains metadata, filter it out to get size of write
            myFile.write(zblock.get(), zsize & (~BLOCK_METADATA));
            current_blocksize = 0;
        }
    }
    void finish() {
        flush();
    }
    void push_data(const char * const inbuffer, const uint64_t len) {
        uint64_t current_pointer_consumed = 0;
        while(current_pointer_consumed < len) {
            if(current_blocksize >= MAX_BLOCKSIZE) { flush(); }
            
            if(current_blocksize == 0 && len - current_pointer_consumed >= MAX_BLOCKSIZE) {
                uint64_t zsize = cp.compress(zblock.get(), MAX_ZBLOCKSIZE, inbuffer + current_pointer_consumed, MAX_BLOCKSIZE, compress_level);
                myFile.writeInteger(static_cast<uint32_t>(zsize));
                // zsize contains metadata, filter it out to get size of write
                myFile.write(zblock.get(), zsize & (~BLOCK_METADATA));
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
        if(current_blocksize >= MIN_BLOCKSIZE) { flush(); }
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

template <class stream_reader, class decompressor> 
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
            throw std::runtime_error("Unexpected end of file while reading next block size");
        }
        size_t bytes_read = myFile.read(zblock.get(), zsize & (~BLOCK_METADATA));
        if(bytes_read != (zsize & (~BLOCK_METADATA))) {
            throw std::runtime_error("Unexpected end of file while reading next block");
        }
        current_blocksize = dp.decompress(block.get(), MAX_BLOCKSIZE, zblock.get(), zsize);
        data_offset = 0;
        TOUT("blocksize ", current_blocksize);
    }
    void decompress_direct(char * outbuffer) {
        uint32_t zsize;
        bool ok = myFile.readInteger(zsize);
        TOUT("decompress_direct ", zsize);
        if(!ok) {
            throw std::runtime_error("Unexpected end of file while reading next block size");
        }
        size_t bytes_read = myFile.read(zblock.get(), zsize & (~BLOCK_METADATA));
        if(bytes_read != (zsize & (~BLOCK_METADATA))) {
            throw std::runtime_error("Unexpected end of file while reading next block");
        }
        current_blocksize = dp.decompress(outbuffer, MAX_BLOCKSIZE, zblock.get(), zsize);
        TOUT("blocksize ", current_blocksize);
    }
    public:
    void finish() {
        // nothing
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
                    bytes_accounted += MAX_BLOCKSIZE;
                    data_offset = current_blocksize;
                } else {
                    decompress_block();
                    std::memcpy(outbuffer + bytes_accounted, block.get(), len - bytes_accounted);
                    data_offset = len - bytes_accounted;
                    bytes_accounted += data_offset;
                }
            }
        }
    }

    // a POD shall not be split across block boundaries
    // But data_offset may be at the end of a block, so we still need to check
    template<typename POD> POD get_pod() {
        if(current_blocksize == data_offset) {
            decompress_block();
        }
        POD pod;
        memcpy(&pod, block.get()+data_offset, sizeof(POD));
        data_offset += sizeof(POD);
        // TOUT("get_pod ", (int)pod, " ", sizeof(POD));
        return pod;
    }

    // unconditionally read from block without checking remaining length
    template<typename POD> POD get_pod_contiguous() {
        POD pod;
        memcpy(&pod, block.get()+data_offset, sizeof(POD));
        data_offset += sizeof(POD);
        // TOUT("get_pod_contiguous ", (int)pod, " ", sizeof(POD));
        return pod;
    }
};

///////////////////////////////////////////////////////////////////////////////
// Multi-threaded algorithms

#include <tbb/concurrent_queue.h>
#include <tbb/flow_graph.h>
#include <tbb/enumerable_thread_specific.h>
#include <atomic>

struct OrderedBlock {
    std::shared_ptr<char[]> block;
    uint64_t blocksize;
    uint64_t blocknumber;
    OrderedBlock(std::shared_ptr<char[]> block, uint64_t blocksize, uint64_t blocknumber) : 
    block(block), blocksize(blocksize), blocknumber(blocknumber) {}
    OrderedBlock() : block(), blocksize(0), blocknumber(0) {}
};

// sequencer requires copy constructor for message, which means using shared_ptr
// would be better if we could use unique_ptr
// nthreads must be >= 2
template <class stream_writer, class compressor>
struct BlockCompressWriterMT {
    // template class objects
    stream_writer & myFile;
    tbb::enumerable_thread_specific<compressor> cp;
    int compress_level;
    
    // intermediate data objects
    tbb::concurrent_queue<std::shared_ptr<char[]>> available_blocks;
    tbb::concurrent_queue<std::shared_ptr<char[]>> available_zblocks;

    // current data block
    std::shared_ptr<char[]> current_block;
    uint64_t current_blocksize;
    uint64_t current_blocknumber;

    // flow graph
    tbb::flow::graph myGraph;
    tbb::flow::function_node<OrderedBlock, OrderedBlock> compressor_node;
    tbb::flow::sequencer_node<OrderedBlock> sequencer_node;
    tbb::flow::function_node<OrderedBlock, int, tbb::flow::rejecting> writer_node;
    BlockCompressWriterMT(stream_writer & f, int cl, int nthreads) :
    // template class objects
    myFile(f),
    cp(), // each thread specific compressor should be default constructed
    compress_level(cl),

    // intermediate data objects
    available_blocks(),
    available_zblocks(),

    // current data block
    current_block(MAKE_SHARED_BLOCK(MAX_BLOCKSIZE)),
    current_blocksize(0),
    current_blocknumber(0),

    // flow graph
    myGraph(),
    compressor_node(tbb::flow::function_node<OrderedBlock, OrderedBlock>(this->myGraph, tbb::flow::unlimited, 
        [this](OrderedBlock block) {
                        // get zblock from available_zblocks
            OrderedBlock zblock;
            if(!available_zblocks.try_pop(zblock.block)) {
                zblock.block = MAKE_SHARED_BLOCK_ASSIGNMENT(MAX_ZBLOCKSIZE);
            }
            // do thread local compression
            typename tbb::enumerable_thread_specific<compressor>::reference cp_local = cp.local();
            zblock.blocksize = cp_local.compress(zblock.block.get(), MAX_ZBLOCKSIZE, 
                                                 block.block.get(), block.blocksize,
                                                 compress_level);
            zblock.blocknumber = block.blocknumber;
            
            // return input block to available_blocks
            available_blocks.push(block.block);
            return zblock;
        })),
    sequencer_node(tbb::flow::sequencer_node<OrderedBlock>(this->myGraph,
        [](OrderedBlock zblock) {
            return zblock.blocknumber;
        })),
    writer_node(tbb::flow::function_node<OrderedBlock, int, tbb::flow::rejecting>(this->myGraph, tbb::flow::serial,
        [this](OrderedBlock zblock) {
                        myFile.writeInteger(static_cast<uint32_t>(zblock.blocksize));
            myFile.write(zblock.block.get(), zblock.blocksize & (~BLOCK_METADATA));

            // return input zblock to available_zblocks
            available_zblocks.push(zblock.block);

            // return 0 to indicate success
            return 0;
        })) {

        // pre-populate available blocks and zblocks queues
        // for(int i = 0; i < nthreads-1; ++i) {
        //     available_blocks.push(MAKE_SHARED_BLOCK_ASSIGNMENT(MAX_BLOCKSIZE));
        //     available_zblocks.push(MAKE_SHARED_BLOCK_ASSIGNMENT(MAX_ZBLOCKSIZE));
        // }
        
        // connect computation graph
        tbb::flow::make_edge(compressor_node, sequencer_node);
        tbb::flow::make_edge(sequencer_node, writer_node);
    }

    void flush() {
                if(current_blocksize > 0) {
            compressor_node.try_put(OrderedBlock(current_block, current_blocksize, current_blocknumber));
            current_blocknumber++;
            current_blocksize = 0;
            // replace current_block with available block
            if(!available_blocks.try_pop(current_block)) {
                current_block = MAKE_SHARED_BLOCK_ASSIGNMENT(MAX_BLOCKSIZE);
            }
        }
    }

    void finish() {
        flush();
        myGraph.wait_for_all();
    }

    void push_data(const char * const inbuffer, const uint64_t len) {
                uint64_t current_pointer_consumed = 0;
        while(current_pointer_consumed < len) {
            if(current_blocksize >= MAX_BLOCKSIZE) { flush(); }
            
            if(current_blocksize == 0 && len - current_pointer_consumed >= MAX_BLOCKSIZE) {
                // Different from ST, memcpy segment of inbuffer to block and then send to compress_node
                std::memcpy(current_block.get(), inbuffer + current_pointer_consumed, MAX_BLOCKSIZE);
                compressor_node.try_put(OrderedBlock(current_block, MAX_BLOCKSIZE, current_blocknumber));
                current_blocknumber++;
                current_pointer_consumed += MAX_BLOCKSIZE;
                current_blocksize = 0;
                // replace current_block with available block
                if(!available_blocks.try_pop(current_block)) {
                    current_block = MAKE_SHARED_BLOCK_ASSIGNMENT(MAX_BLOCKSIZE);
                }
            } else {
                uint64_t remaining_pointer_available = len - current_pointer_consumed;
                uint64_t add_length = remaining_pointer_available < (MAX_BLOCKSIZE - current_blocksize) ? remaining_pointer_available : MAX_BLOCKSIZE-current_blocksize;
                                std::memcpy(current_block.get() + current_blocksize, inbuffer + current_pointer_consumed, add_length);
                current_blocksize += add_length;
                current_pointer_consumed += add_length;
            }
        }
    }

    // same as ST
    template<typename POD> void push_pod(const POD pod) {
        if(current_blocksize >= MIN_BLOCKSIZE) { flush(); }
        const char * ptr = reinterpret_cast<const char*>(&pod);
        std::memcpy(current_block.get() + current_blocksize, ptr, sizeof(POD));
        current_blocksize += sizeof(POD);
    }

    // same as ST
    template<typename POD> void push_pod_contiguous(const POD pod) {
        const char * ptr = reinterpret_cast<const char*>(&pod);
        memcpy(current_block.get() + current_blocksize, ptr, sizeof(POD));
        current_blocksize += sizeof(POD);
    }

    // runtime determination of contiguous
    template<typename POD> void push_pod(const POD pod, const bool contiguous) {
        if(contiguous) { push_pod_contiguous(pod); } else { push_pod(pod); }
    }
};

template <class stream_reader, class decompressor> 
struct BlockCompressReaderMT {
    // template class objects
    stream_reader & myFile;
    tbb::enumerable_thread_specific<decompressor> dp;
    decompressor dp_main; // decompressor for main thread to moonlight as compressor_node

    // intermediate data objects
    tbb::concurrent_queue<std::shared_ptr<char[]>> available_zblocks;
    tbb::concurrent_queue<std::shared_ptr<char[]>> available_blocks;
    tbb::concurrent_queue<OrderedBlock> ordered_zblocks;

    // current data block
    std::shared_ptr<char[]> current_block;
    uint64_t current_blocksize;
    uint64_t data_offset;

    // global control objects
    std::atomic<bool> end_of_file; // set after blocks_to_process and there are insufficient bytes from reader stream for another block
    std::atomic<uint64_t> blocks_to_process;
    uint64_t blocks_processed; // only written and read by main thread

    // flow graph
    tbb::flow::graph myGraph;
    tbb::flow::source_node<tbb::flow::continue_msg> reader_node;
    tbb::flow::function_node<tbb::flow::continue_msg, int> decompressor_node;
    tbb::flow::sequencer_node<OrderedBlock> sequencer_node;

    BlockCompressReaderMT(stream_reader & f, int nthreads) :
    // template class objects
    myFile(f),
    dp(),
    dp_main(),

    // intermediate data objects
    available_zblocks(),
    available_blocks(),
    ordered_zblocks(),

    // current data block
    current_block(MAKE_SHARED_BLOCK(MAX_BLOCKSIZE)),
    current_blocksize(0), 
    data_offset(0),

    // global control objects
    end_of_file(false),
    blocks_to_process(0),
    blocks_processed(0),
    
    // flow graph
    myGraph(),
    reader_node(tbb::flow::source_node<tbb::flow::continue_msg>(this->myGraph,
    [this](tbb::flow::continue_msg & cont) {
        // read size of next zblock. if insufficient bytes read into uint32_t, end of file
        uint32_t zsize;
        bool ok = this->myFile.readInteger(zsize);
        if(!ok) {
            end_of_file.store(true);
            return false;
        }

        // get zblock from available_zblocks or make new
        OrderedBlock zblock;
        if(!available_zblocks.try_pop(zblock.block)) {
            zblock.block = MAKE_SHARED_BLOCK_ASSIGNMENT(MAX_ZBLOCKSIZE);
        }

        // read zblock. if bytes_read is less than zsize, end of file
        size_t bytes_read = this->myFile.read(zblock.block.get(), zsize & (~BLOCK_METADATA));
        if(bytes_read != (zsize & (~BLOCK_METADATA))) {
            end_of_file.store(true);
            return false;
        }

        // set zblock size and block number and push to ordered_zblocks queue
        // Also incremenet blocks_to_process BEFORE zblock is added to ordered_zblocks
        zblock.blocksize = zsize;
        zblock.blocknumber = blocks_to_process.fetch_add(1);

        TOUT("read zblock ", zblock.blocknumber, " with size ", zblock.blocksize);

        this->ordered_zblocks.push(zblock);

        // return continue token to decompressor node
        cont = tbb::flow::continue_msg{};
        return true;
    }, false)),
    decompressor_node(tbb::flow::function_node<tbb::flow::continue_msg, int>(this->myGraph, tbb::flow::unlimited,
    [this](tbb::flow::continue_msg cont) {
        typename tbb::enumerable_thread_specific<decompressor>::reference dp_local = dp.local();
        // decompress routine manually try_put's a block to sequencer_node, return value is just a dummy 0 integer and does nothing
        return this->decompressor_node_routine(dp_local);
    })),
    sequencer_node(tbb::flow::sequencer_node<OrderedBlock>(this->myGraph, 
    [](const OrderedBlock & block) {
        TOUT("sequencer ", block.blocknumber, " with size ", block.blocksize);
        return block.blocknumber; 
    })) {
        // pre-populate available blocks and zblocks queues
        // for(int i = 0; i < nthreads-1; ++i) {
        //     available_blocks.push(MAKE_SHARED_BLOCK_ASSIGNMENT(MAX_BLOCKSIZE));
        //     available_zblocks.push(MAKE_SHARED_BLOCK_ASSIGNMENT(MAX_ZBLOCKSIZE));
        // }
        
        // connect computation graph
        tbb::flow::make_edge(reader_node, decompressor_node);
        reader_node.activate();
        TOUT("activated");
    }
    private:
    int decompressor_node_routine(decompressor & dp_local) {
        TOUT("decompressor ");
        // try_pop from ordered_zblocks queue
        OrderedBlock zblock;
        if(!ordered_zblocks.try_pop(zblock)) {
            return 0; // no available zblock, could be stolen by main thread
        }
        TOUT("decompressor ", zblock.blocknumber, " with size ", zblock.blocksize);
        
        // get available block and decompress
        OrderedBlock block;
        if(!available_blocks.try_pop(block.block)) {
            block.block = MAKE_SHARED_BLOCK_ASSIGNMENT(MAX_BLOCKSIZE);
        }
        block.blocksize = dp_local.decompress(block.block.get(), MAX_BLOCKSIZE, zblock.block.get(), zblock.blocksize);
        block.blocknumber = zblock.blocknumber;

        // return zblock to available_zblocks queue
        available_zblocks.push(zblock.block);

        // push to sequencer queue
        this->sequencer_node.try_put(block);
        return 0;
    }
    void get_new_block() {
        TOUT("get_new_block ");
        OrderedBlock block;
        while( true ) {
            // try_get from sequencer queue until a new block is available
            if( sequencer_node.try_get(block) ) {
                                // put old block back in available_blocks
                available_blocks.push(current_block);

                // replace previous block with new block and increment blocks_processed
                current_block = block.block;
                current_blocksize = block.blocksize;
                data_offset = 0;
                blocks_processed += 1;
                return;
            } else {
                // moonlight by trying to run decompression node routine
                decompressor_node_routine(dp_main);
            }

            // check if end_of_file and blocks_used >= blocks_to_process
            // which means the file unexpectedly ended, because we are still expecting blocks
            if(end_of_file && blocks_processed >= blocks_to_process) {
                throw std::runtime_error("Unexpected end of file");
            }
        }
    }
    public:

    void finish() {
        myGraph.wait_for_all();
    }

    void get_data(char * outbuffer, const uint64_t len) {
                if(current_blocksize - data_offset >= len) {
            std::memcpy(outbuffer, current_block.get()+data_offset, len);
            data_offset += len;
        } else {
            uint64_t bytes_accounted = current_blocksize - data_offset;
            std::memcpy(outbuffer, current_block.get()+data_offset, bytes_accounted);
            while(bytes_accounted < len) {
                if(len - bytes_accounted >= MAX_BLOCKSIZE) {
                    // different from ST, do not directly decompress
                    // get a new block and memcopy
                    get_new_block();
                    std::memcpy(outbuffer + bytes_accounted, current_block.get(), MAX_BLOCKSIZE);
                    bytes_accounted += MAX_BLOCKSIZE;
                    data_offset = current_blocksize;
                } else {
                    // same as ST
                    get_new_block();
                    std::memcpy(outbuffer + bytes_accounted, current_block.get(), len - bytes_accounted);
                    data_offset = len - bytes_accounted;
                    bytes_accounted += data_offset;
                }
            }
        }
    }

    // Same as ST
    // a POD shall not be split across block boundaries
    // But data_offset may be at the end of a block, so we still need to check
    template<typename POD> POD get_pod() {
        if(current_blocksize == data_offset) {
            get_new_block();
        }
        POD pod;
        memcpy(&pod, current_block.get()+data_offset, sizeof(POD));
        data_offset += sizeof(POD);
        return pod;
    }

    // same as ST
    // unconditionally read from block without checking remaining length
    template<typename POD> POD get_pod_contiguous() {
        POD pod;
        memcpy(&pod, current_block.get()+data_offset, sizeof(POD));
        data_offset += sizeof(POD);
        return pod;
    }
};

#endif
