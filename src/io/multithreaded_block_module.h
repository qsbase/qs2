#ifndef _QS2_MULTITHREADED_BLOCK_MODULE_H
#define _QS2_MULTITHREADED_BLOCK_MODULE_H

#include "io/io_common.h"

#include <tbb/concurrent_queue.h>
#include <tbb/concurrent_vector.h>
#include <tbb/flow_graph.h>
#include <tbb/enumerable_thread_specific.h>
#include <atomic>

// single argument macro
#define _SA_(...) __VA_ARGS__

#if __cplusplus >= 201703L
#define SUPPORTS_IF_CONSTEXPR 1
#define DIRECT_MEM_SWITCH(if_true, if_false) \
    if constexpr (direct_mem) {                         \
        if_true;                                        \
    } else {                                            \
        if_false;                                       \
    }
#else
#define SUPPORTS_IF_CONSTEXPR 0
#define DIRECT_MEM_SWITCH(if_true, if_false) if_true;
#endif

struct OrderedBlock {
    std::shared_ptr<char[]> block;
    uint64_t blocksize;
    uint64_t blocknumber;
    OrderedBlock(std::shared_ptr<char[]> block, uint64_t blocksize, uint64_t blocknumber) : 
    block(block), blocksize(blocksize), blocknumber(blocknumber) {}
    OrderedBlock() : block(), blocksize(0), blocknumber(0) {}
};

struct OrderedPtr {
    const char * block;
    uint64_t blocknumber;
    OrderedPtr(const char * block, uint64_t blocknumber) : 
    block(block), blocknumber(blocknumber) {}
    OrderedPtr() : block(), blocknumber(0) {}
};

// sequencer requires copy constructor for message, which means using shared_ptr
// would be better if we could use unique_ptr
// nthreads must be >= 2
template <class stream_writer, class compressor, class hasher, ErrorType E, bool direct_mem>
struct BlockCompressWriterMT {
    // template class objects
    stream_writer & myFile;
    tbb::enumerable_thread_specific<compressor> cp;
    hasher hp; // must be serial
    const int compress_level;
    
    // intermediate data objects
    tbb::concurrent_queue<std::shared_ptr<char[]>> available_blocks;
    tbb::concurrent_queue<std::shared_ptr<char[]>> available_zblocks;

    // current data block
    std::shared_ptr<char[]> current_block;
    uint64_t current_blocksize;
    uint64_t current_blocknumber;

    // flow graph
    tbb::task_group_context tgc;
    tbb::flow::graph myGraph;
    tbb::flow::function_node<OrderedBlock, OrderedBlock> compressor_node;
    tbb::flow::function_node<OrderedPtr, OrderedBlock> compressor_node_direct;
    tbb::flow::sequencer_node<OrderedBlock> sequencer_node;
    tbb::flow::function_node<OrderedBlock, int, tbb::flow::rejecting> writer_node;
    BlockCompressWriterMT(stream_writer & f, const int cl) :
    // template class objects
    myFile(f),
    cp(), // each thread specific compressor should be default constructed
    hp(),
    compress_level(cl),

    // intermediate data objects
    available_blocks(),
    available_zblocks(),

    // current data block
    current_block(MAKE_SHARED_BLOCK(MAX_BLOCKSIZE)),
    current_blocksize(0),
    current_blocknumber(0),

    // flow graph
    tgc(),
    myGraph(this->tgc),
    compressor_node(this->myGraph, tbb::flow::unlimited, 
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
    }),
    compressor_node_direct(this->myGraph, tbb::flow::unlimited, 
    [this](OrderedPtr ptr) {
        // get zblock from available_zblocks
        OrderedBlock zblock;
        if(!available_zblocks.try_pop(zblock.block)) {
            zblock.block = MAKE_SHARED_BLOCK_ASSIGNMENT(MAX_ZBLOCKSIZE);
        }
        // do thread local compression
        typename tbb::enumerable_thread_specific<compressor>::reference cp_local = cp.local();
        zblock.blocksize = cp_local.compress(zblock.block.get(), MAX_ZBLOCKSIZE, 
                                                ptr.block, MAX_BLOCKSIZE,
                                                compress_level);
        zblock.blocknumber = ptr.blocknumber;
        return zblock;
    }),
    sequencer_node(this->myGraph,
    [](const OrderedBlock & zblock) {
        return zblock.blocknumber;
    }),
    writer_node(this->myGraph, tbb::flow::serial,
    [this](OrderedBlock zblock) {
        write_and_update(static_cast<uint32_t>(zblock.blocksize));
        write_and_update(zblock.block.get(), zblock.blocksize & (~BLOCK_METADATA));

        // return input zblock to available_zblocks
        available_zblocks.push(zblock.block);

        // return 0 to indicate success
        return 0;
    })
    {
        // connect computation graph
        tbb::flow::make_edge(compressor_node, sequencer_node);
        DIRECT_MEM_SWITCH(
            _SA_(
                tbb::flow::make_edge(compressor_node_direct, sequencer_node);
            ),
            _SA_(
                // compressor_node_direct not connected
            )
        )
        tbb::flow::make_edge(sequencer_node, writer_node);
    }
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
            compressor_node.try_put(OrderedBlock(current_block, current_blocksize, current_blocknumber));
            current_blocknumber++;
            current_blocksize = 0;
            // replace current_block with available block
            if(!available_blocks.try_pop(current_block)) {
                current_block = MAKE_SHARED_BLOCK_ASSIGNMENT(MAX_BLOCKSIZE);
            }
        }
    }
    
    public:
    uint64_t finish() {
        flush();
        myGraph.wait_for_all();
        return hp.digest();
    }
    void cleanup() {
        if(! tgc.is_group_execution_cancelled()) {
            tgc.cancel_group_execution();
        }
        myGraph.wait_for_all();
    }
    void cleanup_and_throw(std::string msg) {
        cleanup();
        throw_error<E>(msg.c_str());
    }

    void push_data(const char * const inbuffer, const uint64_t len) {
        uint64_t current_pointer_consumed = 0;

        // if data exists in current_block, then append to it first
        if(current_blocksize >= MAX_BLOCKSIZE) { flush(); }
        if(current_blocksize > 0) {
            // append the minimum between remaining_len and remaining_block_space
            uint64_t add_length = std::min(len - current_pointer_consumed, MAX_BLOCKSIZE - current_blocksize);
            std::memcpy(current_block.get() + current_blocksize, inbuffer + current_pointer_consumed, add_length);
            current_blocksize += add_length;
            current_pointer_consumed += add_length;
        }

        // after appending to non-empty block any additional data push assumes that current_blocksize is zero
        // True bc either inbuffer was fully consumed already (no more data to push) or block was filled and flushed (sets current_blocksize to zero)
        if(current_blocksize >= MAX_BLOCKSIZE) { flush(); }
        while(len - current_pointer_consumed >= MAX_BLOCKSIZE) {
            DIRECT_MEM_SWITCH(
                _SA_(
                    compressor_node_direct.try_put(OrderedPtr(inbuffer + current_pointer_consumed, current_blocknumber));
                ),
                _SA_(
                    // Different from ST, memcpy segment of inbuffer to block and then send to compress_node
                    std::shared_ptr<char[]> full_block;
                    if(!available_blocks.try_pop(full_block)) {
                        full_block = MAKE_SHARED_BLOCK_ASSIGNMENT(MAX_BLOCKSIZE);
                    }
                    std::memcpy(full_block.get(), inbuffer + current_pointer_consumed, MAX_BLOCKSIZE);
                    compressor_node.try_put(OrderedBlock(full_block, MAX_BLOCKSIZE, current_blocknumber));
                )
            )
            current_blocknumber++;
            // current_blocksize = 0; // If we are in this loop, current_blocksize is already zero
            current_pointer_consumed += MAX_BLOCKSIZE;
        }

        // check if there is any remaining_len after sending full blocks
        if(len - current_pointer_consumed > 0) {
            uint64_t add_length = len - current_pointer_consumed;
            std::memcpy(current_block.get(), inbuffer + current_pointer_consumed, add_length);
            current_blocksize = add_length;
            // current_pointer_consumed += add_length; // unnecessary
        }
    }

    // same as ST
    template<typename POD> void push_pod(const POD pod) {
        if(current_blocksize > MIN_BLOCKSIZE) { flush(); }
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

template <class stream_reader, class decompressor, ErrorType E> 
struct BlockCompressReaderMT {
    // template class objects
    stream_reader & myFile;
    tbb::enumerable_thread_specific<decompressor> dp;

    // intermediate data objects
    tbb::concurrent_queue<std::shared_ptr<char[]>> available_zblocks;
    tbb::concurrent_queue<std::shared_ptr<char[]>> available_blocks;

    // current data block
    std::shared_ptr<char[]> current_block;
    uint64_t current_blocksize;
    uint64_t data_offset;

    // global control objects
    std::atomic<bool> end_of_file; // set after blocks_to_process and there are insufficient bytes from reader stream for another block
    std::atomic<uint64_t> blocks_to_process;
    uint64_t blocks_processed; // only written and read by main thread

    // flow graph
    tbb::task_group_context tgc;
    tbb::flow::graph myGraph;
    tbb::flow::source_node<OrderedBlock> reader_node;
    tbb::flow::function_node<OrderedBlock, OrderedBlock> decompressor_node;
    tbb::flow::sequencer_node<OrderedBlock> sequencer_node;

    BlockCompressReaderMT(stream_reader & f) :
    // template class objects
    myFile(f),
    dp(),

    // intermediate data objects
    available_zblocks(),
    available_blocks(),

    // current data block
    current_block(MAKE_SHARED_BLOCK(MAX_BLOCKSIZE)),
    current_blocksize(0), 
    data_offset(0),

    // global control objects
    end_of_file(false),
    blocks_to_process(0),
    blocks_processed(0),
    
    // flow graph
    tgc(),
    myGraph(this->tgc),
    reader_node(this->myGraph,
    [this](OrderedBlock & zblock) {
        // read size of next zblock. if insufficient bytes read into uint32_t, end of file
        uint32_t zsize;
        bool ok = this->myFile.readInteger(zsize);
        if(!ok) {
            end_of_file.store(true);
            return false;
        }

        // get zblock from available_zblocks or make new
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
        return true;
    }, false),
    decompressor_node(this->myGraph, tbb::flow::unlimited,
    [this](OrderedBlock zblock) {
        typename tbb::enumerable_thread_specific<decompressor>::reference dp_local = dp.local();

        // get available block and decompress
        OrderedBlock block;
        if(!available_blocks.try_pop(block.block)) {
            block.block = MAKE_SHARED_BLOCK_ASSIGNMENT(MAX_BLOCKSIZE);
        }
        block.blocksize = dp_local.decompress(block.block.get(), MAX_BLOCKSIZE, zblock.block.get(), zblock.blocksize);
        if(decompressor::is_error(block.blocksize)) {
            // don't throw error within graph
            // main thread will check for cancellelation and throw
            tgc.cancel_group_execution();
            return block;
        }
        block.blocknumber = zblock.blocknumber;

        // return zblock to available_zblocks queue
        available_zblocks.push(zblock.block);

        return block;
    }),
    sequencer_node(this->myGraph, 
    [](const OrderedBlock & block) {
        return block.blocknumber; 
    })
    {
        // connect computation graph
        tbb::flow::make_edge(reader_node, decompressor_node);
        tbb::flow::make_edge(decompressor_node, sequencer_node);
        reader_node.activate();
    }
    private:
    void get_new_block() {
        OrderedBlock block;
        while( true ) {
            // try_get from sequencer queue until a new block is available
            if( sequencer_node.try_get(block) ) {
                // put old block back in available_blocks
                available_blocks.push(current_block);

                // replace previous block with new block and increment blocks_processed
                current_block = block.block;
                current_blocksize = block.blocksize;
                blocks_processed += 1;
                return;
            }

            // check if end_of_file and blocks_used >= blocks_to_process
            // which means the file unexpectedly ended, because we are still expecting blocks
            if(end_of_file && blocks_processed >= blocks_to_process) {
                cleanup_and_throw("Unexpected end of file");
            }
            if(tgc.is_group_execution_cancelled()) {
                cleanup_and_throw("File read / decompression error");
            }
        }
    }
    public:

    void finish() {
        myGraph.wait_for_all();
    }
    void cleanup() {
        if(! tgc.is_group_execution_cancelled()) {
            tgc.cancel_group_execution();
        }
        myGraph.wait_for_all();
    }
    void cleanup_and_throw(std::string msg) {
        cleanup();
        throw_error<E>(msg.c_str());
    }

    void get_data(char * outbuffer, const uint64_t len) {
        if(current_blocksize - data_offset >= len) {
            std::memcpy(outbuffer, current_block.get()+data_offset, len);
            data_offset += len;
        } else {
            // remainder of current block, may be zero
            uint64_t bytes_accounted = current_blocksize - data_offset;
            std::memcpy(outbuffer, current_block.get()+data_offset, bytes_accounted);
            while(len - bytes_accounted >= MAX_BLOCKSIZE) {
                // different from ST, do not directly decompress
                // get a new block and memcopy
                get_new_block();
                std::memcpy(outbuffer + bytes_accounted, current_block.get(), current_blocksize);
                bytes_accounted += MAX_BLOCKSIZE;
                data_offset = MAX_BLOCKSIZE;
            }
            if(len - bytes_accounted > 0) { // but less than MAX_BLOCKSIZE
                get_new_block();
                // if not enough bytes in block then something went wrong, throw error
                if(current_blocksize < len - bytes_accounted) {
                    cleanup_and_throw("Corrupted block data");
                }
                std::memcpy(outbuffer + bytes_accounted, current_block.get(), len - bytes_accounted);
                data_offset = len - bytes_accounted;
                // bytes_accounted += data_offset; // no need to update since we are returning
            }
        }
    }

    const char * get_ptr(const uint64_t len) {
        if(current_blocksize - data_offset >= len) {
            const char * ptr = current_block.get() + data_offset;
            data_offset += len;
            return ptr;
        } else {
            // return nullptr, indicating len exceeds current block
            // copy data using get_data
            return nullptr;
        }
    }

    // Same as ST
    template<typename POD> POD get_pod() {
        if(current_blocksize == data_offset) {
            get_new_block();
            data_offset = 0;
        }
        if(current_blocksize - data_offset < sizeof(POD)) {
            cleanup_and_throw("Corrupted block data");
        }
        POD pod;
        memcpy(&pod, current_block.get()+data_offset, sizeof(POD));
        data_offset += sizeof(POD);
        return pod;
    }

    // same as ST
    // unconditionally read from block without checking remaining length
    template<typename POD> POD get_pod_contiguous() {
        if(current_blocksize - data_offset < sizeof(POD)) {
            cleanup_and_throw("Corrupted block data");
        }
        POD pod;
        memcpy(&pod, current_block.get()+data_offset, sizeof(POD));
        data_offset += sizeof(POD);
        return pod;
    }
};


// In contrast to BlockCompressReaderMT, BlockCompressReaderHashMT calculates hash of all input data before allowing deserialization
// this is done in the constructor, so that there is no difference in interface
template <class stream_reader, class decompressor, class hasher, ErrorType E>
struct BlockCompressReaderHashMT {
    // template class objects
    stream_reader & myFile;
    tbb::enumerable_thread_specific<decompressor> dp;
    hasher hp; // main thread only

    // intermediate data objects
    tbb::concurrent_queue<std::shared_ptr<char[]>> available_zblocks;
    tbb::concurrent_vector<std::pair< std::shared_ptr<char[]>, uint32_t> > block_vector;

    // global control objects
    uint64_t blocks_processed; // only written and read by main thread; reset to zero after reading in all data first

    // use task group instead of flow graph, since were only multithreading decompression
    // tg has an internal task_group_context
    // https://github.com/RcppCore/RcppParallel/blob/8c000aace9ff0ed48a641b81340b1c25b73ff091/src/tbb/include/tbb/task_group.h#L171
    // is_canceling() is equivalent to tgc.is_group_execution_cancelled()
    // cancel() is equivalent to tgc.cancel_group_execution()
    tbb::task_group tg;

    // current data block
    const char * current_block;
    uint64_t current_blocksize;
    uint64_t data_offset;

    BlockCompressReaderHashMT(stream_reader & f, const uint64_t stored_hash) :
    // template class objects
    myFile(f),
    dp(),
    hp(),

    // intermediate data objects
    available_zblocks(),
    block_vector(),

    // global control objects
    blocks_processed(0),

    tg(),

    // current data block
    current_block(nullptr),
    current_blocksize(0), 
    data_offset(0)
    {
        while(true) {
            if(tg.is_canceling()) {
                cleanup_and_throw("File read / decompression error");
            }
            uint32_t zsize;
            bool ok = myFile.readInteger(zsize);
            if(!ok) {
                break; // end of file, is there a better way to check EOF and break out of this loop?
            }
            // get zblock from available_zblocks
            OrderedBlock zblock;
            if(!available_zblocks.try_pop(zblock.block)) {
                zblock.block = MAKE_SHARED_BLOCK_ASSIGNMENT(MAX_ZBLOCKSIZE);
            }
            uint32_t bytes_to_read = zsize & (~BLOCK_METADATA);
            uint32_t bytes_read = myFile.read(zblock.block.get(), bytes_to_read);
            if(bytes_read != bytes_to_read) {
                cleanup_and_throw("Unexpected end of file while reading next block");
            }
            hp.update(zsize);
            hp.update(zblock.block.get(), bytes_read);

            zblock.blocksize = zsize;
            zblock.blocknumber = blocks_processed;
            blocks_processed++;
            
            // Add decompression_node_routine task to task_group
            // zblock must be passed by value since it goes out of scope at the end of this iteration
            tg.run([this, zblock]() {
                decompressor_node_routine(zblock);
            });
        }
        // after while loop reads in all data the only possible error is a decompression error, which is signalled by group cancel
        tg.wait();
        if(hp.digest() != stored_hash) {
            cleanup_and_throw("Hash mismatch, file may be incomplete or corrupted");
        }
        if(tg.is_canceling()) {
            cleanup_and_throw("File read / decompression error");
        }
        blocks_processed = 0; // reset to zero, will be re-used for deserialization
    }
    private:
    int decompressor_node_routine(OrderedBlock zblock) {
        typename tbb::enumerable_thread_specific<decompressor>::reference dp_local = dp.local();
        std::unique_ptr<char[]> block(MAKE_UNIQUE_BLOCK(MAX_BLOCKSIZE));
        uint32_t blocksize = dp_local.decompress(block.get(), MAX_BLOCKSIZE, zblock.block.get(), zblock.blocksize);
        if(decompressor::is_error(blocksize)) {
            // don't throw error within graph, main thread will check for cancellelation and throw
            tg.cancel();
            return -1;
        }

        // return zblock to available_zblocks queue
        available_zblocks.push(zblock.block);

        // Add block to block_vector at blocknumber
        block_vector.grow_to_at_least(zblock.blocknumber + 1);
        block_vector[zblock.blocknumber] = std::pair<std::unique_ptr<char[]>, uint32_t>(std::move(block), blocksize);
        return 0;
    }
    void decompress_block() {
        if(blocks_processed >= block_vector.size()) {
            cleanup_and_throw("Unexpected end of data");
        }
        // get next block
        current_block = block_vector[blocks_processed].first.get();
        current_blocksize = block_vector[blocks_processed].second;
        // clear memory of previous block if blocks_processed > 0
        if(blocks_processed > 0) {
            block_vector[blocks_processed-1].first.reset();
        }
        blocks_processed++;
    }
    void decompress_direct(char * outbuffer) {
        // memcpy block directly into outbuffer
        if(blocks_processed >= block_vector.size()) {
            cleanup_and_throw("Unexpected end of data");
        }
        std::memcpy(outbuffer, block_vector[blocks_processed].first.get(), MAX_BLOCKSIZE);
        // clear memory of previous block if blocks_processed > 0
        if(blocks_processed > 0) {
            block_vector[blocks_processed-1].first.reset();
        }
        blocks_processed++;
    }

    public:
    void finish() {
        // already wait()'ed in constructor
    }
    void cleanup() {
        if(! tg.is_canceling()) {
            tg.cancel();
        }
        tg.wait();
    }
    void cleanup_and_throw(std::string msg) {
        cleanup();
        throw_error<E>(msg.c_str());
    }

    void get_data(char * outbuffer, const uint64_t len) {
        if(current_blocksize - data_offset >= len) {
            std::memcpy(outbuffer, current_block+data_offset, len);
            data_offset += len;
        } else {
            // remainder of current block, may be zero
            uint64_t bytes_accounted = current_blocksize - data_offset;
            std::memcpy(outbuffer, current_block+data_offset, bytes_accounted);
            while(len - bytes_accounted >= MAX_BLOCKSIZE) {
                decompress_direct(outbuffer + bytes_accounted);
                bytes_accounted += MAX_BLOCKSIZE;
                data_offset = MAX_BLOCKSIZE;
            }
            if(len - bytes_accounted > 0) { // but less than MAX_BLOCKSIZE
                decompress_block();
                if(current_blocksize < len - bytes_accounted) {
                    cleanup_and_throw("Corrupted block data");
                }
                std::memcpy(outbuffer + bytes_accounted, current_block, len - bytes_accounted);
                data_offset = len - bytes_accounted;
                // bytes_accounted += data_offset; // no need to update since we are returning
            }
        }
    }

    const char * get_ptr(const uint64_t len) {
        if(current_blocksize - data_offset >= len) {
            const char * ptr = current_block + data_offset;
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
        memcpy(&pod, current_block+data_offset, sizeof(POD));
        data_offset += sizeof(POD);
        return pod;
    }

    template<typename POD> POD get_pod_contiguous() {
        if(current_blocksize - data_offset < sizeof(POD)) {
            cleanup_and_throw("Corrupted block data");
        }
        POD pod;
        memcpy(&pod, current_block+data_offset, sizeof(POD));
        data_offset += sizeof(POD);
        return pod;
    }
};


#endif
