#ifndef _QIO_MULTITHREADED_BLOCK_MODULE_H
#define _QIO_MULTITHREADED_BLOCK_MODULE_H

#include "io_common.h"
#include "xxhash_module.h"

#include <atomic>
#include <cstdio>
#include <ios>
#include <new>
#include <stdexcept>
#include <string>
#include <utility>
#include <tbb/concurrent_queue.h>
#include <tbb/concurrent_vector.h>
#include <tbb/enumerable_thread_specific.h>
#include <tbb/flow_graph.h>

// qdata-cpp uses the oneTBB flow-graph API: input_node with a
// T(tbb::flow_control &) body. Classic TBB (2020.3 and earlier) spells this
// source_node with a bool(T &) body and is not supported. Classic
// flow_graph.h pulls in tbb_stddef.h, so the macro is present to catch it
// here; oneTBB wrappers such as RcppParallel's do not always define it, and
// for those the missing tbb::flow::input_node is the diagnostic.
#if defined(TBB_INTERFACE_VERSION) && TBB_INTERFACE_VERSION < 12000
#error "qdata-cpp requires oneTBB 2021.1 or newer (TBB_INTERFACE_VERSION >= 12000)."
#endif

struct OrderedBlock {
    std::shared_ptr<char[]> block;
    uint32_t blocksize;
    uint64_t blocknumber;
    OrderedBlock(std::shared_ptr<char[]> block, uint32_t blocksize, uint64_t blocknumber) :
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

// The first failure on a worker thread, kept as its kind and message and
// raised again on the calling thread. An exception that escapes a task body
// would reach wait_for_all() through std::exception_ptr, and when libstdc++
// and libc++ are both loaded, std::rethrow_exception can bind to libc++abi
// while throw and catch bind to libstdc++, so the rethrown exception only
// matches catch (...). Task bodies run inside run() instead.
//
// The writer raises it with throw_if_failed() from finish(), which runs after
// the serializer has returned. The reader must stop where the data runs out,
// which can be inside R's read-bytes callback, so it passes message() to its
// error policy instead.
class QioWorkerError {
public:
    // Runs fn and records what it throws; returns whether it succeeded. Only
    // the first failure is kept.
    template <class Fn>
    bool run(Fn && fn) noexcept {
        try {
            fn();
            return true;
        } catch(const std::ios_base::failure & e) {
            record(kind::io_failure, e.what());  // callers catch it to report write failures
        } catch(const std::bad_alloc &) {
            record(kind::bad_alloc, "Out of memory in a worker thread");
        } catch(const std::exception & e) {
            record(kind::std_error, e.what());
        } catch(...) {
            record(kind::unknown, "Unknown error in a worker thread");
        }
        return false;
    }
    bool failed() const noexcept {
        return state.load(std::memory_order_acquire) != 0;
    }
    // The recorded message, or nullptr if no failure has been recorded. Call
    // after the graph has stopped, so a failure in progress has finished.
    const char * message() const noexcept {
        return state.load(std::memory_order_acquire) == recorded ? text : nullptr;
    }
    // Call after the graph has stopped; does nothing if no worker failed.
    void throw_if_failed() const {
        if(state.load(std::memory_order_acquire) != recorded) return;
        switch(error_kind) {
            case kind::io_failure:
                throw std::ios_base::failure(text);
            case kind::bad_alloc:
                throw std::bad_alloc();
            default:
                throw std::runtime_error(text);
        }
    }

private:
    enum class kind { std_error, io_failure, bad_alloc, unknown };
    static constexpr int claimed = 1;
    static constexpr int recorded = 2;
    std::atomic<int> state{0};
    kind error_kind = kind::unknown;
    char text[4096] = {};  // fixed size: nothing may throw while recording

    void record(const kind k, const char * const msg) noexcept {
        int expected = 0;
        if(!state.compare_exchange_strong(expected, claimed, std::memory_order_acq_rel)) return;
        error_kind = k;
        std::snprintf(text, sizeof(text), "%s", msg);
        state.store(recorded, std::memory_order_release);
    }
};

template <class stream_writer, class compressor, class hasher, class error_policy, bool direct_mem>
struct BlockCompressWriterMT {
    stream_writer & myFile;
    tbb::enumerable_thread_specific<compressor> cp;
    hasher hp;
    const int compress_level;

    tbb::concurrent_queue<std::shared_ptr<char[]>> available_blocks;
    tbb::concurrent_queue<std::shared_ptr<char[]>> available_zblocks;

    std::shared_ptr<char[]> current_block;
    uint32_t current_blocksize;
    uint64_t current_blocknumber;

    // No exception may leave a task body; see QioWorkerError.
    QioWorkerError worker_error;
    tbb::task_group_context tgc;
    tbb::flow::graph myGraph;
    tbb::flow::function_node<OrderedBlock, OrderedBlock> compressor_node;
    tbb::flow::function_node<OrderedPtr, OrderedBlock> compressor_direct_node;
    tbb::flow::sequencer_node<OrderedBlock> sequencer_node;
    tbb::flow::function_node<OrderedBlock, int, tbb::flow::rejecting> writer_node;

    BlockCompressWriterMT(stream_writer & f, const int cl) :
    myFile(f),
    cp(),
    hp(),
    compress_level(cl),
    available_blocks(),
    available_zblocks(),
    current_block(MAKE_SHARED_BLOCK(MAX_BLOCKSIZE)),
    current_blocksize(0),
    current_blocknumber(0),
    worker_error(),
    tgc(),
    myGraph(this->tgc),
    compressor_node(this->myGraph, tbb::flow::unlimited,
    [this](OrderedBlock block) {
        OrderedBlock zblock = compress_block(block.block.get(), block.blocksize, block.blocknumber);
        recycle_block(block.block);
        return zblock;
    }),
    compressor_direct_node(this->myGraph, tbb::flow::unlimited,
    [this](OrderedPtr ptr) {
        return compress_block(ptr.block, MAX_BLOCKSIZE, ptr.blocknumber);
    }),
    sequencer_node(this->myGraph,
    [](const OrderedBlock & zblock) {
        return zblock.blocknumber;
    }),
    writer_node(this->myGraph, tbb::flow::serial,
    [this](OrderedBlock zblock) {
        // a failed block arrives empty; after any failure nothing more is written
        if(zblock.block && !worker_error.failed()) {
            const bool ok = worker_error.run([&] {
                write_and_update(static_cast<uint32_t>(zblock.blocksize));
                write_and_update(zblock.block.get(), zblock.blocksize & (~BLOCK_METADATA));
            });
            if(!ok) tgc.cancel_group_execution();
        }
        recycle_zblock(zblock.block);
        return 0;
    })
    {
        tbb::flow::make_edge(compressor_node, sequencer_node);
        if constexpr (direct_mem) {
            tbb::flow::make_edge(compressor_direct_node, sequencer_node);
        }
        tbb::flow::make_edge(sequencer_node, writer_node);
    }
    private:
    // Runs on a worker. Every path returns the block's own number, because
    // sequencer_node is a successor on every path and a duplicate sequence
    // number is undefined behaviour in oneTBB. A failed block comes back empty.
    OrderedBlock compress_block(const char * const data, const uint32_t blocksize, const uint64_t blocknumber) noexcept {
        OrderedBlock zblock;
        zblock.blocknumber = blocknumber;
        const bool ok = worker_error.run([&] {
            if(!available_zblocks.try_pop(zblock.block)) {
                zblock.block = MAKE_SHARED_BLOCK_ASSIGNMENT(MAX_ZBLOCKSIZE);
            }
            typename tbb::enumerable_thread_specific<compressor>::reference cp_local = cp.local();
            zblock.blocksize = cp_local.compress(zblock.block.get(), MAX_ZBLOCKSIZE,
                                                 data, blocksize, compress_level);
            if(compressor::is_error(zblock.blocksize)) {
                throw std::runtime_error("Compression error");
            }
        });
        if(ok) return zblock;
        tgc.cancel_group_execution();
        recycle_zblock(zblock.block);
        zblock.block.reset();
        return zblock;
    }
    void recycle_block(const std::shared_ptr<char[]> & block) noexcept {
        if(!block) return;
        try { available_blocks.push(block); } catch(...) {}
    }
    void recycle_zblock(const std::shared_ptr<char[]> & block) noexcept {
        if(!block) return;
        try { available_zblocks.push(block); } catch(...) {}
    }
    void write_and_update(const char * const inbuffer, const uint64_t len) {
        myFile.write(inbuffer, len);
        hp.update(inbuffer, len);
    }
    template <typename POD>
    void write_and_update(const POD value) {
        myFile.writeInteger(value);
        hp.update(value);
    }
    inline void submit_block(std::shared_ptr<char[]> block, const uint32_t blocksize, const uint64_t blocknumber) {
        compressor_node.try_put(OrderedBlock(block, blocksize, blocknumber));
    }
    inline void submit_direct_block(const char * const block, const uint64_t blocknumber) {
        compressor_direct_node.try_put(OrderedPtr(block, blocknumber));
    }
    // These two run once per block and can be reached from underneath R's
    // serializer, so a C++ exception must not escape them. Each raises after
    // its catch, when the only live owner is current_block, which the Frame
    // owns; an escaping exception would cross R's C frames and a raise from
    // inside the handler would strand the exception object.
    void submit_current_block(const uint32_t blocksize) {
        bool ok = true;
        try { submit_block(current_block, blocksize, current_blocknumber); } catch(...) { ok = false; }
        if(!ok) cleanup_and_throw("Failed to submit output block");
    }
    // leaves the writer owning an empty current_block
    void acquire_current_block() {
        if(available_blocks.try_pop(current_block)) return;
        bool ok = true;
        try { current_block = MAKE_SHARED_BLOCK_ASSIGNMENT(MAX_BLOCKSIZE); } catch(...) { ok = false; }
        if(!ok) cleanup_and_throw("Failed to allocate output block");
    }
    void flush() {
        if(current_blocksize > 0) {
            submit_current_block(current_blocksize);
            current_blocknumber++;
            current_blocksize = 0;
            acquire_current_block();
        }
    }

    public:
    // Not reached from underneath R's serializer, so a worker failure is
    // thrown directly, as the same kind of exception the worker caught.
    uint64_t finish() {
        flush();
        myGraph.wait_for_all();
        worker_error.throw_if_failed();
        return hp.digest();
    }
    // best effort teardown; must not throw, callers may be mid-unwind
    void cleanup() noexcept {
        try {
            if(! tgc.is_group_execution_cancelled()) {
                tgc.cancel_group_execution();
            }
            myGraph.wait_for_all();
        } catch(...) {}
    }
    void cleanup_and_throw(const char * const msg) {
        cleanup();
        throw_error<error_policy>(msg);
    }

    void push_data(const char * const inbuffer, const uint64_t len) {
        uint64_t current_pointer_consumed = 0;

        if(current_blocksize >= MAX_BLOCKSIZE) { flush(); }
        if(current_blocksize > 0) {
            uint32_t add_length = std::min<uint64_t>(len - current_pointer_consumed, MAX_BLOCKSIZE - current_blocksize);
            std::memcpy(current_block.get() + current_blocksize, inbuffer + current_pointer_consumed, add_length);
            current_blocksize += add_length;
            current_pointer_consumed += add_length;
        }

        if(current_blocksize >= MAX_BLOCKSIZE) { flush(); }
        while(len - current_pointer_consumed >= MAX_BLOCKSIZE) {
            if constexpr (direct_mem) {
                submit_direct_block(inbuffer + current_pointer_consumed, current_blocknumber);
            } else {
                // current_blocksize is zero here: the input was either fully
                // consumed above or the block was filled and flushed
                std::memcpy(current_block.get(), inbuffer + current_pointer_consumed, MAX_BLOCKSIZE);
                submit_current_block(MAX_BLOCKSIZE);
                acquire_current_block();
            }
            current_blocknumber++;
            current_pointer_consumed += MAX_BLOCKSIZE;
        }

        if(len - current_pointer_consumed > 0) {
            uint32_t add_length = len - current_pointer_consumed;
            std::memcpy(current_block.get(), inbuffer + current_pointer_consumed, add_length);
            current_blocksize = add_length;
        }
    }

    template<typename POD> void push_pod(const POD pod) {
        if(current_blocksize > MIN_BLOCKSIZE) { flush(); }
        const char * ptr = reinterpret_cast<const char*>(&pod);
        std::memcpy(current_block.get() + current_blocksize, ptr, sizeof(POD));
        current_blocksize += sizeof(POD);
    }

    template<typename POD> void push_pod_contiguous(const POD pod) {
        const char * ptr = reinterpret_cast<const char*>(&pod);
        memcpy(current_block.get() + current_blocksize, ptr, sizeof(POD));
        current_blocksize += sizeof(POD);
    }

    template<typename POD> void push_pod(const POD pod, const bool contiguous) {
        if(contiguous) { push_pod_contiguous(pod); } else { push_pod(pod); }
    }
};

template <class stream_reader, class decompressor, class error_policy, class byte_copier = QioByteCopier>
struct BlockCompressReaderMT {
    stream_reader & myFile;
    tbb::enumerable_thread_specific<decompressor> dp;
    xxHashEnv hp;

    tbb::concurrent_queue<std::shared_ptr<char[]>> available_zblocks;
    tbb::concurrent_queue<std::shared_ptr<char[]>> available_blocks;

    std::shared_ptr<char[]> current_block;
    uint32_t current_blocksize;
    uint32_t data_offset;
    OrderedBlock scratch_block; // kept empty between calls; get_new_block() may longjmp

    std::atomic<bool> end_of_file;
    std::atomic<uint64_t> blocks_to_process;
    uint64_t blocks_processed;

    // No exception may leave a task body; see QioWorkerError.
    QioWorkerError worker_error;
    tbb::task_group_context tgc;
    tbb::flow::graph myGraph;
    tbb::flow::input_node<OrderedBlock> reader_node;
    tbb::flow::function_node<OrderedBlock, OrderedBlock> decompressor_node;
    tbb::flow::sequencer_node<OrderedBlock> sequencer_node;
    BlockCompressReaderMT(stream_reader & f) :
    myFile(f),
    dp(),
    hp(),
    available_zblocks(),
    available_blocks(),
    current_block(MAKE_SHARED_BLOCK(MAX_BLOCKSIZE)),
    current_blocksize(0),
    data_offset(0),
    end_of_file(false),
    blocks_to_process(0),
    blocks_processed(0),
    worker_error(),
    tgc(),
    myGraph(this->tgc),
    reader_node(this->myGraph,
    [this](tbb::flow_control & fc) {
        OrderedBlock zblock;
        bool more = false;
        if(!worker_error.run([&] { more = read_next_zblock(zblock); })) {
            tgc.cancel_group_execution();
        }
        if(!more) {
            fc.stop(); // the value returned alongside stop() is not forwarded
        }
        return zblock;
    }),
    decompressor_node(this->myGraph, tbb::flow::unlimited,
    [this](OrderedBlock zblock) {
        // carry the sequence number before anything can return: sequencer_node
        // is a successor on every path, and a duplicate sequence number (the
        // default-constructed 0) is undefined behaviour in oneTBB
        OrderedBlock block;
        block.blocknumber = zblock.blocknumber;
        const bool ok = worker_error.run([&] {
            typename tbb::enumerable_thread_specific<decompressor>::reference dp_local = dp.local();
            if(!available_blocks.try_pop(block.block)) {
                block.block = MAKE_SHARED_BLOCK_ASSIGNMENT(MAX_BLOCKSIZE);
            }
            block.blocksize = dp_local.decompress(block.block.get(), MAX_BLOCKSIZE, zblock.block.get(), zblock.blocksize);
        });
        recycle_zblock(zblock.block);  // the compressed input is spent on every path
        // a failed block goes on empty (size 0, maybe no buffer); get_new_block() stops at it
        if(!ok) {
            block.blocksize = 0;
            tgc.cancel_group_execution();
            return block;
        }
        if(decompressor::is_error(block.blocksize)) {
            tgc.cancel_group_execution();
        }
        return block;
    }),
    sequencer_node(this->myGraph,
    [](const OrderedBlock & block) {
        return block.blocknumber;
    })
    {
        tbb::flow::make_edge(reader_node, decompressor_node);
        tbb::flow::make_edge(decompressor_node, sequencer_node);
        reader_node.activate();
    }
    public:

    private:
    bool read_next_zblock(OrderedBlock & zblock) {
        uint32_t zsize;
        bool ok = this->myFile.readInteger(zsize);
        if(!ok) {
            end_of_file.store(true);
            return false;
        }
        const uint32_t zbytes = compressed_block_size(zsize);
        if(!compressed_block_size_fits_buffer(zsize)) {
            tgc.cancel_group_execution();
            return false;
        }
        if(!available_zblocks.try_pop(zblock.block)) {
            zblock.block = MAKE_SHARED_BLOCK_ASSIGNMENT(MAX_ZBLOCKSIZE);
        }
        uint32_t bytes_read = this->myFile.read(zblock.block.get(), zbytes);
        if(bytes_read != zbytes) {
            end_of_file.store(true);
            return false;
        }
        hp.update(zsize);
        hp.update(zblock.block.get(), bytes_read);
        zblock.blocksize = zsize;
        zblock.blocknumber = blocks_to_process.fetch_add(1);
        return true;
    }
    // failing to return a block to the free list only costs its reuse
    void recycle_block(const std::shared_ptr<char[]> & block) noexcept {
        if(!block) return;
        try { available_blocks.push(block); } catch(...) {}
    }
    void recycle_zblock(const std::shared_ptr<char[]> & zblock) noexcept {
        if(!zblock) return;
        try { available_zblocks.push(zblock); } catch(...) {}
    }
    // Runs on the main thread, possibly inside R's read-bytes callback, so
    // every failure goes through the error policy.
    void get_new_block() {
        while( true ) {
            if( sequencer_node.try_get(scratch_block) ) {
                // no valid block is empty; a failed one stops the read here,
                // before any later block is taken in its place
                if(scratch_block.blocksize == 0 || decompressor::is_error(scratch_block.blocksize)) {
                    recycle_block(scratch_block.block);
                    scratch_block.block.reset();
                    cleanup_and_throw_failure();
                }
                recycle_block(current_block);
                current_block = std::move(scratch_block.block);
                current_blocksize = scratch_block.blocksize;
                blocks_processed += 1;
                return;
            }

            if(end_of_file && blocks_processed >= blocks_to_process) {
                cleanup_and_throw("Unexpected end of file");
            }
            if(tgc.is_group_execution_cancelled()) {
                cleanup_and_throw_failure();
            }
        }
    }
    // A worker's recorded error if there is one, else a read or decompression error
    void cleanup_and_throw_failure() {
        cleanup();  // waits for every task, so a worker's failure is fully recorded
        const char * const msg = worker_error.message();
        throw_error<error_policy>(msg != nullptr ? msg : "File read / decompression error");
    }
    public:
    void finish() {
        myGraph.wait_for_all();
        if(const char * const msg = worker_error.message()) {
            throw_error<error_policy>(msg);
        }
        // wait_for_all() resets the task group context, so ask the graph,
        // which records whether the wait ended cancelled
        if(myGraph.is_cancelled()) {
            throw_error<error_policy>("File read / decompression error");
        }
    }
    void cleanup() noexcept {
        try {
            if(! tgc.is_group_execution_cancelled()) {
                tgc.cancel_group_execution();
            }
            myGraph.wait_for_all();
        } catch(...) {}
    }
    void cleanup_and_throw(const char * const msg) {
        cleanup();
        throw_error<error_policy>(msg);
    }
    uint64_t get_hash_digest() {
        return hp.digest();
    }
    const char * current_data() {
        if(current_blocksize == data_offset) {
            get_new_block();
            data_offset = 0;
        }
        return current_block.get() + data_offset;
    }
    uint32_t remaining_data() {
        if(current_blocksize == data_offset) {
            get_new_block();
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
            byte_copier::copy(outbuffer, current_block.get()+data_offset, len);
            data_offset += len;
        } else {
            uint64_t bytes_accounted = current_blocksize - data_offset;
            byte_copier::copy(outbuffer, current_block.get()+data_offset, bytes_accounted);
            while(len - bytes_accounted >= MAX_BLOCKSIZE) {
                get_new_block();
                if(current_blocksize != MAX_BLOCKSIZE) {
                    cleanup_and_throw("Corrupted block data");
                }
                byte_copier::copy(outbuffer + bytes_accounted, current_block.get(), MAX_BLOCKSIZE);
                bytes_accounted += MAX_BLOCKSIZE;
                data_offset = MAX_BLOCKSIZE;
            }
            if(len - bytes_accounted > 0) {
                get_new_block();
                if(current_blocksize < len - bytes_accounted) {
                    cleanup_and_throw("Corrupted block data");
                }
                byte_copier::copy(outbuffer + bytes_accounted, current_block.get(), len - bytes_accounted);
                data_offset = len - bytes_accounted;
            }
        }
    }

    const char * get_ptr(const uint64_t len) {
        if(current_blocksize - data_offset >= len) {
            const char * ptr = current_block.get() + data_offset;
            data_offset += len;
            return ptr;
        } else {
            return nullptr;
        }
    }

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

#endif
