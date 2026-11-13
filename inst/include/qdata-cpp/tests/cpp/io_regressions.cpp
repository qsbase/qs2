#include <atomic>
#include <cstddef>
#include <cstdint>
#include <ios>
#include <limits>
#include <new>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#if defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <sys/mman.h>
#if defined(MAP_ANONYMOUS)
#define QDATA_TEST_MAP_ANONYMOUS MAP_ANONYMOUS
#elif defined(MAP_ANON)
#define QDATA_TEST_MAP_ANONYMOUS MAP_ANON
#else
#error "Anonymous mmap is required for the large-reader regression test"
#endif
#endif

#include "io/block_module.h"
#include "io/zstd_module.h"

#ifdef QIO_HAS_TBB
#include <tbb/global_control.h>
#include "io/multithreaded_block_module.h"
#endif

namespace {

template <class Fn>
void expect_runtime_error(Fn&& fn, const std::string& needle) {
    try {
        std::forward<Fn>(fn)();
    } catch(const std::runtime_error& err) {
        if(std::string(err.what()).find(needle) == std::string::npos) {
            throw std::runtime_error("unexpected error message: " + std::string(err.what()));
        }
        return;
    }
    throw std::runtime_error("expected std::runtime_error");
}

struct CountingWriter {
    std::uint64_t bytes_written = 0;

    void write(const char*, const std::uint64_t size) {
        bytes_written += size;
    }

    template <class T>
    void writeInteger(const T) {
        bytes_written += sizeof(T);
    }
};

struct ErrorCompressor {
    static bool is_error(const std::uint32_t size) {
        return size == COMPRESSION_ERROR;
    }

    std::uint32_t compress(char*, std::uint32_t, const char*, std::uint32_t, int) {
        return COMPRESSION_ERROR;
    }
};

struct ConstructorErrorCompressor {
    ConstructorErrorCompressor() {
        throw std::runtime_error("fake compression context creation failure");
    }

    static bool is_error(const std::uint32_t size) {
        return size == COMPRESSION_ERROR;
    }

    std::uint32_t compress(char*, std::uint32_t, const char*, std::uint32_t, int) {
        return 1;
    }
};

struct FakeBlockReader {
    std::uint64_t blocks_remaining;

    explicit FakeBlockReader(const std::uint64_t block_count) : blocks_remaining(block_count) {}

    template <class T>
    bool readInteger(T& value) {
        if(blocks_remaining == 0) return false;
        value = static_cast<T>(1);
        --blocks_remaining;
        return true;
    }

    std::uint32_t read(char* const destination, const std::uint32_t size) {
        if(size != 1) return 0;
        destination[0] = 0;
        return 1;
    }
};

struct FakeDecompressor {
    static bool is_error(const std::uint32_t size) {
        return size == COMPRESSION_ERROR;
    }

    std::uint32_t decompress(char*, std::uint32_t, const char*, std::uint32_t) {
        return MAX_BLOCKSIZE;
    }
};

struct NoopCopier {
    static void copy(void*, const void*, std::size_t) {}
};

class VirtualReservation {
public:
    explicit VirtualReservation(const std::uint64_t size) : size_(static_cast<std::size_t>(size)) {
        if(size > static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
            throw std::runtime_error("test virtual reservation exceeds size_t");
        }
#if defined(_WIN32)
        data_ = static_cast<char*>(VirtualAlloc(nullptr, size_, MEM_RESERVE, PAGE_NOACCESS));
        if(data_ == nullptr) throw std::runtime_error("failed to reserve test address space");
#else
        void* const result = mmap(nullptr, size_, PROT_NONE, MAP_PRIVATE | QDATA_TEST_MAP_ANONYMOUS, -1, 0);
        if(result == MAP_FAILED) throw std::runtime_error("failed to reserve test address space");
        data_ = static_cast<char*>(result);
#endif
    }

    ~VirtualReservation() {
#if defined(_WIN32)
        if(data_ != nullptr) VirtualFree(data_, 0, MEM_RELEASE);
#else
        if(data_ != nullptr) munmap(data_, size_);
#endif
    }

    char* data() const {
        return data_;
    }

private:
    char* data_ = nullptr;
    std::size_t size_;
};

void test_single_thread_writer_errors() {
    CountingWriter partial_output;
    BlockCompressWriter<CountingWriter, ErrorCompressor, xxHashEnv, StdErrorPolicy, true>
        partial_writer(partial_output, 3);
    const char byte = 1;
    partial_writer.push_data(&byte, 1);
    expect_runtime_error([&] { partial_writer.finish(); }, "Compression error");
    if(partial_output.bytes_written != 0) {
        throw std::runtime_error("partial compression error wrote output");
    }

    CountingWriter direct_output;
    BlockCompressWriter<CountingWriter, ErrorCompressor, xxHashEnv, StdErrorPolicy, true>
        direct_writer(direct_output, 3);
    std::vector<char> input(MAX_BLOCKSIZE);
    expect_runtime_error(
        [&] { direct_writer.push_data(input.data(), input.size()); },
        "Compression error"
    );
    if(direct_output.bytes_written != 0) {
        throw std::runtime_error("direct compression error wrote output");
    }
}

void test_context_checks() {
    expect_runtime_error(
        [] { checked_zstd_compression_context(nullptr); },
        "compression context"
    );
    expect_runtime_error(
        [] { checked_zstd_decompression_context(nullptr); },
        "decompression context"
    );
}

void test_single_thread_large_read() {
    if(sizeof(std::size_t) < sizeof(std::uint64_t)) return;
    const std::uint64_t block_count = (std::uint64_t{1} << 32) / MAX_BLOCKSIZE + 1;
    const std::uint64_t length = block_count * MAX_BLOCKSIZE;
    VirtualReservation output(length);
    FakeBlockReader stream(block_count);
    BlockCompressReader<FakeBlockReader, FakeDecompressor, StdErrorPolicy, NoopCopier> reader(stream);
    reader.get_data(output.data(), length);
    if(stream.blocks_remaining != 0) {
        throw std::runtime_error("large single-threaded read did not consume every block");
    }
}

#ifdef QIO_HAS_TBB

struct TrapErrorPolicy {
    static std::atomic<bool> called;

    [[noreturn]] static void raise(const char* const message) {
        called.store(true);
        throw std::runtime_error(message);
    }

    [[noreturn]] static void raise(const std::string& message) {
        called.store(true);
        throw std::runtime_error(message);
    }
};

std::atomic<bool> TrapErrorPolicy::called{false};

struct GatedNoopCopier {
    static std::atomic<std::uint64_t> blocks_consumed;

    static void copy(void*, const void*, const std::size_t size) {
        if(size == MAX_BLOCKSIZE) {
            blocks_consumed.fetch_add(1);
        }
    }
};

std::atomic<std::uint64_t> GatedNoopCopier::blocks_consumed{0};

struct GatedBlockReader {
    const std::uint64_t block_count;
    std::uint64_t blocks_produced = 0;

    explicit GatedBlockReader(const std::uint64_t count) : block_count(count) {}

    template <class T>
    bool readInteger(T& value) {
        if(blocks_produced >= block_count) return false;
        while(blocks_produced >= GatedNoopCopier::blocks_consumed.load() + 3) {
            std::this_thread::yield();
        }
        value = static_cast<T>(1);
        ++blocks_produced;
        return true;
    }

    std::uint32_t read(char* const destination, const std::uint32_t size) {
        if(size != 1) return 0;
        destination[0] = 0;
        return 1;
    }
};

// Worker failures must not leave a task body: TBB would carry them to
// wait_for_all() in a std::exception_ptr, which loses their type when
// libstdc++ and libc++ are both loaded.
template <class Io>
void expect_quiet_graph(Io& io) {
    try {
        io.myGraph.wait_for_all();
    } catch(...) {
        throw std::runtime_error("an exception escaped a multithreaded task body");
    }
}

struct FailingWriter {
    void write(const char*, const std::uint64_t) {
        throw std::ios_base::failure("fake write failure");
    }

    template <class T>
    void writeInteger(const T) {
        throw std::ios_base::failure("fake write failure");
    }
};

struct BadAllocCompressor {
    static bool is_error(const std::uint32_t size) {
        return size == COMPRESSION_ERROR;
    }

    std::uint32_t compress(char*, std::uint32_t, const char*, std::uint32_t, int) {
        throw std::bad_alloc();
    }
};

void test_multi_thread_writer_error(const bool direct) {
    TrapErrorPolicy::called.store(false);
    CountingWriter output;
    BlockCompressWriterMT<CountingWriter, ErrorCompressor, xxHashEnv, TrapErrorPolicy, true>
        writer(output, 3);
    std::vector<char> input(direct ? MAX_BLOCKSIZE : 1);
    writer.push_data(input.data(), input.size());
    if(direct) expect_quiet_graph(writer);  // a 1-byte input is only submitted by finish()
    expect_runtime_error([&] { writer.finish(); }, "Compression error");
    if(TrapErrorPolicy::called.load()) {
        throw std::runtime_error("multithreaded compression used the error policy on a worker");
    }
    if(output.bytes_written != 0) {
        throw std::runtime_error("multithreaded compression error wrote output");
    }
}

// A failed write reaches the caller as std::ios_base::failure, which qs_save
// and qd_save catch to name the file.
void test_multi_thread_write_failure() {
    TrapErrorPolicy::called.store(false);
    FailingWriter output;
    BlockCompressWriterMT<FailingWriter, ZstdCompressor, xxHashEnv, TrapErrorPolicy, false>
        writer(output, 3);
    std::vector<char> input(2 * MAX_BLOCKSIZE);
    writer.push_data(input.data(), input.size());
    expect_quiet_graph(writer);
    bool caught = false;
    try {
        writer.finish();
    } catch(const std::ios_base::failure& err) {
        caught = std::string(err.what()).find("fake write failure") != std::string::npos;
    }
    if(!caught) {
        throw std::runtime_error("multithreaded write failure did not reach finish() as std::ios_base::failure");
    }
    if(TrapErrorPolicy::called.load()) {
        throw std::runtime_error("multithreaded write failure used the error policy on a worker");
    }
}

void test_multi_thread_bad_alloc() {
    TrapErrorPolicy::called.store(false);
    CountingWriter output;
    BlockCompressWriterMT<CountingWriter, BadAllocCompressor, xxHashEnv, TrapErrorPolicy, false>
        writer(output, 3);
    std::vector<char> input(MAX_BLOCKSIZE);
    writer.push_data(input.data(), input.size());
    expect_quiet_graph(writer);
    bool caught = false;
    try {
        writer.finish();
    } catch(const std::bad_alloc&) {
        caught = true;
    }
    if(!caught) {
        throw std::runtime_error("multithreaded allocation failure did not reach finish() as std::bad_alloc");
    }
    if(TrapErrorPolicy::called.load() || output.bytes_written != 0) {
        throw std::runtime_error("multithreaded allocation failure used the error policy or wrote output");
    }
}

void test_multi_thread_context_error() {
    TrapErrorPolicy::called.store(false);
    CountingWriter output;
    expect_runtime_error(
        [&] {
            BlockCompressWriterMT<CountingWriter, ConstructorErrorCompressor, xxHashEnv, TrapErrorPolicy, true>
                writer(output, 3);
            const char byte = 1;
            writer.push_data(&byte, 1);
            writer.finish();
        },
        "context creation failure"
    );
    if(TrapErrorPolicy::called.load()) {
        throw std::runtime_error("multithreaded context error used the error policy on a worker");
    }
}

// Each compressed block holds one byte, its index, so a decompressor can fail
// on a chosen block whatever order the workers run in.
struct IndexedBlockReader {
    const std::uint64_t block_count;
    std::uint64_t blocks_produced = 0;

    explicit IndexedBlockReader(const std::uint64_t count) : block_count(count) {}

    template <class T>
    bool readInteger(T& value) {
        if(blocks_produced >= block_count) return false;
        value = static_cast<T>(1);
        return true;
    }

    std::uint32_t read(char* const destination, const std::uint32_t size) {
        if(size != 1) return 0;
        destination[0] = static_cast<char>(blocks_produced++);
        return 1;
    }
};

constexpr char failing_block = 2;

struct BadAllocAtBlockDecompressor {
    static bool is_error(const std::uint32_t size) {
        return size == COMPRESSION_ERROR;
    }

    std::uint32_t decompress(char*, std::uint32_t, const char* const source, std::uint32_t) {
        if(source[0] == failing_block) throw std::bad_alloc();
        return MAX_BLOCKSIZE;
    }
};

struct ErrorAtBlockDecompressor {
    static bool is_error(const std::uint32_t size) {
        return size == COMPRESSION_ERROR;
    }

    std::uint32_t decompress(char*, std::uint32_t, const char* const source, std::uint32_t) {
        return source[0] == failing_block ? COMPRESSION_ERROR : MAX_BLOCKSIZE;
    }
};

// fails in dp.local(), before the worker has an output block
struct ConstructorErrorDecompressor {
    ConstructorErrorDecompressor() {
        throw std::runtime_error("fake decompression context creation failure");
    }

    static bool is_error(const std::uint32_t size) {
        return size == COMPRESSION_ERROR;
    }

    std::uint32_t decompress(char*, std::uint32_t, const char*, std::uint32_t) {
        return MAX_BLOCKSIZE;
    }
};

// Reads in small pieces until the reader raises; returns the bytes read
// before that and checks the message.
template <class Reader>
std::uint64_t read_until_error(Reader& reader, const std::string& needle) {
    std::vector<char> buffer(4096);
    std::uint64_t bytes_read = 0;
    expect_runtime_error(
        [&] {
            for(int i = 0; i < 100000; ++i) {
                reader.get_data(buffer.data(), buffer.size());
                bytes_read += buffer.size();
            }
        },
        needle
    );
    return bytes_read;
}

// The main thread reports a worker's failure through the error policy, with
// its message, and takes no block after the failed one.
void test_multi_thread_reader_bad_alloc() {
    IndexedBlockReader stream(4);
    BlockCompressReaderMT<IndexedBlockReader, BadAllocAtBlockDecompressor, StdErrorPolicy, NoopCopier> reader(stream);
    expect_quiet_graph(reader);
    const std::uint64_t bytes_read = read_until_error(reader, "Out of memory in a worker thread");
    if(bytes_read != failing_block * std::uint64_t{MAX_BLOCKSIZE}) {
        throw std::runtime_error("multithreaded reader read past a block whose worker failed");
    }
}

void test_multi_thread_reader_decompression_error() {
    IndexedBlockReader stream(4);
    BlockCompressReaderMT<IndexedBlockReader, ErrorAtBlockDecompressor, StdErrorPolicy, NoopCopier> reader(stream);
    const std::uint64_t bytes_read = read_until_error(reader, "File read / decompression error");
    if(bytes_read != failing_block * std::uint64_t{MAX_BLOCKSIZE}) {
        throw std::runtime_error("multithreaded reader read past a block that failed to decompress");
    }
}

void test_multi_thread_reader_context_error() {
    IndexedBlockReader stream(4);
    BlockCompressReaderMT<IndexedBlockReader, ConstructorErrorDecompressor, StdErrorPolicy, NoopCopier> reader(stream);
    expect_quiet_graph(reader);
    if(read_until_error(reader, "fake decompression context creation failure") != 0) {
        throw std::runtime_error("multithreaded reader returned data after its decompression context failed");
    }
}

// A failure in a block after the object is reported by finish().
void test_multi_thread_reader_trailing_failure() {
    IndexedBlockReader stream(failing_block + 1);
    BlockCompressReaderMT<IndexedBlockReader, BadAllocAtBlockDecompressor, StdErrorPolicy, NoopCopier> reader(stream);
    std::vector<char> output(failing_block * std::size_t{MAX_BLOCKSIZE});
    reader.get_data(output.data(), output.size());
    expect_runtime_error([&] { reader.finish(); }, "Out of memory in a worker thread");
}

void test_multi_thread_large_read() {
    if(sizeof(std::size_t) < sizeof(std::uint64_t)) return;
    const std::uint64_t block_count = (std::uint64_t{1} << 32) / MAX_BLOCKSIZE + 1;
    const std::uint64_t length = block_count * MAX_BLOCKSIZE;
    VirtualReservation output(length);
    GatedNoopCopier::blocks_consumed.store(0);
    GatedBlockReader stream(block_count);
    BlockCompressReaderMT<GatedBlockReader, FakeDecompressor, StdErrorPolicy, GatedNoopCopier> reader(stream);
    reader.get_data(output.data(), length);
    reader.finish();
    if(stream.blocks_produced != block_count || GatedNoopCopier::blocks_consumed.load() != block_count) {
        throw std::runtime_error("large multithreaded read did not consume every block");
    }
}

#endif

}

int main() {
    test_single_thread_writer_errors();
    test_context_checks();
    test_single_thread_large_read();
#ifdef QIO_HAS_TBB
    tbb::global_control control(tbb::global_control::parameter::max_allowed_parallelism, 2);
    test_multi_thread_writer_error(false);
    test_multi_thread_writer_error(true);
    test_multi_thread_write_failure();
    test_multi_thread_bad_alloc();
    test_multi_thread_context_error();
    test_multi_thread_reader_bad_alloc();
    test_multi_thread_reader_decompression_error();
    test_multi_thread_reader_context_error();
    test_multi_thread_reader_trailing_failure();
    test_multi_thread_large_read();
#endif
    return 0;
}
