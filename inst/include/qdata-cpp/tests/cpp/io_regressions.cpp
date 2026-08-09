#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
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

void test_multi_thread_writer_error(const bool direct) {
    TrapErrorPolicy::called.store(false);
    CountingWriter output;
    BlockCompressWriterMT<CountingWriter, ErrorCompressor, xxHashEnv, TrapErrorPolicy, true>
        writer(output, 3);
    std::vector<char> input(direct ? MAX_BLOCKSIZE : 1);
    writer.push_data(input.data(), input.size());
    expect_runtime_error([&] { writer.finish(); }, "Compression error");
    if(TrapErrorPolicy::called.load()) {
        throw std::runtime_error("multithreaded compression used the error policy on a worker");
    }
    if(output.bytes_written != 0) {
        throw std::runtime_error("multithreaded compression error wrote output");
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
    test_multi_thread_context_error();
    test_multi_thread_large_read();
#endif
    return 0;
}
