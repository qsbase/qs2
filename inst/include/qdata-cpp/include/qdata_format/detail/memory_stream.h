#ifndef QDATA_FORMAT_DETAIL_MEMORY_STREAM_H
#define QDATA_FORMAT_DETAIL_MEMORY_STREAM_H

#include "byte_buffer.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>

namespace qdata {
namespace detail {

class memory_reader {
public:
    memory_reader(const void* const data, const std::uint64_t size) :
    buffer_(reinterpret_cast<const char*>(data)),
    size_(size),
    position_(0) {}

    std::uint32_t read(char* const data, const std::uint64_t bytes_to_read) {
        const auto bytes_available = size_ - position_;
        const auto bytes_to_actually_read = std::min(bytes_to_read, bytes_available);
        if(bytes_to_actually_read > 0) {
            std::memcpy(data, buffer_ + position_, static_cast<std::size_t>(bytes_to_actually_read));
            position_ += bytes_to_actually_read;
        }
        return static_cast<std::uint32_t>(bytes_to_actually_read);
    }

    template <typename T>
    bool readInteger(T& value) {
        return read(reinterpret_cast<char*>(&value), sizeof(T)) == sizeof(T);
    }

    void seekg(const std::uint64_t pos) {
        position_ = std::min(pos, size_);
    }

    std::uint64_t tellg() const {
        return position_;
    }

private:
    const char* buffer_;
    std::uint64_t size_;
    std::uint64_t position_;
};

class erased_memory_writer {
public:
    explicit erased_memory_writer(void* const buffer_ctx, const output_buffer_ops ops) :
    buffer_ctx_(buffer_ctx),
    ops_(ops),
    position_(0) {}

    std::uint32_t write(const char* const data, const std::uint64_t size) {
        ensure_capacity(size);
        if(size > 0) {
            auto* const buffer = static_cast<char*>(ops_.data_fn(buffer_ctx_));
            std::memcpy(buffer + position_, data, static_cast<std::size_t>(size));
            position_ += static_cast<std::size_t>(size);
        }
        return static_cast<std::uint32_t>(size);
    }

    template <typename T>
    void writeInteger(const T value) {
        write(reinterpret_cast<const char*>(&value), sizeof(T));
    }

    void seekp(const std::uint64_t pos) {
        if(pos > size_bytes()) {
            throw std::out_of_range("Seek position is beyond buffer capacity");
        }
        position_ = static_cast<std::size_t>(pos);
    }

    std::uint64_t tellp() const {
        return position_;
    }

private:
    static constexpr std::size_t initial_capacity_bytes = 1024;

    void* buffer_ctx_;
    output_buffer_ops ops_;
    std::size_t position_;

    std::size_t size_bytes() const {
        return ops_.size_fn(buffer_ctx_);
    }

    void ensure_capacity(const std::uint64_t additional_size) {
        const auto required_size = checked_required_capacity(position_, additional_size);
        if(required_size > size_bytes()) {
            std::size_t grown_size = size_bytes() > 0 ? size_bytes() : initial_capacity_bytes;
            while(grown_size < required_size) {
                if(grown_size > std::size_t(-1) / 2) {
                    grown_size = required_size;
                    break;
                }
                grown_size *= 2;
            }
            ops_.resize_fn(buffer_ctx_, grown_size);
        }
    }
};

template <class Buffer>
class memory_writer {
public:
    memory_writer() :
    buffer_(),
    writer_(static_cast<void*>(&buffer_), make_output_buffer_ops<Buffer>()) {
        validate_output_buffer<Buffer>();
    }

    memory_writer(const memory_writer&) = delete;
    memory_writer& operator=(const memory_writer&) = delete;
    memory_writer(memory_writer&&) = delete;
    memory_writer& operator=(memory_writer&&) = delete;

    std::uint32_t write(const char* const data, const std::uint64_t size) {
        return writer_.write(data, size);
    }

    template <typename T>
    void writeInteger(const T value) {
        writer_.writeInteger(value);
    }

    void seekp(const std::uint64_t pos) {
        writer_.seekp(pos);
    }

    std::uint64_t tellp() const {
        return writer_.tellp();
    }

    Buffer take_bytes(const std::size_t size) {
        if(size > buffer_.size()) {
            throw std::out_of_range("Requested output size exceeds buffer size");
        }
        buffer_.resize(size);
        return std::move(buffer_);
    }

private:
    Buffer buffer_;
    erased_memory_writer writer_;
};

} // namespace detail
} // namespace qdata

#endif
