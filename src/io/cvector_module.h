// include guard
#ifndef _QS2_CVECTOR_MODULE_H
#define _QS2_CVECTOR_MODULE_H

#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <cstdint>
#include "io/io_common.h"

// Vector-like classes for writing/reading to a memory buffer
// For CVectorOut we use malloc so we can release the memory to C

class CVectorIn {
private:
    const char* buffer;    // Pointer to the memory buffer for reading
    uint64_t size;         // Size of the buffer
    uint64_t position;     // Current read position within the buffer

public:
    // Constructor that takes a buffer and its size
    CVectorIn(const char* buf, uint64_t bufSize) : buffer(buf), size(bufSize), position(0) {}

    // Read function that reads up to 'bytesToRead' bytes into 'data'
    // Returns the actual number of bytes read as uint32_t
    uint32_t read(char* data, uint64_t bytesToRead) {
        // Calculate the number of bytes that can actually be read
        uint64_t bytesAvailable = size - position;
        uint64_t bytesToActuallyRead = (bytesToRead > bytesAvailable) ? bytesAvailable : bytesToRead;

        // Copy the data and advance the position
        std::memcpy(data, buffer + position, bytesToActuallyRead);
        position += bytesToActuallyRead;

        // Return the number of bytes read as uint32_t
        return static_cast<uint32_t>(bytesToActuallyRead);
    }
    // return whether enough bytes were read to fill the value
    template <typename T> bool readInteger(T & value) {
        return read(reinterpret_cast<char*>(&value), sizeof(T)) == sizeof(T);
    }

    // Seek function (seekg as in ifstream) that sets the read position
    void seekg(uint64_t pos) {
        if (pos > size) {
            position = size; // Move to the end if pos exceeds buffer size
        } else {
            position = pos;
        }
    }

    // Get current read position (tellg as in ifstream)
    uint64_t tellg() const {
        return position;
    }

    // Disable copying and assignment
    CVectorIn(const CVectorIn&) = delete;
    CVectorIn& operator=(const CVectorIn&) = delete;
};


class CVectorOut {
private:
    char* buffer;           // Memory buffer for writing
    uint64_t capacity;      // Current buffer capacity
    uint64_t position;      // Current write position within the buffer

    // Helper function to ensure the buffer has enough capacity
    void ensureCapacity(uint64_t additionalSize) {
        if (position + additionalSize > capacity) {
            // Increase capacity, doubling the current capacity
            uint64_t newCapacity = (capacity == 0) ? additionalSize : capacity * 2;
            while (newCapacity < position + additionalSize) {
                newCapacity *= 2;
            }
            buffer = (char*)realloc(buffer, newCapacity);
            if (!buffer) {
                throw std::runtime_error("Failed to allocate memory");
            }
            capacity = newCapacity;
        }
    }

public:
    // Constructor
    CVectorOut() : buffer(nullptr), capacity(0), position(0) {}

    // Destructor
    ~CVectorOut() {
        if (buffer) {
            free(buffer);
        }
    }
    
    // Move constructor
    CVectorOut(CVectorOut&& other) noexcept 
        : buffer(other.buffer), capacity(other.capacity), position(other.position) {
        other.buffer = nullptr;
        other.capacity = 0;
        other.position = 0;
    }

    // Move assignment operator
    CVectorOut& operator=(CVectorOut&& other) noexcept {
        if (this != &other) {
            // Free any existing buffer
            if (buffer) {
                free(buffer);
            }
            // Transfer ownership from 'other' to 'this'
            buffer = other.buffer;
            capacity = other.capacity;
            position = other.position;

            // Nullify 'other' to prevent double-free
            other.buffer = nullptr;
            other.capacity = 0;
            other.position = 0;
        }
        return *this;
    }

    // Write function (same as ofstream)
    void write(const char* data, uint64_t size) {
        ensureCapacity(size);          // Ensure buffer has enough capacity
        std::memcpy(buffer + position, data, size);
        position += size;
    }
    template <typename T> void writeInteger(const T value) {
        write(reinterpret_cast<const char*>(&value), sizeof(T));
    }

    // Seek function (same as ofstream)
    void seekp(uint64_t pos) {
        if (pos > capacity) {  // Check if pos is beyond the buffer's capacity
            throw std::out_of_range("Seek position is beyond buffer capacity");
        }
        position = pos;
    }

    // Release function - relinquish control of the buffer
    char* release() {
        char* temp = buffer;
        buffer = nullptr;
        capacity = 0;
        position = 0;
        return temp;
    }

    // get data but do not release
    char* data() const {
        return buffer;
    }

    // Get current position (tellp as in ofstream)
    uint64_t tellp() const {
        return position;
    }

    // Get current capacity
    uint64_t getCapacity() const {
        return capacity;
    }

    // Disable copying and assignment
    CVectorOut(const CVectorOut&) = delete;
    CVectorOut& operator=(const CVectorOut&) = delete;
};

#endif