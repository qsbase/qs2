#ifndef NULLABLE_ARRAY_H
#define NULLABLE_ARRAY_H

#include <memory>
#include <algorithm>
#include <type_traits>
#include <cstdlib>
#include <cstring>
#include <limits>

namespace trqwe {

// simple wrapper around a C style array. Nullable.
// Does not store capacity, no push_back method. Can be re-allocated with reset(size) or resize(size)
// Check null status or make null: is_null and nullify
template<class T, class Allocator = std::allocator<T>, class S = size_t>
class nullable_array {
  static_assert(std::is_pod<T>::value, "nullable_array type should be POD");
public:
  typedef T      value_type;
  typedef T*     pointer_type;
  typedef T&     reference_type;
  typedef S      size_type;
  static constexpr size_type nullsize = std::numeric_limits<size_type>::max();
private:
  struct Members : Allocator { // derive from Allocator to use empty base optimization
    pointer_type _data;
    size_type    _size;
    inline pointer_type allocate_check_and_copy(value_type const * const data, const size_type size) {
      if(size == nullsize) {
        return nullptr;
      } else {
        pointer_type result = this->allocate(size);
        std::copy(data, data + size, result);
        return result;
      }
    }
    inline pointer_type allocate_check(const size_type size) {
      if(size == nullsize) {
        return nullptr;
      } else {
        pointer_type result = this->allocate(size);
        return result;
      }
    }
    inline void deallocate_check() {
      if(_size != nullsize) {
        this->deallocate(_data, _size);
      }
    }
    Members(value_type const * const data, const size_type size) : _data(allocate_check_and_copy(data, size)), _size(size) {}
    Members(const size_type size) : _data(allocate_check(size)), _size(size) {}
    Members() : _data(nullptr), _size(nullsize) {}
  } m;
  friend void swap(nullable_array & first, nullable_array & second) noexcept {
    using std::swap;
    std::swap(first.m._data, second.m._data);
    std::swap(first.m._size, second.m._size);
  }
  
public:
  nullable_array() : m() {}
  nullable_array(value_type const * const data, const size_t size) : m(data, size) {}
  nullable_array(const size_t size) : m(size) {}
  nullable_array(const size_t size, const value_type value) : m(size) {
    std::fill(begin(), end(), value);
  }
  // Copy
  nullable_array(nullable_array const & other) : m(other.m._data, other.m._size) {}
  // Move
  nullable_array(nullable_array && other) noexcept : m() {
    swap(*this, other);
  }
  // "copy and swap" covers both move and copy assignment https://stackoverflow.com/q/3279543/2723734
  nullable_array & operator=(nullable_array other) {
    swap(*this, other);
    return *this;
  }
  // destructor
  ~nullable_array() {
    m.deallocate_check();
  }
  bool is_null() const { return m._size == nullsize; }
  void nullify() {
    m.deallocate_check();
    m._data = nullptr;
    m._size = nullsize;
  }
  // reset and resize are the same, but reset doesn't copy over old data
  // we don't have m._capacity so there is always a re-allocation
  // assume size != nullsize
  void reset(const size_type size) {
    m.deallocate_check();
    m._data = m.allocate(size);
    m._size = size;
  }
  void resize(const size_type size) {
    if(!is_null()) {
      pointer_type new_addr = m.allocate(size);
      std::copy(m._data, m._data + std::min(size, m._size), new_addr);
      m.deallocate(m._data, m._size);
      m._data = new_addr;
      m._size = size;
    } else {
      m._data = m.allocate(size);
      m._size = size;
    }
  }
  size_type size() const { return m._size; }
  pointer_type data() const { return m._data; }
  reference_type operator[](size_type idx) const { return *(m._data + idx); }
  pointer_type begin() const { return m._data; }
  pointer_type end() const { return m._data + m._size; }
  pointer_type begin() { return m._data; }
  pointer_type end() { return m._data + m._size; }
};

} // end namespace

#endif // include guard