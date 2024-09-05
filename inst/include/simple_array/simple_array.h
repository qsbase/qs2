#ifndef SIMPLE_ARRAY_H
#define SIMPLE_ARRAY_H

#include <memory>
#include <algorithm>
#include <type_traits>
#include <cstdlib>
#include <cstring>

namespace trqwe {

// simple wrapper around a C style array
// Does not store capacity, no push_back method. Can be re-allocated with reset(size) or resize(size)
template<class T, class Allocator = std::allocator<T>, class S = size_t>
class simple_array {
  static_assert(std::is_pod<T>::value, "simple_array type should be POD");
public:
  typedef T      value_type;
  typedef T*     pointer_type;
  typedef T&     reference_type;
  typedef S      size_type;
private:
  struct Members : Allocator { // derive from Allocator to use empty base optimization
    pointer_type _data;
    size_type    _size;
    inline pointer_type allocate_and_copy(value_type const * const data, const size_type size) {
      pointer_type result = this->allocate(size);
      std::copy(data, data + size, result);
      return result;
    }
    Members(value_type const * const data, const size_type size) : _data(allocate_and_copy(data, size)), _size(size) {}
    Members(const size_type size) : _data(this->allocate(size)), _size(size) {}
    Members() : _data(this->allocate(0)), _size(0) {}
  } m;
  friend void swap(simple_array & first, simple_array & second) noexcept {
    using std::swap;
    std::swap(first.m._data, second.m._data);
    std::swap(first.m._size, second.m._size);
  }
  
public:
  simple_array() : m() {}
  simple_array(value_type const * const data, const size_t size) : m(data, size) {}
  simple_array(const size_t size) : m(size) {}
  simple_array(const size_t size, const value_type value) : m(size) {
    std::fill(begin(), end(), value);
  }
  // Copy
  simple_array(simple_array const & other) : m(other.m._data, other.m._size) {}
  // Move
  simple_array(simple_array && other) noexcept : m() {
    swap(*this, other);
  }
  // "copy and swap" covers both move and copy assignment https://stackoverflow.com/q/3279543/2723734
  simple_array & operator=(simple_array other) {
    swap(*this, other);
    return *this;
  }
  // destructor
  ~simple_array() {
    m.deallocate(m._data, m._size);
  }
  void reset(const size_type size) {
    m.deallocate(m._data, m._size);
    m._data = m.allocate(size);
    m._size = size;
  }
  void resize(const size_type size) { // we don't have m._capacity so there is always a re-allocation
    pointer_type temp = m._data;
    m._data = m.allocate(size);
    std::copy(temp, temp + std::min(size, m._size), m._data);
    m.deallocate(temp, m._size);
    m._size = size;
  }
  size_type size() const { return m._size; }
  pointer_type data() const { return m._data; }
  reference_type operator[](size_type idx) const { return *(m._data + idx); }
  pointer_type begin() const { return m._data; }
  pointer_type end() const { return m._data + m._size; }
  pointer_type begin() { return m._data; }
  pointer_type end() { return m._data + m._size; }
};

}

#endif // include guard