#ifndef RING_BUFFER_RINGBUFF_H
#define RING_BUFFER_RINGBUFF_H

#include <cstddef>
#include <stdexcept>

template<typename T>
class RingBuffer {
public:
    // constructor
    RingBuffer(size_t sz) : _size(sz), _empty(true) {
        if (_size == 0) throw std::invalid_argument("Buffer size cannot be zero");
        _buf = new T[sz];
        _pTail = _pHead = _buf;
    };

    // destructor
    ~RingBuffer() {
        delete[] _buf;
    }

    // copy
    RingBuffer(const RingBuffer& other) : _size(other._size), _empty(other._empty) {
        _buf = new T[_size];

        size_t ofsHead = other._pHead - other._buf;
        size_t ofsTail = other._pTail - other._buf;
        _pHead = _buf + ofsHead;
        _pTail = _buf + ofsTail;

        size_t elToCopy = other.getCount();
        if (~_empty) {
            for (size_t i = 0; i < elToCopy; ++i) {
                _buf[ofsHead + i] = other._buf[ofsHead + i];
            }
        }
    }

    // operators
    RingBuffer& operator = (const RingBuffer& rhv) {
        if (this == &rhv) return *this;

        RingBuffer temp(rhv);
        swap(temp, *this);
        return *this;
    }

    // methods
    size_t getSize() const { return _size; }
    bool isEmpty() const { return _empty; }

    size_t getCount() const {
        if (_pHead == _pTail && _empty) {
            return 0;
        } else if (_pHead == _pTail && ~_empty) {
            return _size;
        } else if (_pHead < _pTail) {
            return _pTail - _pHead;
        } else if (_pHead > _pTail) {
            return _size - (_pHead - _pTail);
        } else {
            throw std::logic_error("Unexpected condition in getCount()");
        }
    }

    bool isFull() const { return _size == getCount(); } ;
    size_t getFree() const { return _size - getCount(); };

    void push(const T& value) {
        if (isFull()) throw std::out_of_range("Buffer is full");
        if (_empty) _empty = false;
        *_pTail = value;
        ++_pTail;

        if (_pTail == _buf + _size) _pTail = _buf;
    }

    T pop() {
        if (_empty) throw std::out_of_range("Buffer is empty");
        T poppedEl = *_pHead;
        _pHead++;

        if (_pHead == _buf + _size) _pHead = _buf;
        if (_pHead == _pTail) _empty = true;

        return poppedEl;
    }

    T& front() {
        if (_empty) throw std::out_of_range("Buffer is empty");
        return *_pHead;
    }

    const T& front() const {
        if (_empty) throw std::out_of_range("Buffer is empty");
        return *_pHead;
    }

    T& back() {
        if (_empty) throw std::out_of_range("Buffer is empty");
        if (_pTail == _buf) {
            return *(_buf + _size - 1);
        } else {
            return *(_pTail - 1);
        }
    }

    const T& back() const {
        if (_empty) throw std::out_of_range("Buffer is empty");
        if (_pTail == _buf) {
            return *(_buf + _size - 1);
        } else {
            return *(_pTail - 1);
        }
    }

    static void swap(RingBuffer& lhv, RingBuffer& rhv) {
        std::swap(lhv._buf, rhv._buf);
        std::swap(lhv._pTail, rhv._pTail);
        std::swap(lhv._pHead, rhv._pHead);
        std::swap(lhv._size, rhv._size);
        std::swap(lhv._empty, rhv._empty);
    }

private:
    // fields
    T* _buf;
    size_t _size;
    bool _empty;

    T* _pTail;  // points to the position where the next element wil be added
    T* _pHead;  // points to the oldest element
};

#endif //RING_BUFFER_RINGBUFF_H
