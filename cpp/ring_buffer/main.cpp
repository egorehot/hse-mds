#include <iostream>
#include "ringbuff.h"

#include <chrono>
#include <random>
#include <cassert>

void printRingBuffState(const RingBuffer<int>& r) {
    std::cout << "Size " << r.getSize() << std::endl;
    std::cout << "Count " << r.getCount() << std::endl;
    std::cout << "Free " << r.getFree() << std::endl;
    std::cout << "Empty " << std::boolalpha << r.isEmpty() << std::endl;
    std::cout << "Full " << std::boolalpha << r.isFull() << std::endl;
    std::cout << std::endl;
}

void printFrontBack(const RingBuffer<int>& r) {
    if (r.isEmpty()) {
        std::cout << "Buffer is empty!" << std::endl;
        return;
    }
    std::cout << "Front " << r.front() << std::endl;
    std::cout << "Back " << r.back() << std::endl;
    std::cout << std::endl;
}

int main() {
//    RingBuffer<int> t1(3);
//    printRingBuffState(t1);
//    t1.push(1);
//    printFrontBack(t1);
//
//    printRingBuffState(t1);
//    t1.push(2);
//    printFrontBack(t1);
//
//    {
//        int p = t1.pop();
//        std::cout << "Popped " << p << std::endl;
//    }
//    printFrontBack(t1);
//
//    {
//        int p = t1.pop();
//        std::cout << "Popped " << p << std::endl;
//    }
//    printFrontBack(t1);

//  test copying
    std::random_device rd;
    std::mt19937 gen(rd());
    int lower_bound = 1;
    int upper_bound = 10000;
    std::uniform_int_distribution<int> dist(lower_bound, upper_bound);

    RingBuffer<int> t2(5000000);
    size_t nEl = t2.getSize() / 10;
    for (size_t i = 0; i < nEl; ++i) {
        t2.push(dist(gen));
    }
    printRingBuffState(t2);

    int nIter = 10;
    double totalSec = 0;
    for (int i = 0; i < nIter; ++i) {
        auto start = std::chrono::high_resolution_clock::now();
        RingBuffer<int> t3 = t2;
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> duration = end - start;
        totalSec += duration.count();
    }
    std::cout << "Average execution time " << (totalSec / nIter) << std::endl;
    RingBuffer<int> t3 = t2;
    for (size_t i = 0; i < nEl; ++i) {
        assert(t2.pop() == t3.pop());
    }

    return 0;
}
