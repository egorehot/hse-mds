# Matrix

In this practical task, you are asked to develop a custom data structure — a class for working with matrices of real numbers. The task is divided into several stages. After completing each stage, you must submit the result for manual verification (SGA). For each stage on the platform, there is a separate block for submit. Using a separate class for such a complex entity as a matrix gives us the following advantages.
- representing a matrix as a mature solid object; - it is not enough to have an array of arrays, because it can also represent a jagged array, which is not appropriate for rectangular matrices;
- making it consistent;
- impossibility to accidentally have rows of different lengths;
- convenient user interface including basic matrix operations;
- defining factory methods for creating some standard matrices.

The new class should be named Matrix, and its definition should be placed in the module with the same name, divided into interface and implementation parts: matrix.h and matrix.cpp, respectively.
