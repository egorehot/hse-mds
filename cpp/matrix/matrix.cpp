#include "matrix.h"

Matrix::Matrix() : Matrix(0, 0, 0.0) {};

Matrix::Matrix(size_t m, size_t n, double def) {
    resize(m, n, def);
};

Matrix::MatrixVector Matrix::createVector(size_t sz, double val) {
    MatrixVector matrixVector(sz, val);
    return matrixVector;
};

void Matrix::resize(size_t m, size_t n, double def) {
    _matrix.resize(m, createVector(n, def));
};

const Matrix::MatrixVector& Matrix::operator[] (const size_t i) const {
    if (i > _matrix.size()) {
        throw std::out_of_range("There is no such a row");
    }
    return _matrix[i];
};

double Matrix::getEl(size_t i, size_t j) const {
    if (i > _matrix.size()) {
        throw std::out_of_range("There is no such a row");
    }
    if (j > _matrix[i].size()) {
        throw std::out_of_range("There is no such a column");
    }
    return _matrix[i][j];
};

void Matrix::setEl(size_t i, size_t j, double val) {
    if (i > _matrix.size()) {
        throw std::out_of_range("There is no such a row");
    }
    if (j > _matrix[i].size()) {
        throw std::out_of_range("There is no such a column");
    }
    _matrix[i][j] = val;
}

bool Matrix::operator==(const Matrix& rhv) const {
    return _matrix == rhv._matrix;
}

Matrix Matrix::operator+(const Matrix& rhv) const {
    if (getSize() != rhv.getSize()) {
        throw std::invalid_argument("Matrix sizes are incompatible");
    }
    Matrix::SizePair m_n = getSize();
    Matrix result(m_n.first, m_n.second, 0.0);
    for (size_t i = 0; i < m_n.first; ++i) {
        for (size_t j = 0; j < m_n.second; ++j) {
            result.setEl(i, j, getEl(i, j) + rhv.getEl(i, j));
        }
    }

    return result;
};

Matrix operator*(const Matrix& m, double k) {
    Matrix result = m;
    Matrix::SizePair m_n = m.getSize();
    for (size_t i = 0; i < m_n.first; ++i) {
        for (size_t j = 0; j < m_n.second; ++j) {
            result.setEl(i, j, m.getEl(i, j) * k);
        }
    }
    return result;
}

Matrix operator*(double k, const Matrix& m) {
    return m * k;
}
