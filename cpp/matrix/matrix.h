#ifndef INC_4WEEK_MATRIX_MATRIX_H
#define INC_4WEEK_MATRIX_MATRIX_H

#include <vector>

class Matrix {
public:
    typedef std::vector<double> MatrixVector;
    typedef std::vector<MatrixVector> VectorOfMatrixVector;
    typedef std::pair<size_t, size_t> SizePair;

    Matrix();
    Matrix(size_t m, size_t n, double def);

    static MatrixVector createVector(size_t sz, double val);
    void resize(size_t m, size_t n, double def);

    SizePair getSize() const { return SizePair(_matrix.size(), _matrix.empty() ? 0 : _matrix[0].size()); }

    double getEl(size_t i, size_t j) const;
    void setEl(size_t i, size_t j, double val);

    const MatrixVector& operator[] (size_t i) const;
    bool operator==(const Matrix& rhv) const;
    bool operator!=(const Matrix& rhv) const { return !(*this == rhv); }
    Matrix operator+(const Matrix& rhv) const;

    friend Matrix operator*(const Matrix& m, double k);
    friend Matrix operator*(double k, const Matrix& m);

private:
    VectorOfMatrixVector _matrix;
};

Matrix operator*(const Matrix& m, double k);
Matrix operator*(double k, const Matrix& m);

#endif //INC_4WEEK_MATRIX_MATRIX_H
