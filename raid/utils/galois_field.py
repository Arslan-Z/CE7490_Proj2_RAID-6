import numpy as np
from numba import njit, prange
from numba import int32, int64
from numba.experimental import jitclass

spec = [
    ('gf_len', int32),
    ('polynomial', int32),
    ('gflog', int64[:]),
    ('gfilog', int64[:]),
    ('Vmat', int64[:, :]),
]


@jitclass(spec)
class GaloisField(object):
    def __init__(self, k=4, m=3, w=8, polynomial=0x11d):
        self.gf_len = 1 << w
        self.polynomial = polynomial
        # self.use_numba = use_numba
        self.gflog = np.zeros(self.gf_len, dtype=np.int64)
        self.gfilog = np.zeros(self.gf_len, dtype=np.int64)
        self.__build_tables()
        self.Vmat = np.zeros((m, k), dtype=np.int64)
        self.__build_Vmat(k, m)

    def __len__(self):
        return self.gf_len

    def __build_tables(self):
        x = 1
        for i in range(self.gf_len):
            self.gflog[x] = i
            self.gfilog[i] = x
            x = x << 1
            if x & self.gf_len:
                x ^= self.polynomial

    def __build_Vmat(self, k=4, m=3):
        for i in range(m):
            for j in range(k):
                self.Vmat[i][j] = self.__power(j+1, i)

    def __add(self, x, y):
        return x ^ y

    def __sub(self, x, y):
        return x ^ y

    def __power(self, x, n):
        n %= self.gf_len - 1
        result = 1
        while True:
            if n == 0:
                return result
            n -= 1
            result = self.__mul(x, result)

    def __mul(self, x, y):
        if x == 0 or y == 0:
            return 0
        else:
            return self.gfilog[(self.gflog[x]+self.gflog[y]) % (self.gf_len-1)]

    def __div(self, x, y):
        if y == 0:
            raise ZeroDivisionError
        if x == 0:
            return 0
        diff_log = self.gflog[x] - self.gflog[y]
        if diff_log < 0:
            diff_log += self.gf_len - 1
        return self.gfilog[diff_log]

    def __dot(self, x, y):
        assert x.size == y.size, "Size unmatched to conduct dot product"
        result = 0
        for i in range(x.size):
            result = self.__add(result, self.__mul(x[i], y[i]))
        return result

    def matmul_3d(self, X, mat_3d, output_shape):
        return matmul_3d_p(self, X, mat_3d, output_shape)

    def matmul(self, X, Y):
        return matmul_p(self, X, Y)

    def inv(self, X):
        if X.shape[0] != X.shape[1]:
            X_T = np.transpose(X)
            X_ = self.matmul(X_T, X)
        else:
            X_ = X
        X_ = np.concatenate((X_, np.eye(X_.shape[0], dtype=np.int64)), axis=1)
        dim = X_.shape[0]
        for i in range(dim):
            if not X_[i, i]:
                for j in range(i+1, dim):
                    if X_[j, i]:
                        break
                X_[i, :] = list(map(self.__add, X_[i, :], X_[j, :]))
            X_[i, :] = list(map(self.__div, X_[i, :], [
                            X_[i, i]] * len(X_[i, :])))
            for j in range(i+1, dim):
                X_[j, :] = self.__add(X_[j, :],  np.array(list(
                    map(self.__mul, X_[i, :], [self.__div(X_[j, i], X_[i, i])] * len(X_[i, :]))), dtype=np.int64))
        for i in range(dim-1, -1, -1):
            for j in range(i):
                X_[j, :] = self.__add(X_[j, :], np.array(list(
                    map(self.__mul, X_[i, :], [X_[j, i]] * len(X_[i, :]))), dtype=np.int64))
        X_inv = X_[:, dim:2*dim]
        if X.shape[0] != X.shape[1]:
            X_inv = self.matmul(X_inv, X_T)
        return X_inv


@njit
def matmul_3d_p(GF, X, mat_3d, output_shape):
    result = np.zeros(output_shape, dtype=np.int64)
    for i in range(len(mat_3d)):
        result[i] = matmul_p(GF, X, mat_3d[i])
    return result


@njit(parallel=True)
def matmul_p(GF, X, Y):
    if X.shape[1] != Y.shape[0]:
        raise ValueError
    result = np.zeros((X.shape[0], Y.shape[1]), dtype=np.int64)
    for i in prange(X.shape[0]):
        for j in range(Y.shape[1]):
            result[i, j] = __dot(GF, X[i, :], Y[:, j])
    return result


@njit()
def __dot(GF, x, y):
    assert x.size == y.size, "Size unmatched to conduct dot product"
    result = 0
    for i in prange(x.size):
        result = __add(result, __mul(GF, x[i], y[i]))
    return result


@njit
def __mul(GF, x, y):
    if x == 0 or y == 0:
        return 0
    else:
        return GF.gfilog[(GF.gflog[x]+GF.gflog[y]) % (GF.gf_len-1)]

@njit
def __add(x, y):
    return x ^ y
