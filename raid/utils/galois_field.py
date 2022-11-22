import numpy as np


class GaloisField(object):
    def __init__(self, config, w=8, polynomial=0x11d):
        self.config = config
        self.gf_len = 1 << w
        self.polynomial = polynomial
        self.__build_tables()
        self.__build_Vmat()

    def __len__(self):
        return self.gf_len

    def __build_tables(self):
        self.gflog = np.zeros(self.gf_len, dtype=np.int)
        self.gfilog = np.zeros(self.gf_len, dtype=np.int)
        x = 1
        for i in range(self.gf_len):
            self.gflog[x] = i
            self.gfilog[i] = x
            x = x << 1
            if x & self.gf_len:
                x ^= self.polynomial

    def __build_Vmat(self):
        self.Vmat = np.zeros(
            (self.config['parity_disks_num'], self.config['data_disks_num']), dtype=np.int)
        for i in range(self.config['parity_disks_num']):
            for j in range(self.config['data_disks_num']):
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

    def matmul(self, X, Y):
        if X.shape[1] != Y.shape[0]:
            raise ValueError
        result = np.zeros((X.shape[0], Y.shape[1]), dtype=np.int)
        for i in range(X.shape[0]):
            for j in range(Y.shape[1]):
                result[i, j] = self.__dot(X[i, :], Y[:, j])
        return result

    def inv(self, X):
        # if X == 0:
        #     raise ZeroDivisionError
        # if X.shape[0] != X.shape[1]:
        #     X_T = np.transpose(X)
        if X.shape[0] != X.shape[1]:
            X_T = np.transpose(X)
            X_ = self.matmul(X_T, X)
        else:
            X_ = X
        X_ = np.concatenate((X_, np.eye(X_.shape[0], dtype=int)), axis=1)
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
                X_[j, :] = self.__add(X_[j, :],  list(
                    map(self.__mul, X_[i, :], [self.__div(X_[j, i], X_[i, i])] * len(X_[i, :]))))
        for i in reversed(range(dim)):
            for j in range(i):
                X_[j, :] = self.__add(X_[j, :], list(
                    map(self.__mul, X_[i, :], [X_[j, i]] * len(X_[i, :]))))
        X_inv = X_[:, dim:2*dim]
        if X.shape[0] != X.shape[1]:
            X_inv = self.matmul(X_inv, X_T)
        return X_inv
