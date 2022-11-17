import numpy as np

class GaloisField(object):
    def __init__(self, config, w=8, modulus=0b100011101):
        self.config = config
        self.w = w
        self.x_to_w = 1 << w 
        self.modulus = modulus
        self.gflog, self.gfilog = self.build_log_table()
        self.vender_mat = self.build_vender_mat()
        
    def build_log_table(self):
        gflog = np.zeros(self.x_to_w, dtype=np.int)
        gfilog = np.zeros(self.x_to_w, dtype=np.int)
        x = 1
        for i in range(self.x_to_w - 1):
            gflog[x] = i
            gfilog[i] = x
            x = x << 1
            if x & self.x_to_w:
                x = x ^ self.modulus
        return gflog, gfilog
    
    def build_vender_mat(self):
        vender_mat = np.zeros((self.config['parity_disks_num'], self.config['data_disks_num']), dtype=np.int)
        for i in range(self.config['parity_disks_num']):
            for j in range(self.config['data_disks_num']):
                vender_mat[i][j] = self.power(j+1, i)
        return vender_mat
        
    def add(self, x, y):
        return x ^ y
    
    def sub(self, x, y):
        return x ^ y
    
    def power(self, x, n):
        n %= self.x_to_w - 1
        result = 1
        while True:
            if n == 0:
                return result
            n -= 1
            result = self.multiply(x, result)

    def multiply(self, x, y):
        if x == 0 or y == 0:
            return 0
        sum_log = self.gflog[x] + self.gflog[y]
        if sum_log >= self.x_to_w - 1:
            sum_log -= self.x_to_w - 1
        return self.gfilog[sum_log]
        
    def divide(self, x, y):
        if y == 0:
            raise ZeroDivisionError
        if x == 0:
            return 0
        diff_log = self.gflog[x] - self.gflog[y]
        if diff_log < 0:
            diff_log += self.x_to_w - 1
        return self.gfilog[diff_log]
    
    def dot(self, x, y):
        if x.size != y.size:
            raise ValueError
        result = 0
        for i in range(x.size):
            result = self.add(result, self.multiply(x[i], y[i]))
        return result
    
    def matmul(self, X, Y):
        if X.shape[1] != Y.shape[0]:
            raise ValueError
        result = np.zeros((X.shape[0], Y.shape[1]), dtype=np.int)
        for i in range(X.shape[0]):
            for j in range(Y.shape[1]):
                result[i, j] = self.dot(X[i, :], Y[:, j])
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
                X_[i, :] = list(map(self.add, X_[i, :], X_[j, :]))
            X_[i, :] = list(map(self.divide, X_[i, :], [X_[i, i]] * len(X_[i, :])))
            for j in range(i+1, dim):
                X_[j, :] = self.add(X_[j, :],  list(map(self.multiply, X_[i, :], [self.divide(X_[j, i], X_[i, i])] * len(X_[i, :]))))
        for i in reversed(range(dim)):
            for j in range(i):
                X_[j, :] = self.add(X_[j, :], list(map(self.multiply, X_[i, :], [X_[j, i]] * len(X_[i, :]))))
        X_inv = X_[:, dim:2*dim]
        if X.shape[0] != X.shape[1]:
            X_inv = self.matmul(X_inv, X_T)
        return X_inv
