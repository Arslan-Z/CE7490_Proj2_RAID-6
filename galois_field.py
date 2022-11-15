import numpy as np

class GaloisField(object):
    def __init__(self, config, w=8, modulus=0b100011101):
        self.config = config
        self.w = w
        self.x_to_w = 1 << w 
        self.modulus = modulus
        self.gflog, self.gfilog = self.build_log_table()
        
        
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
    
    def add(self, x, y):
        return x ^ y
    
    def sub(self, x, y):
        return x ^ y
    
    def multiply(self, x, y):
        if x == 0 or y == 0:
            return 0
        return self.gfilog[(self.gflog[x] + self.gflog[y]) % (self.x_to_w - 1)]
        
    def divide(self, x, y):
        if y == 0:
            raise ZeroDivisionError
        if x == 0:
            return 0
        return self.gfilog[(self.gflog[x] + self.x_to_w - self.gflog[y]) % (self.x_to_w - 1)]
    
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
    
    def inverse(self, X):
        if X == 0:
            raise ZeroDivisionError
        if X.shape[0] != X.shape[1]:
            X_T = np.transpose(X)
            
