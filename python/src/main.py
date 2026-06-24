import numpy as np

class SimpleNN:
    def __init__(self, input_dim, hidden_dim, output_dim):
        # initialize weights and biases
        self.input_dim =input_dim
        self.hidden_dim =hidden_dim
        self.output_dim =output_dim
        self.Wih =np.zeros((input_dim, hidden_dim))
        self.Who =np.zeros((hidden_dim, output_dim))
        self.bih =np.zeros((hidden_dim, 1))
        self.bho =np.zeros((output_dim, 1))
    
    def forward(self, X):
        # X: (n, input_dim)
        # return: (n, output_dim)
        n, in_dim =X.shape
        assert in_dim == self.input_dim, "input dim of X size not correct "
        y1 = X @ self.Wih  + self.bih
        self.y2 = self.ReLu(y1)
        self.y3 = self.y2 @ self.Who + self.bho
        self.y4 = 1 /(1 + np.exp(-self.y3))
        return self.y4
    
    def backward(self, X, y_true, learning_rate):
        # update weights using gradient descent
        B1, input_dim  = X.shape
        assert input_dim== self.input_dim, 'input dim not correct'
        B2, output_dim = y_true.shape
        assert B1 ==B2, 'X and y_ture should have same batch number'
        assert output_dim ==self.output_dim, 'X and y_ture should have same batch number'
        L_y3 = 1.0/output_dim * (self.y4 - y_true)
        L_Who = self.y2.T @ L_y3
        L_bho = np.sum(L_y3, axis=0, keepdims=True)
        L_y2 = L_y3 @ self.Who
        L_y1 = L_y2 * (self.y1 > 0)  
        L_Wih =X.T @ L_y1
        L_bih = np.sum(L_y1, axis=0, keepdims=True)
    
        self.Who =self.Who - learning_rate * L_Who
        self.bho =self.bho - learning_rate * L_bho
        self.Wih =self.Wih - learning_rate * L_Wih
        self.bih =self.bih - learning_rate * L_bih
        



    def ReLu(self, X):
        return np.maximum(X, 0, keepdims =True)
# Architecture:
# Input → Linear → ReLU → Linear → Sigmoid → output
# Loss: binary cross entropy
class KNN:
    def __init__(self, k):
        self.k =k
        
    
    def fit(self, X_train, y_train):
        self.X_train = X_train  # shape: (n_samples, n_features)
        self.y_train = y_train  # shape: (n_samples,)
    
    def predict(self, X_test):
        # return predicted labels for each test sample
        sq1 =np.sum(X_test**2, axis =1).reshape(-1, 1)
        sq2 =np.sum(self.X_train ** 2, axis=1).reshape(-1, 1)
        score =sq1 + sq2.T -2 * X_test @ self.X_train.T
        idx = np.argsort(score)[:, :self.k]; 
        arr =[]
        for i in range(X_test.shape[0]):
            y_train_i = self.y_train[idx[i]]
            dic, counts =np.unique(y_train_i, return_counts =True)
            arr.append(dic[np.argmax(counts)])

        return np.array(arr)

def multi_head_attention(Q, K, V, num_heads):
    """
    Q, K, V: (batch, seq_len, d_model)
    num_heads: number of attention heads
    assume d_model % num_heads == 0
    return: (batch, seq_len, d_model)
    """
    pass

