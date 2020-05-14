#!/usr/bin/env python
# coding: utf-8

# In[23]:


import torch
import torch.nn as nn
import numpy as np
import torch.utils.data as Data


# In[17]:


pytorch_device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")


# In[18]:


# data = np.loadtxt('data.csv', delimiter = ",", skiprows = 1)
# data = torch.FloatTensor(data)

# X, Y = data[:,1:], data[:,0]
# state = np.array([[i%49] for i in range(90*49)])
# X = np.column_stack((X, state))

# X = torch.FloatTensor(X)
# Y = torch.FloatTensor(Y)


# In[94]:


data = np.loadtxt('data2.csv', delimiter = ",", skiprows = 1)
X, Y = data[:,10:], data[:,1] # binned features  (column9: latitude not included)

Y = Y.astype(int)
Y -= 1

state = np.array([[i%49] for i in range(90*49)])
date = np.array([[i//49] for i in range(90*49)])

X = np.column_stack((X, state, date))

X = torch.FloatTensor(X)
Y = torch.FloatTensor(Y)

x_train = X[:-490*2]
y_train = Y[:-490*2]

x_test = X[-490*2:]
y_test = Y[-490*2:]


# In[95]:


BATCH_SIZE = 49 

torch_dataset_train = Data.TensorDataset(x_train, y_train)
torch_dataset_test = Data.TensorDataset(x_test, y_test)

train_loader = Data.DataLoader(
    dataset=torch_dataset_train,      
    batch_size=BATCH_SIZE,      
    shuffle=False            
)

test_loader = Data.DataLoader(
    dataset=torch_dataset_test,      
    batch_size=1,      
    shuffle=False
)


# In[96]:


len(test_loader)


# In[170]:


class LSTM(nn.Module):
    def __init__(self, input_size=1, hidden_layer_size=32, output_size=1):
        super().__init__()
        self.hidden_layer_size = hidden_layer_size

        self.lstm = nn.LSTM(input_size, hidden_layer_size)

        self.linear = nn.Linear(hidden_layer_size, output_size)

        self.hidden_cell = (torch.zeros(1, 1, self.hidden_layer_size).to(pytorch_device),
                            torch.zeros(1, 1, self.hidden_layer_size).to(pytorch_device))

    def forward(self, input_seq):
        lstm_out, self.hidden_cell = self.lstm(input_seq.view(len(input_seq) ,1, -1), self.hidden_cell)
        predictions = self.linear(lstm_out.view(len(input_seq), -1))
        return predictions[-1]


# In[97]:


class Activation_Net(nn.Module):
    def __init__(self, input_size=9, hidden_layer_size=8, output_size=1, batch_size=49):
        super().__init__()
        
        self.batch_size = batch_size
        
        self.hidden_layer_size = hidden_layer_size

        self.layer1 = nn.Sequential(nn.Linear(input_size, hidden_layer_size), nn.ReLU(True))

        self.linear = nn.Linear(hidden_layer_size, output_size)
        
    def forward(self, input_seq):
        x = self.layer1(input_seq)
        x = self.linear(x)
        return x


# In[98]:


model = Activation_Net().to(pytorch_device)
loss_function = nn.BCEWithLogitsLoss()
optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
print(model)


# In[99]:


epochs = 10

for i in range(epochs):
    for s, (batch_x, batch_y) in enumerate(train_loader):
        batch_x = batch_x.to(pytorch_device)
        batch_y = batch_y.to(pytorch_device)

        optimizer.zero_grad()
        y_pred = model(batch_x.view(1, BATCH_SIZE, -1))
        single_loss = loss_function(y_pred, batch_y.view(1, BATCH_SIZE, -1))
        single_loss.backward(retain_graph=True)
        optimizer.step()

    print(f'epoch: {i:3} loss: {single_loss.item():10.8f}')


# In[100]:


with torch.no_grad():
    correct = 0
    total = 0
    for step, (batch_x, batch_y) in enumerate(test_loader): 
        batch_x = batch_x.to(pytorch_device)
        batch_y = batch_y.to(pytorch_device)

        y_pred = model(batch_x.view(1, 1, -1))
        y_pred = 1 if torch.sigmoid(y_pred)>0.5 else 0
        if y_pred == batch_y:
            correct += 1
        total += 1
print(correct/total)


# In[ ]:




