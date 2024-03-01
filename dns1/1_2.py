from tqdm import tqdm
import numpy as np
import pandas as pd
import random
from xgboost import XGBClassifier
import xgboost as xgb

tot_fqdn = 20512

feature_all = []
label_all = [0] * tot_fqdn

cnt = 0
df = pd.read_csv('feature.csv')
for index,row in tqdm(df.iterrows()):
    label_all[cnt] = row[0]
    feature_all.append(row[1:])
    cnt += 1

train_feature_0 = [] 
train_label_0 = [] 

test_num_0 = [] 
test_feature_0 = []  

train_random_0 = [] 

for i in range(tot_fqdn):
    if label_all[i] != -1:
        train_feature_0.append(feature_all[i])
        train_label_0.append(1)

for i in range(tot_fqdn):
    if label_all[i] == -1:
        train_random_0.append([i, feature_all[i], 0])
        test_num_0.append(i)
        test_feature_0.append(feature_all[i])

random.shuffle(train_random_0) 

for i in range(len(train_random_0)):
    train_feature_0.append(train_random_0[i][1])
    train_label_0.append(train_random_0[i][2])

params = { 
    'booster': 'gbtree',
    'objective': 'binary:logistic',
    'gamma': 0.1,
    'max_depth': 6,
    'lambda': 2,
    'subsample': 0.8,
    'colsample_bytree': 0.8,
    'min_child_weight': 1,
    'eta': 0.05
}

dtrain = xgb.DMatrix(train_feature_0[:2000], train_label_0[:2000])
num_rounds = 500

model = xgb.train(params, dtrain, num_rounds)

dtest = xgb.DMatrix(test_feature_0)
test_label_0_float = model.predict(dtest)

test_num = []
test_feature = []

test_label_0 = [0] * len(test_label_0_float)
for i in range(len(test_label_0_float)):
    test_label_0[i] = round(test_label_0_float[i])
    if test_label_0[i] == 1:
        test_num.append(test_num_0[i])
        test_feature.append(test_feature_0[i])

train_feature = []
train_label = []

for i in range(tot_fqdn):
    if label_all[i] != -1:
        train_feature.append(feature_all[i])
        train_label.append(label_all[i])

params = { 
    'booster': 'gbtree',
    'objective': 'multi:softmax',
    'num_class': 9,
    'gamma': 0.1,
    'max_depth': 6,
    'lambda': 2,
    'subsample': 0.8,
    'colsample_bytree': 0.8,
    'min_child_weight': 1,
    'eta': 0.05
}

dtrain = xgb.DMatrix(train_feature, train_label)
num_rounds = 500

model = xgb.train(params, dtrain, num_rounds)

dtest = xgb.DMatrix(test_feature)
test_label = model.predict(dtest)

with open("result.csv", "w") as csvfile:
    for i in range(len(test_num)):
        csvfile.write("fqdn_" + str(test_num[i]) + "," + str(int(test_label[i])) + "\n")