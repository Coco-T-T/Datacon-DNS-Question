import json
import random
from tqdm import tqdm
import numpy as np
import math
import pandas as pd
from datetime import date, datetime
import datetime
from sklearn.ensemble import RandomForestClassifier

tot_fqdn = 17468

feature_all = []
label_all = [0] * tot_fqdn

cnt = 0
df = pd.read_csv('feature.csv')
for index,row in tqdm(df.iterrows()):
    label_all[cnt] = row[0]
    feature_all.append(row[1:])
    cnt += 1

train_num = [] # 恶意域名训练集编号
train_feature = [] # 恶意域名训练集特征
train_label = []  # 恶意域名训练集标签

for i in range(tot_fqdn):
    if label_all[i] != -1:
        train_num.append(i)
        train_feature.append(feature_all[i])
        train_label.append(label_all[i]) # 恶意样本

clf = RandomForestClassifier(random_state = 0)
clf.fit(train_feature,train_label)

# 二分类
with open("result.csv", "w") as outputfile:
    for i in tqdm(range(tot_fqdn)):
        if label_all[i] == -1:
            ans = clf.predict([feature_all[i]])[0]
            if ans == 1:
                outputfile.write("fqdn_" + str(i) + "," + str(int(ans)) + "\n")