from html.entities import codepoint2name
import json
import random
from tqdm import tqdm
import numpy as np
import math
import pandas as pd
from datetime import date, datetime
import datetime
from sklearn.ensemble import RandomForestClassifier
from sklearn.cluster import KMeans

tot_fqdn = 17468

feature_all = []
label_all = [0] * tot_fqdn

cnt = 0
df = pd.read_csv('feature.csv')
for index,row in tqdm(df.iterrows()):
    label_all[cnt] = row[0]
    tmp = []
    tmp.extend(row[1:9])
    tmp.extend(row[11:14])
    feature_all.append(tmp)
    cnt += 1

kmeans = KMeans(n_clusters=5)
kmeans.fit(feature_all)

cnt = [0] * 5

with open("result.csv", "w") as file:
    file.write("fqdn_no,label\n")
    for i in tqdm(range(tot_fqdn)):
        file.write("fqdn_" + str(i) + "," + str(int(kmeans.labels_[i])) + "\n")
        cnt[kmeans.labels_[i]] += 1

for i in range(5):
    print(cnt[i])
