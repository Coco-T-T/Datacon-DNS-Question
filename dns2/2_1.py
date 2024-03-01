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
date_0 = date(2020, 3, 1)

df_access = pd.read_csv('access.csv')
# 按小时统计ip
# 按小时统计request
# 按日期统计ip
# 按日期统计request
# 全网访问量

access_ip_hour = [[0]*24 for _ in range(tot_fqdn)]
access_ip_day = [[0]*100 for _ in range(tot_fqdn)]
access_req_hour = [[0]*24 for _ in range(tot_fqdn)]
access_req_day = [[0]*100 for _ in range(tot_fqdn)]

for index,row in tqdm(df_access.iterrows()):
    id = int(row[0][5:])
    ip = int(row[1])
    req = int(row[2])
    date_1 = datetime.datetime.strptime(str(row[4]), '%Y%m%d').date()
    hour = int(row[5])
    access_ip_hour[id][hour] += ip
    access_ip_day[id][(date_1 - date_0).days] += ip
    access_req_hour[id][hour] += req
    access_req_day[id][(date_1 - date_0).days] += req
    # if index > 10000 :
    #     break

df_flint = pd.read_csv('flint.csv')
# 被解析成的ip
# 被解析成的域名
# 按日期统计请求次数

l_ip = {}
df_ip = pd.read_csv('ip.csv')
for index,row in tqdm(df_ip.iterrows()):
    l_ip[row[0]] = row[1:]

df_ipv6 = pd.read_csv('ipv6.csv')
for index,row in tqdm(df_ipv6.iterrows()):
    l_ip[row[0]] = row[1:]

flint_ip = [set() for _ in range(tot_fqdn)]
flint_domain = [set() for _ in range(tot_fqdn)]
flint_count_day = [[0]*100 for _ in range(tot_fqdn)]
flint_ip_country = [set() for _ in range(tot_fqdn)] # 解析ip所在国家
flint_ip_city = [set() for _ in range(tot_fqdn)] # 解析ip所在城市
flint_ip_isp = [set() for _ in range(tot_fqdn)] # 解析ip的ISP

for index,row in tqdm(df_flint.iterrows()):
    id = int(row[0][5:])
    result = row[2]
    count = int(row[3])
    date_1 = datetime.datetime.strptime(str(row[4]), '%Y%m%d').date()
    if (result[0:4] == 'fqdn'):
        flint_domain[id].add(result[5:])
    else:
        flint_ip[id].add(result)
        if result in l_ip.keys():
            match_ip = l_ip[result]
            flint_ip_country[id].add(match_ip[0])  # 解析ip的国家列表
            flint_ip_city[id].add(match_ip[2])  # 解析ip的城市列表
            flint_ip_isp[id].add(match_ip[5])  # 解析ip的ISP列表
    flint_count_day[id][(date_1 - date_0).days] += count
    # if index > 10000 :
    #     break

df_fqdn = pd.read_csv('fqdn.csv')
# 长度
# 字符占比
# 数字占比
# 单词个数
# 单词长度占比
# 深度

fqdn_length = [0] * tot_fqdn
fqdn_ch_rate = [0] * tot_fqdn
fqdn_num_rate = [0] * tot_fqdn
fqdn_word_rate = [0] * tot_fqdn
fqdn_word_count = [0] * tot_fqdn
fqdn_dep = [0] * tot_fqdn

for index,row in tqdm(df_fqdn.iterrows()):
    id = int(row[1][5:])
    name = row[0]
    fqdn_length[id] = len(name)
    ch = 0
    num = 0
    word = 0
    word_len = 0
    dep = 0
    flag = 0
    for i in name:
        if i == 'a' :
            ch += 1
            if flag == 1 :
                word_len += 1
        elif i == '0' :
            num += 1
        elif i == '[' :
            word += 1
            flag = 1
        elif i == '.' :
            dep += 1
        elif i == ']':
            flag = 0
    fqdn_ch_rate[id] = ch / len(name)
    fqdn_num_rate[id] = num / len(name)
    fqdn_word_rate[id] = word_len / len(name)
    fqdn_word_count[id] = word
    fqdn_dep[id] = dep

df_label = pd.read_csv('label.csv')

label_all = [-1] * tot_fqdn

for index,row in tqdm(df_label.iterrows()):
    id = int(row[0][5:])
    num = row[1]
    label_all[id] = num

feature_all = []
for i in tqdm(range(tot_fqdn)) :
    f_access_ip_hour = access_ip_hour[i] # 按小时统计访问ip
    f_access_ip_day = access_ip_day[i] # 按日期统计访问ip
    f_access_req_hour = access_req_hour[i] # 按小时统计req
    f_access_req_day = access_req_day[i] # 按日期统计req
    f_flint_ip = len(flint_ip[i]) # 解析成的ip次数
    f_flint_domain = len(flint_domain[i]) # 解析成的域名次数
    f_flint_count_day = flint_count_day[i] # 按日期统计解析次数
    f_ip_country = len(flint_ip_country[i]) # ip国家数
    f_ip_city = len(flint_ip_city[i]) # ip城市数
    f_ip_isp = len(flint_ip_isp[i]) # ip的ISP数
    f_length = fqdn_length[i] # 长度
    f_ch_rate = fqdn_ch_rate[i] # 字符占比
    f_num_rate = fqdn_num_rate[i] # 数字占比
    f_word_rate = fqdn_word_rate[i] # 单词长度占比
    f_word_count = fqdn_word_count[i] # 单词个数
    f_dep = fqdn_dep[i] # 深度

    tmp = [f_flint_ip, f_flint_domain, f_ip_country, f_ip_city, f_ip_isp,
           f_length, f_ch_rate, f_num_rate, f_word_rate, f_word_count, f_dep]
    tmp.extend(f_access_ip_hour)
    tmp.extend(f_access_ip_day)
    tmp.extend(f_access_req_hour)
    tmp.extend(f_access_req_day)
    tmp.extend(f_flint_count_day)
    feature_all.append(tmp)

cnt = 0
with open("feature.csv", "w") as outputfile:
    outputfile.write("label,f_flint_ip,f_flint_domain,f_ip_country,f_ip_city,f_ip_isp,f_length,f_ch_rate,f_num_rate,f_word_rate,f_word_count,f_dep")
    for _ in range(24):
        outputfile.write(",aihour"+str(_))
    for _ in range(100):
        outputfile.write(",aiday"+str(_))
    for _ in range(24):
        outputfile.write(",arhour"+str(_))
    for _ in range(100):
        outputfile.write(",arday"+str(_))
    for _ in range(100):
        outputfile.write(",fday"+str(_))
    outputfile.write("\n")
    for i in label_all :
        outputfile.write(f"{i}")
        for j in feature_all[cnt] :
            outputfile.write(f",{j}")
        outputfile.write("\n")
        cnt += 1