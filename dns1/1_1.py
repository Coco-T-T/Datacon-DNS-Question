import json
import random
from tqdm import tqdm
import numpy as np
import math
import pandas as pd
from datetime import date, datetime
import datetime
from sklearn.ensemble import RandomForestClassifier

tot_fqdn = 20512
date_0 = date(2020, 3, 1)

df_access = pd.read_csv('access.csv')
# 访问次数
# 访问ip
# 按小时统计访问次数
# 按日期统计访问次数

l_ip = {}
df_ip = pd.read_csv('ip.csv')
for index,row in tqdm(df_ip.iterrows()):
    l_ip[row[0]] = row[1:]

df_ipv6 = pd.read_csv('ipv6.csv')
for index,row in tqdm(df_ipv6.iterrows()):
    l_ip[row[0]] = row[1:]

access_count = [0] * tot_fqdn
access_ip = [set() for _ in range(tot_fqdn)]
access_hour = [[0]*24 for _ in range(tot_fqdn)]
access_day = [[0]*100 for _ in range(tot_fqdn)]
access_ip_country = [set() for _ in range(tot_fqdn)] # 访问ip所在国家
access_ip_city = [set() for _ in range(tot_fqdn)] # 访问ip所在城市
access_ip_isp = [set() for _ in range(tot_fqdn)] # 访问ip的ISP

for index,row in tqdm(df_access.iterrows()):
    id = int(row[0][5:])
    ip = row[1]
    count = int(row[2])
    time = str(row[3])
    date_1 = datetime.datetime.strptime(time[:8], '%Y%m%d').date()
    access_count[id] += count
    access_ip[id].add(ip)
    access_hour[id][int(time[8:10])] += count
    access_day[id][(date_1 - date_0).days] += count
    if ip in l_ip.keys():
        match_ip = l_ip[ip]
        access_ip_country[id].add(match_ip[0])  # 访问ip的国家列表
        access_ip_city[id].add(match_ip[2])  # 访问ip的城市列表
        access_ip_isp[id].add(match_ip[5])  # 访问ip的ISP列表
    # if index > 10000 :
    #     break

df_flint = pd.read_csv('flint.csv')
# 被解析成的ip
# 被解析成的域名
# 按照日期请求请求次数

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

l_whois = []
with open('whois.json', 'r', encoding='utf-8') as f:
    f_whois = json.load(f)
    for row in f_whois:
        l_whois.append(row)

whois_create = [10**6] * tot_fqdn # 域名创建日期
whois_expire = [0] * tot_fqdn # 域名过期日期
whois_update = [set() for _ in range(tot_fqdn)]  # 域名更新日期

whois_nameserver = [set() for _ in range(tot_fqdn)]
whois_adminemail = [set() for _ in range(tot_fqdn)]
whois_registercountry = [set() for _ in range(tot_fqdn)]
whois_registeremail = [set() for _ in range(tot_fqdn)]
whois_registerstate = [set() for _ in range(tot_fqdn)]
whois_techemial = [set() for _ in range(tot_fqdn)]
whois_serverlist = [set() for _ in range(tot_fqdn)]
whois_server = [set() for _ in range(tot_fqdn)]
whois_sponsor = [set() for _ in range(tot_fqdn)]

for whois in l_whois:
    num = int(whois['fqdn_no'][5:]) # 域名编号
    if whois['createddate'] != None:
        whois_create[num] = min(int(whois['createddate']/86400000), whois_create[num])  # 域名创建日期
    if whois['expiresdate'] != None:
        whois_expire[num] = max(int(whois['expiresdate']/86400000), whois_expire[num])  # 域名过期日期
    if whois['updateddate'] != None:
        whois_update[num].add(int(whois['updateddate']))  # 域名更新日期
    if whois['nameservers'] != None:
        whois_nameserver[num].update(whois['nameservers'])
    if whois['admin_email'] != None:
        whois_adminemail[num].update(whois['admin_email'])  # 域名的管理员邮箱
    if whois['registrant_country'] != None:
        whois_registercountry[num].update(whois['registrant_country'])  # 域名的注册国家
    if whois['registrant_email'] != None:
        whois_registeremail[num].update(whois['registrant_email'])  # 域名的注册邮箱
    if whois['registrant_state'] != None:
        whois_registerstate[num].update(whois['registrant_state'])  # 域名的注册省份
    if whois['tech_email'] != None:
        whois_techemial[num].update(whois['tech_email'])  # 域名的注册邮箱
    if whois['r_whoisserver_list'] != None:
        whois_serverlist[num].update(whois['r_whoisserver_list'])  # 域名的 DNS 服务器列表
    if whois['whoisserver'] != None:
        whois_server[num].update(whois['whoisserver'])  # 域名的 DNS 服务器
    if whois['sponsoring'] != None:
        whois_sponsor[num].update(whois['sponsoring'])  # 域名的注册商

df_label = pd.read_csv('label.csv')

label_all = [-1] * tot_fqdn

for index,row in tqdm(df_label.iterrows()):
    id = int(row[0][5:])
    num = row[1]
    label_all[id] = num

feature_all = []
for i in tqdm(range(tot_fqdn)) :
    f_access_count = access_count[i] # 访问次数
    f_access_ip = len(access_ip[i]) # 访问ip数
    f_access_hour = access_hour[i]  # 按小时统计访问次数
    f_access_day = access_day[i]  # 按日期统计访问次数
    f_access_ip_country = len(access_ip_country[i])
    f_access_ip_city = len(access_ip_city[i])
    f_access_ip_isp = len(access_ip_isp[i])

    f_flint_ip = len(flint_ip[i]) # 解析成的ip次数
    f_flint_domain = len(flint_domain[i]) # 解析成的域名次数
    f_flint_count = flint_count_day[i]
    f_flint_ip_country = len(flint_ip_country[i])
    f_flint_ip_city = len(flint_ip_city[i])
    f_flint_ip_isp = len(flint_ip_isp[i])

    f_length = fqdn_length[i] # 长度
    f_ch_rate = fqdn_ch_rate[i] # 字符占比
    f_num_rate = fqdn_num_rate[i] # 数字占比
    f_word_rate = fqdn_word_rate[i] # 单词长度占比
    f_word_count = fqdn_word_count[i] # 单词个数
    f_dep = fqdn_dep[i] # 深度

    f_whois_create = whois_create[i]
    f_whois_expire = whois_expire[i]
    f_whois_update = len(whois_update[i]) 
    f_whois_nameserver = len(whois_nameserver[i]) 
    f_whois_adminemail = len(whois_adminemail[i])  
    f_whois_registercountry = len(whois_registercountry[i]) 
    f_whois_registeremail = len(whois_registeremail[i]) 
    f_whois_registerstate = len(whois_registerstate[i]) 
    f_whois_techemial = len(whois_techemial[i]) 
    f_whois_serverlist = len(whois_serverlist[i]) 
    f_whois_server = len(whois_server[i]) 
    f_whois_sponsor = len(whois_sponsor[i]) 

    tmp = [f_access_count, f_access_ip, f_access_ip_country, f_access_ip_city, f_access_ip_isp,
           f_flint_ip, f_flint_domain, f_flint_ip_country, f_flint_ip_city, f_flint_ip_isp,
           f_length, f_ch_rate, f_num_rate, f_word_rate, f_word_count, f_dep,
           f_whois_create, f_whois_expire, f_whois_update, f_whois_nameserver, f_whois_adminemail, f_whois_registercountry, f_whois_registeremail, f_whois_registerstate, f_whois_techemial, f_whois_serverlist, f_whois_server, f_whois_sponsor]
    tmp.extend(f_access_hour)
    tmp.extend(f_access_day)
    tmp.extend(f_flint_count)
    feature_all.append(tmp)

cnt = 0
with open("feature.csv", "w") as outputfile:
    outputfile.write("label,f_access_count,f_access_ip,f_access_ip_country,f_access_ip_city,f_access_ip_isp,f_flint_ip,f_flint_domain,f_flint_ip_country,f_flint_ip_city,f_flint_ip_isp,f_length,f_ch_rate,f_num_rate,f_word_rate,f_word_count,f_dep,")
    outputfile.write("f_whois_create,f_whois_expire,f_whois_update,f_whois_nameserver,f_whois_adminemail,f_whois_registercountry,f_whois_registeremail,f_whois_registerstate,f_whois_techemial,f_whois_serverlist,f_whois_server,f_whois_sponsor")
    for _ in range(24):
        outputfile.write(",ahour"+str(_))
    for _ in range(100):
        outputfile.write(",aday"+str(_))
    for _ in range(100):
        outputfile.write(",fday"+str(_))
    outputfile.write("\n")
    for i in label_all :
        outputfile.write(f"{i}")
        for j in feature_all[cnt] :
            outputfile.write(f",{j}")
        outputfile.write("\n")
        cnt += 1