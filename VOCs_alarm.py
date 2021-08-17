# -*- coding: utf-8 -*-
"""
Created on Thu Jul  1 11:01:53 2021

@author: wangxh
"""

import pandas as pd


def count_eff(df_raw1,stations,pollutants):
    """
    有效数据的计数，返回每个站点的每个污染物在这段时间内的有效数据个数。
    数据的时间分别率为小时。

    Parameters
    ----------
    df_raw1 : pd.DataFrame
        原始数据
    stations : list
        站点名称列表
    pollutants : list
        污染物名称列表

    Returns
    -------
    df_count_eff : pd.DataFrame
        有效数据个数

    """
    df_count_eff = pd.DataFrame(index=stations,columns=pollutants,dtype=float)
    for ii in range(len(stations)):
        df_temp = df_raw1[df_raw1['站点']==stations[ii]]
        for jj in range(len(pollutants)):
            df_count_eff.iloc[ii,jj] = df_temp.iloc[:,jj+2].notnull().sum()
    # df_count_eff[df_count_eff == 0] = pd.NA #如果是0就设为NaN,防止出现除0的现象
    return df_count_eff



def cal_eff_ratio(df,stations,pollutants):
    """
    计算数据有效率，返回每个站点的每个污染物在这段时间内的数据有效率。
    数据的时间分别率为小时。
    Parameters
    ----------
    df : pd.DataFrame
        原始数据
    stations : list
        站点名称列表
    pollutants : list
        污染物名称列表

    Returns
    -------
    df_eff_ratio : pd.DataFrame
        数据有效率
    """
    df_eff_ratio = pd.DataFrame(index=stations,columns=pollutants,dtype=float)
    for ii in range(len(stations)):
        df_temp = df[df['站点']==stations[ii]]        
        for jj in range(len(pollutants)):
            df_eff_ratio.iloc[ii,jj] = df_temp.iloc[:,jj+2].notnull().sum()/len(df_temp)

    return df_eff_ratio


def count_alarm(df_raw,stations,p_threshold):
    """
    报警次数的计数，返回每个站点每个污染物（需要有浓度限值）在这段时间内的报价次数。
    数据的时间分别率为小时。
    Parameters
    ----------
    df_raw : pd.DataFrame
        原始数据
    stations : list
        站点名称列表
    p_threshold : dictionary
        记录污染物名称和对应的浓度限值
        {污染物名称：浓度限值}

    Returns
    -------
    df_alarm : pd.DataFrame
        报警次数

    """
    
    df_raw = df_raw.fillna(0)
    df_alarm = pd.DataFrame(index=stations,columns=p_threshold.keys(),dtype=int)
    df_alarm.iloc[:,:] = 0
    for ii in range(len(stations)):
        df_temp = df_raw[df_raw['站点']==stations[ii]]
        jj = 0
        for pp,value in p_threshold.items():
            df_temp.loc[:,pp][df_temp.loc[:,pp]<=value] = 0
            df_temp.loc[:,pp][df_temp.loc[:,pp]>value] = 1 
            df_alarm.iloc[ii,jj] = df_temp[pp].sum()    
            jj = jj + 1
            
    return df_alarm

#==================主程序从这里开始=========================

# 读取两个文件
period = "2021年6月1日-6月30日化工区"
df1 = pd.read_excel("./input/测试-VOCs报警统计-202106.xlsx",skiprows=[1]) #VOCs表，去掉第二行的单位
df2 = pd.read_excel("./input/测试-硫化氢和氨报警统计-202106.xlsx",skiprows=[1]) #硫化氢和氨的表，去掉第二行的单位
df3 = pd.read_excel("./input/11个报警因子及限值.xlsx",header=0) #11种污染物的限值列表

#合并（表VOCs）和（表硫化氢和氨）
df = pd.concat([df1,df2.iloc[:,2:]],axis=1)
df.to_excel("./output/"+period+"47种污染物原始数据.xlsx")
p47 = df.columns[2:] #提取所有物种的名称列表 47个
stations = df['站点'].drop_duplicates().tolist() #提取所有站点名字

# #提取报警因子
# p11_threshold ={}
# for i in range(len(df3)):
#     p11_threshold[df3.iloc[i,0]]=df3.iloc[i,1]
# p11 = p11_threshold.keys() #提取11种有污染限值的物种名称列表

# #建立11种污染物报警的统计表
# df_alarm11 = df.iloc[:,0:2] #复制站点和时间两列
# for name in p11_threshold.keys():
#     df_alarm11 = pd.concat([df_alarm11,df[name]],axis=1)
# df_alarm11.to_excel("output/"+period+"11种污染物原始数据.xlsx")

# #计算数据有效次数
# df_count_eff47=count_eff(df,stations,p47)
# print(df_count_eff47)
# df_count_eff47.to_excel("./output/"+period+"47种污染物的数据有效次数.xlsx")

# #计算数据有效率
# df_eff47 = cal_eff_ratio(df,stations,p47)
# print(df_eff47)
# df_eff47.to_excel("./output/"+period+"47种污染物的数据有效率.xlsx")


# #计算报警次数
# df_alarm11_count = count_alarm(df_alarm11,stations,p11_threshold)
# print(df_alarm11_count)
# df_alarm11_count.to_excel("./output/"+period+"11种污染物报警次数.xlsx")

# #计算总报警率
# df_alarm11_all_ratio = df_alarm11_count / len(df_alarm11.groupby("时间").count())
# print(df_alarm11_all_ratio)
# df_alarm11_all_ratio.to_excel("./output/"+period+"11种污染物总报警率.xlsx")

# #计算有效数据的报警率
# df_alarm11_eff_ratio = df_alarm11_count / count_eff(df_alarm11,stations,p11)
# print(df_alarm11_eff_ratio)
# df_alarm11_eff_ratio.to_excel("./output/"+period+"11种污染物有效数据报警率.xlsx")


#计算12个污染物的三级报警次数和4个污染物的二级报警次数
df_level_3_limit = pd.read_excel("./input/12个报警因子三级限值.xlsx")
df_level_2_limit = pd.read_excel("./input/4个报警因子二级限值.xlsx")
#提取12种三级报警因子
p12_threshold ={}   
for i in range(len(df_level_3_limit)):
    p12_threshold[df_level_3_limit.iloc[i,0]]=df_level_3_limit.iloc[i,1]
p12 = p12_threshold.keys() #提取12种有污染限值的物种名称列表
#建立12种污染物报警的统计表
df_alarm12 = df.iloc[:,0:2] #复制站点和时间两列
for name in p12_threshold.keys():
    df_alarm12 = pd.concat([df_alarm12,df[name]],axis=1)
df_alarm12.to_excel("output/"+period+"12种污染物原始数据.xlsx")
#提取4种二级报警因子
p4_threshold ={}   
for i in range(len(df_level_2_limit)):
    p4_threshold[df_level_2_limit.iloc[i,0]]=df_level_2_limit.iloc[i,1]
p4 = p4_threshold.keys() #提取4种有污染限值的物种名称列表
#建立4种污染物报警的统计表
df_alarm4 = df.iloc[:,0:2] #复制站点和时间两列
for name in p4_threshold.keys():
    df_alarm4 = pd.concat([df_alarm4,df[name]],axis=1)
df_alarm4.to_excel("output/"+period+"4种污染物原始数据.xlsx")
#计算二级报警次数
df_alarm_level_2_count = count_alarm(df_alarm4,stations,p4_threshold)
print(df_alarm_level_2_count)
df_alarm_level_2_count.to_excel("./output/"+period+"4种污染物二级报警次数.xlsx")
#计算三级报警
df_alarm_level_3_count = count_alarm(df_alarm12,stations,p12_threshold) #其中四个污染物是二级+三级次数
print(df_alarm_level_3_count)
for pp in p4_threshold.keys():
    df_alarm_level_3_count[pp] = df_alarm_level_3_count[pp] - df_alarm_level_2_count[pp] #减去二级
print(df_alarm_level_3_count)
df_alarm_level_3_count.to_excel("./output/"+period+"12种污染物三级报警次数.xlsx")


#单独另算VOCs-36的报警



