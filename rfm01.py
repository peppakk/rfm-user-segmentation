#!/usr/bin/env python
# coding: utf-8

# In[23]:


import time  # 时间库
import numpy as np
import pandas as pd
import pymysql

from sqlalchemy import create_engine  # 关键：导入create_engine函数
from pyecharts.charts import Bar3D
from pyecharts import options as opts

import os

# rfm：r:最近购买时间间隔，f：购买频次 m:购买金额

# 重置所有Pandas显示选项为默认
pd.reset_option('all')


# In[2]:


# 加载数据
# 定义列表，记录excel表名
sheet_names = ['2015', '2016', '2017', '2018', '会员等级']
sheet_dict = pd.read_excel(
    'E:/数据分析/rfm案例/sales.xlsx',
    sheet_name=sheet_names[:-1])

# 2. 循环处理每个sheet：删除Unnamed列 + 更新sheet_dict
for sheet_name, df in sheet_dict.items():
    # 2.1 删除所有Unnamed列（列名以Unnamed开头）
    df_clean = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    # 2.2 关键：将处理后的数据赋值回sheet_dict
    sheet_dict[sheet_name] = df_clean

# 查看处理结果
sheet_dict


# In[3]:


# 查看2016数据 41278,41277 说明有缺失值，可以选择删除

# 查看字典中每个df对象的基本信息 和 统计信息
for item in sheet_names[:-1]:
    print(sheet_dict[item].info())
    print(sheet_dict[item].describe())


# In[4]:


# 数据预处理
# 需要处理的内容，1、删除异常值 2、过滤出金额>1的订单 3、r：最近购买间隔时间，电器一般按年算。固定时间节点，以每年的最后一天最为当年的 分析节点

# 遍历表名，除了最后一项“会员等级”
for item in sheet_names[:-1]:
    # 删除缺失值
    sheet_dict[item]=sheet_dict[item].dropna()
    # 过滤金额>1的订单
    sheet_dict[item]=sheet_dict[item][sheet_dict[item]['订单金额']>1]
    # 固定时间节点，以每年的最后一天作为当年的 分析节点
    sheet_dict[item]['max_year_date']=sheet_dict[item]['提交日期'].max()


# In[5]:


# 查看处理后的数据
# 查看字典中每个df对象的基本信息 和 统计信息
for item in sheet_names[:-1]:
    print(item)
    print(sheet_dict[item].info())
    print(sheet_dict[item].describe())


# In[6]:


# 把上述 四张表（对应的4格df对象），合并成一个df对象，  垂直合并,忽略索引
df_merge=pd.concat(list(sheet_dict.values()),ignore_index=True)
df_merge


# In[7]:


# 为了好区分，给df对象新增 year 列，从提交日期中提取出年份
df_merge['year']=df_merge['提交日期'].dt.year

# 给表新增一列，date_interval 表示本订单 距 统计节点时间的 差值  需要间隔时间的最小值
df_merge['date_interval']=df_merge['max_year_date']-df_merge['提交日期']
df_merge


# In[8]:


# 把date_interval 转化成int 类型
df_merge['date_interval']=df_merge['date_interval'].dt.days
df_merge


# In[9]:


# 数据统计分析
# 基于year和会员ID分组，统计 rfm 三项的基本数据
rfm_gb=df_merge.groupby(['year','会员ID'],as_index=False).agg({
    'date_interval':'min',
    '订单号':'count',
    '订单金额':'sum'
})
rfm_gb


# In[10]:


# 修改列名
rfm_gb.columns=['year','会员ID','r','f','m']
rfm_gb


# In[11]:


# 分别查看r f m 这三列值的分布情况
rfm_gb.iloc[:,2:].describe().T


# In[12]:


# 划分区间，分别给出：rfm的评分，依据：r:最近一次购买时间 越小分越高，f：购买次数 越大分越高，m:购买金额 越大分越高
# 思路1：我们给定区间数，由系统自动划分区间范围
pd.cut(rfm_gb['r'],bins=3).unique()

# 思路2，手动指定区间范围，由系统自动划分区间数,包右不包左
r_bins=[-1,79,255,365]
f_bins=[0,2,5,130]
m_bins=[1,69,1199,206252]
pd.cut(rfm_gb['r'],bins=r_bins).unique()


# In[13]:


# 思路3 ：基于我们手动指定区间范围，给出每个范围的 评分（三分法，低中高）
rfm_gb['r_label']=pd.cut(rfm_gb['r'],bins=r_bins,labels=[3,2,1])
rfm_gb['f_label']=pd.cut(rfm_gb['f'],bins=f_bins,labels=[1,2,3])
rfm_gb['m_label']=pd.cut(rfm_gb['m'],bins=m_bins,labels=[1,2,3])
rfm_gb


# In[14]:


# 实际开发写法，完整版
# 思路4 ：基于我们手动指定区间范围，给出每个范围的 评分（三分法，低中高）
rfm_gb['r_label']=pd.cut(rfm_gb['r'],bins=r_bins,labels=[i for i in range(len(r_bins)-1,0,-1)])
rfm_gb['f_label']=pd.cut(rfm_gb['f'],bins=f_bins,labels=[i for i in range(1,len(f_bins))])
rfm_gb['m_label']=pd.cut(rfm_gb['m'],bins=m_bins,labels=[i for i in range(1,len(m_bins))])
rfm_gb


# In[15]:


# 统计每个会员的rfm的评分  拼接
# 转换类型，分类类型转化为字符串类型
rfm_gb['r_label']=rfm_gb['r_label'].astype(str)
rfm_gb['f_label']=rfm_gb['f_label'].astype(str)
rfm_gb['m_label']=rfm_gb['m_label'].astype(str)
# 拼接
rfm_gb['rfm_group']=rfm_gb['r_label']+rfm_gb['f_label']+rfm_gb['m_label']
rfm_gb


# In[16]:


# 导出结果
rfm_gb.to_excel('E:/数据分析/rfm案例/sale_rfm_group.xlsx',index=False)


# In[17]:


# 导出结果到MySQL中
# 创建引擎对象
temp_engine = create_engine('mysql+pymysql://root:root1234@localhost:3306/mysql?charset=utf8')
with temp_engine.connect() as conn:
    conn.execute("CREATE DATABASE IF NOT EXISTS rfm_gb CHARACTER SET utf8;")
    
engine=create_engine('mysql+pymysql://root:root1234@localhost:3306/rfm_gb?charset=utf8')
# 导出数据到mysql中
# 参1 存储结果的数据表名 2 引擎对象 3 忽略索引 4 如果表存在，则替换数据
rfm_gb.to_sql('sales_rfm_score',engine,index=False,if_exists='replace')
# 查看数据
pd.read_sql('select * from sales_rfm_score',engine)


# In[18]:


# 数据可视化
# 准备可视化的数据，即：rfm_group(分组评分)，year（统计年份），number（评分个数）
display_data=rfm_gb.groupby(['rfm_group','year'],as_index=False).agg({'会员ID':'count'})
display_data


# In[20]:


display_data.columns=['rfm_group','year','number']
# 把number类型转化为int类型
display_data['number']=display_data['number'].astype(int)
display_data


# In[25]:


# 绘制图形
# 颜色池
range_color=['#313695','#4575b4','#74abd9e9','#e0f3f8','#ffffbf','#fee090','#fdae61','#f46d43','#d73027','#a50026']
range_max=int(display_data['number'].max())
c=(
    Bar3D()# 设置一个3D的柱形图对象
    .add(
        "",# 图例
        [d.tolist() for d in display_data.values],# 数据
        xaxis3d_opts=opts.Axis3DOpts(type_="category",name="分组名称"),# x轴数据类型，名称，rfm_group
        yaxis3d_opts=opts.Axis3DOpts(type_="category",name="年份"),# y轴数据类型，名称，year
        zaxis3d_opts=opts.Axis3DOpts(type_="value",name="会员数量"),# z轴数据类型，名称，number
    )
    .set_global_opts( # 全局设置
        visualmap_opts=opts.VisualMapOpts(max_=range_max,range_color=range_color),# 设置颜色，以及不同的取值
        title_opts=opts.TitleOpts(title="RFM分组结果"),# 设置标题
    )
)
c.render() # 保存到本地网页中
c.render_notebook()


# In[ ]:




