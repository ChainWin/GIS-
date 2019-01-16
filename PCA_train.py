
# coding: utf-8

# In[1]:


import numpy as np
from sklearn.decomposition import PCA
from sklearn.externals import joblib
from scipy.stats.distributions import chi2
from sklearn.preprocessing import StandardScaler
#from sklearn.ensemble import GradientBoostingClassifier
import pymysql


# 项目分为有监督和无监督方式，有监督使用GBDT算法，无监督时，暂时还没想好

# # 连接数据库并构建正常训练集

# In[2]:


#只考虑温度、压力等有意义的方便检测的component，开关状态component不考虑
tem_component_code=['hw3', 'hw2', 'hw1','[TEMP_DIFF:hw1,hw2,hw3]','[TEMP_DIFF:3-1,3-2,3-4]','[TEMP_DIFF:2-1,2-2,2-3]','3-4','3-2','3-1','2-5','2-4','2-3','2-2','2-1']
pres_component_code=['3-7','3-6','3-4','3-2','3-1','2-7','2-6','2-5','2-4','2-3','2-2','2-1']
tem_component_results=[]
pres_component_results=[]


# In[3]:


# 打开数据库连接
db = pymysql.connect("localhost","root","0502","robot" )
 
# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor()
sql_get_tem_value="select c_observation_value from fault_detection_value where c_equipment_id= %s and c_component_code = %s and c_distinguish_type_id='infrared' and c_state=1 and fault='normal'"
sql_get_pres_value="select c_observation_value from fault_detection_value where c_equipment_id= %s and c_component_code = %s and c_distinguish_type_id='meterReading' and c_state=1"
try:
    for component_code in tem_component_code:
        cursor.execute(sql_get_tem_value,('equipment_switch_36',component_code))
        tem_component_results.append(cursor.fetchall())
    for component_code in pres_component_code:
        cursor.execute(sql_get_pres_value,('equipment_switch_36',component_code))
        pres_component_results.append(cursor.fetchall())
except Exception as e:
 #错误回滚
    db.rollback() 
finally:
    db.close() 


# In[4]:


#建立数据训练集，该数据集中的数据都是跟据规则判定的正常数据
dataset=np.empty([10000,26]) 

for i in range(dataset.shape[0]):
    for j in range(len(tem_component_results)):
        dataset[i][j]=tem_component_results[j][np.random.randint(len(tem_component_results[j]))][0]
    for k in range(len(pres_component_results)):
        dataset[i][k+len(tem_component_results)]=pres_component_results[k][np.random.randint(len(pres_component_results[k]))][0]


# # 数据标准化

# In[5]:


scaler = StandardScaler()
x=scaler.fit_transform(dataset)


# In[32]:


pca = PCA(n_components=0.95)
#pca = PCA(n_components='mle',svd_solver='full')
x_rec=pca.inverse_transform(pca.fit_transform(x))
res=x-x_rec


# In[35]:


spe=np.diag(np.dot(res,np.transpose(res)))


# In[36]:


#使用卡方分布的方法计算统计控制线
spe_mean=np.mean(spe)
spe_var=np.var(spe)
SPE_limit = spe_var/2/spe_mean * chi2.ppf(0.99,2*spe_mean**2/spe_var)


# In[37]:


SPE_limit


# In[38]:


joblib.dump(scaler, 'gis_diagnosis_scaler.pkl') 
joblib.dump(pca, 'gis_diagnosis_pca.pkl') 


# In[ ]:


# # 创建对象的基类:
# Base = declarative_base()
# # 构建数据模型  
# class gis_electric(Base):
#     __tablename__ = "gis电气信息"         # 表名
#     __table_args__ = {
#         "mysql_engine": "InnoDB",   # 表的引擎
#         "mysql_charset": "utf8",    # 表的编码格式
#     }

#     # 表结构,具体更多的数据类型自行百度
#     id = Column("id", Integer, primary_key=True, autoincrement=True)
#     line_voltage = Column("线电压", Float)
#     point_discharge = Column("尖端放电", Integer, default=0)
#     void_discharge = Column("内部放电", Integer, default=0)
#     suspended_discharge = Column("悬浮放电", Integer, default=0)
#     surface_discharge = Column("沿面放电", Integer, default=0)
#     pd_capacitance = Column("局部放电量", Float)
#     Dielectric_loss_tan = Column("套管介质损耗角tan", Float)
#     tube_capacitance = Column("套管电容量", Float)
#     tube_resistance = Column("套管绝缘电阻", Float)
#     core_resistance = Column("铁心接地电阻", Float)
#     core_current = Column("铁心接地电流", Float)
#     fault_probability = Column("设备故障概率", Float)
#     def __repr__(self):
#         return '%r' % (self.id)


# In[ ]:


# engine = create_engine("mysql+pymysql://wxh:wxh@192.168.0.109:3306/substation_anomaly_detection", encoding="utf8", echo=False)
# DBSession = sessionmaker(bind=engine)


# In[ ]:


# # 创建Session:
# session = DBSession()
# dataset = session.query(gis_electric).filter_by(fault_probability=None).all()
# datanum=session.query(gis_electric).filter_by(fault_probability=None).count()
# x=np.zeros(shape=(datanum,11)) 
# i=0
# for data in dataset:
#     x[i,0]=data.line_voltage
#     x[i,1]=data.point_discharge
#     x[i,2]=data.void_discharge
#     x[i,3]=data.suspended_discharge
#     x[i,4]=data.surface_discharge
#     x[i,5]=data.pd_capacitance
#     x[i,6]=data.Dielectric_loss_tan
#     x[i,7]=data.tube_capacitance
#     x[i,8]=data.tube_resistance
#     x[i,9]=data.core_resistance
#     x[i,10]=data.core_current
#     i+=1


# # 装载模型并输出预测

# In[ ]:


# svc=joblib.load('gis_anomaly_detection.pkl')
# dec = svc.predict_proba(x)
# i=0
# for data in dataset:
#     data.fault_probability=float(dec[i][1])
#     i+=1


# In[ ]:


# # 提交即保存到数据库:
# session.commit()


# In[ ]:


# # 关闭Session:
# session.close()

