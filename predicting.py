
# coding: utf-8

# In[1]:


print(__doc__)
import numpy as np


# In[2]:


#from sklearn.preprocessing import StandardScaler
#from sklearn.model_selection import train_test_split
#from sklearn.model_selection import cross_val_score
#from sklearn.metrics import r2_score
from sklearn.externals import joblib
#from sklearn import svm
# we're setting some options for nicer printing here
#np.set_printoptions(suppress=True, precision=4)


# In[3]:


from sqlalchemy import Column, String, create_engine,Integer,Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


# # 连接数据库

# In[4]:


# 创建对象的基类:
Base = declarative_base()
# 构建数据模型  
class gis_electric(Base):
    __tablename__ = "gis电气信息"         # 表名
    __table_args__ = {
        "mysql_engine": "InnoDB",   # 表的引擎
        "mysql_charset": "utf8",    # 表的编码格式
    }

    # 表结构,具体更多的数据类型自行百度
    id = Column("id", Integer, primary_key=True, autoincrement=True)
    line_voltage = Column("线电压", Float)
    point_discharge = Column("尖端放电", Integer, default=0)
    void_discharge = Column("内部放电", Integer, default=0)
    suspended_discharge = Column("悬浮放电", Integer, default=0)
    surface_discharge = Column("沿面放电", Integer, default=0)
    pd_capacitance = Column("局部放电量", Float)
    Dielectric_loss_tan = Column("套管介质损耗角tan", Float)
    tube_capacitance = Column("套管电容量", Float)
    tube_resistance = Column("套管绝缘电阻", Float)
    core_resistance = Column("铁心接地电阻", Float)
    core_current = Column("铁心接地电流", Float)
    fault_probability = Column("设备故障概率", Float)
    def __repr__(self):
        return '%r' % (self.id)


# In[5]:


engine = create_engine("mysql+pymysql://wxh:wxh@192.168.0.109:3306/substation_anomaly_detection", encoding="utf8", echo=False)
DBSession = sessionmaker(bind=engine)


# In[6]:


# 创建Session:
session = DBSession()
dataset = session.query(gis_electric).filter_by(fault_probability=None).all()
datanum=session.query(gis_electric).filter_by(fault_probability=None).count()
x=np.zeros(shape=(datanum,11)) 
i=0
for data in dataset:
    x[i,0]=data.line_voltage
    x[i,1]=data.point_discharge
    x[i,2]=data.void_discharge
    x[i,3]=data.suspended_discharge
    x[i,4]=data.surface_discharge
    x[i,5]=data.pd_capacitance
    x[i,6]=data.Dielectric_loss_tan
    x[i,7]=data.tube_capacitance
    x[i,8]=data.tube_resistance
    x[i,9]=data.core_resistance
    x[i,10]=data.core_current
    i+=1


# # 装载模型并输出预测

# In[9]:


svc=joblib.load('gis_anomaly_detection.pkl')
dec = svc.predict_proba(x)
i=0
for data in dataset:
    data.fault_probability=float(dec[i][1])
    i+=1


# In[10]:


# 提交即保存到数据库:
session.commit()


# In[11]:


# 关闭Session:
session.close()

