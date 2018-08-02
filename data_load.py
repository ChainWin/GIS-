
# coding: utf-8



print(__doc__)

import random
from time import sleep
from sqlalchemy import Column, String, create_engine,Integer,Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


# # 连接数据库


# 创建对象的基类:
Base = declarative_base()
# 构建数据模型  
class gis_electric(Base):
    __tablename__ = "gis数据"         # 表名
    __table_args__ = {
        "mysql_engine": "InnoDB",   # 表的引擎
        "mysql_charset": "utf8",    # 表的编码格式
    }

    # 表结构,具体更多的数据类型自行百度
    id = Column("id", Integer, primary_key=True, autoincrement=True)
    line_voltage = Column("线电压", Float, default=1.5)
    point_discharge = Column("尖端放电", Integer, default=0)
    void_discharge = Column("内部放电", Integer, default=0)
    suspended_discharge = Column("悬浮放电", Integer, default=0)
    surface_discharge = Column("沿面放电", Integer, default=0)
    pd_capacitance = Column("局部放电量", Float, default=0.5)
    tem1 = Column("绕组热点温度", Float, default=120)
    tem2 = Column("绝缘油温度", Float, default=105)
    tube_resistance = Column("套管绝缘电阻", Float, default=1)
    core_resistance = Column("铁芯接地电阻", Float, default=0.2)
    core_current = Column("铁芯接地电流", Float, default=0.1)
    fault_probability = Column("设备故障概率", Float)
    def __repr__(self):
        return '%r' % (self.id)



engine = create_engine("mysql+pymysql://wxh:wxh@192.168.0.111:3306/substation_anomaly_detection", encoding="utf8", echo=False)
DBSession = sessionmaker(bind=engine)

items=0
while(1):
    # 创建Session:
    items+=1
    session = DBSession()
    if items %5 == 0:
        new_data=gis_electric(line_voltage=1.5+random.uniform(-0.2,0.2),point_discharge=random.choice((0,1)),void_discharge=random.choice((0,1)),suspended_discharge=random.choice((0,1)),surface_discharge=random.choice((0,1)),pd_capacitance=0.5+random.uniform(-0.2,0.2),tem1=120+random.uniform(-10,10),tem2=105+random.uniform(-10,10),tube_resistance=1+random.uniform(-0.5,0.5),core_resistance=0.2+random.uniform(-0.1,0.1),core_current=0.1+random.uniform(-0.05,0.05))
    else:
        new_data=gis_electric(line_voltage=1.5+random.uniform(-0.2,0),point_discharge=0,void_discharge=0,suspended_discharge=0,surface_discharge=0,pd_capacitance=0.5+random.uniform(-0.2,0),tem1=120+random.uniform(-10,0),tem2=105+random.uniform(-10,0),tube_resistance=1+random.uniform(0,0.5),core_resistance=0.2+random.uniform(0,0.1),core_current=0.1+random.uniform(-0.05,0))
    # 添加到session:
    session.add(new_data)
    # 提交即保存到数据库:
    session.commit()
    # 关闭Session:
    session.close()
    sleep(15)



