
# coding: utf-8

# In[3]:


from time import sleep
import pymysql


# In[22]:
#这个程序需要改，因为现在是六个robot信息数据表和故障诊断数据表在一个robot数据库中，我们在实际应用中，它们不是在一个数据库中，所以对db的游标操作需要分成多个阶段

# 打开数据库连接
db = pymysql.connect("localhost","root","0502","robot" )
 
# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor()
#取得故障诊断表中的日期最大值
sql_maxdatetime="select d_create_time from fault_detection_value where d_create_time in (select MAX(d_create_time) from fault_detection_value)"

#将日期大于日期最大值的所有数据拷贝到故障诊断表中
sql_copy="INSERT INTO fault_detection_value(c_id,d_create_time,c_component_id,c_observation_value,c_distinguish_type_id,c_state,c_json_value,c_fault_level) SELECT c_id,d_create_time,c_component_id,c_observation_value,c_distinguish_type_id,c_state,c_json_value,c_fault_level FROM rb_component_value WHERE d_create_time > %s" 

#sql_inquire="SELECT c_component_id FROM fault_detection_value WHERE c_equipment_id is null"

#将rb_component表合并到fault表中
sql_fill="UPDATE fault_detection_value f,rb_component c SET f.c_equipment_id=c.c_equipment_id,f.c_device_type=c.c_device_type,f.c_device_area=c.c_device_area,f.c_component_code=c.c_code,f.c_component_name=c.c_name,f.c_component_meter_type=c.c_meter_type WHERE f.c_component_id=c.c_id and f.c_equipment_id is null;"

try:
    cursor.execute(sql_maxdatetime)
    max_datetime = cursor.fetchone()
    if max_datetime is None:
        max_datetime='0'
    cursor.execute(sql_copy,(str(max_datetime[0])))
    cursor.execute(sql_fill)
    #提交
    db.commit()
except Exception as e:
 #错误回滚
    db.rollback() 
finally:
    db.close() 

