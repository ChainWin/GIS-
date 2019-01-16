
# coding: utf-8
import numpy as np
from sklearn.externals import joblib
import pymysql

# # 确定当前的检测设备和控制线：
equipment='equipment_switch_36'
SPE_limit=6
if equipment=='equipment_switch_36':
    vector=26
# # 装载已经训练好的模型
pca=joblib.load('gis_diagnosis_pca.pkl')
scaler=joblib.load('gis_diagnosis_scaler.pkl')

#数据矩阵
x=np.empty((1,vector))

#设置数据缺省值字典
#目前只考虑了switch_36
#温度缺省值
tem_def = {'hw3' : 28.5, 'hw2' : 28.5,'hw1' : 27.0,'[TEMP_DIFF:hw1,hw2,hw3]' : 1,'[TEMP_DIFF:3-1,3-2,3-4]' : 1.5,'[TEMP_DIFF:2-1,2-2,2-3]' : 2.0,'3-4' : 30.0,'3-2' : 30.0,'3-1' : 29.3,'2-5' : 33.8,'2-4' : 32.25,'2-3' : 34.12,'2-2' : 35,'2-1' : 35.8}
#压力缺省值
pres_def = {'3-7' : 69, '3-6' : 0.05,'3-4' : 1.13,'3-2' : 0.17,'3-1' : 0.257,'2-7' : 30.0,'2-6' : 0.028,'2-5' : 36.6,'2-4' : 2.0,'2-3' : 3.124,'2-2' : 1.9,'2-1' : 0.0853}    
tem_latest=tem_def
pres_latest=pres_def

# # 连接数据库，取得最新的数据

while(True):

    # 打开数据库连接

    db = pymysql.connect("localhost","root","0502","robot" )

    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    #查找预测值为空且时间最远的一条数据
    sql_get_value="select min(d_create_time),c_observation_value,c_component_code,c_distinguish_type_id from fault_detection_value where c_equipment_id='"+equipment+"' and equipment_fault_prob is null and c_observation_value != 'ON' and c_observation_value != 'OFF' and c_state=1 and c_component_code not like '%REF%'"

    try:
        cursor.execute(sql_get_value)
        latest_data=cursor.fetchall()
    except Exception as e:
    #错误回滚
        db.rollback() 
    finally:
        db.close() 

    if latest_data[0][0]== None: 
        break

    #下面如果取消注释，那么就会变成针对单个compoment的检测，而不是针对设备的检测
    # #温度当前值
    # tem_latest =tem_def
    # #压力当前值
    # pres_latest = pres_def


    if latest_data[0][3]=='meterReading':
        pres_latest[latest_data[0][2]]=latest_data[0][1]
    elif latest_data[0][3]=='infrared':
        tem_latest[latest_data[0][2]]=latest_data[0][1]

    #建立数据
    i=0
    for value in tem_latest.values():
        x[0][i]=value
        i+=1
    for value in pres_latest.values():
        x[0][i]=value
        i+=1


    # # 开始训练

    x_scaled=scaler.transform(x)
    x_rec=pca.inverse_transform(pca.transform(x_scaled))
    res=x_scaled-x_rec
    spe=float(np.diag(np.dot(res,np.transpose(res))))
    # #将spe转化成故障率
    # if spe <= SPE_limit and spe!=0:
    #     spe=spe ** 0.5 *50 / (SPE_limit**0.5)
    # elif spe > SPE_limit: 
    #     spe=(spe-SPE_limit)
    # else: 
    #     spe=99.99
            
    # #做一下故障定位,贡献矩阵res_c
    if spe>SPE_limit:
        sort_index=np.argsort(-x_scaled[0])
        tem_or_pres=[]
        component_code_top5=[]
        for i in range(5):
            if sort_index[i]<len(tem_def):
                component_code_top5.append(list(tem_def.keys())[sort_index[i]])
                tem_or_pres.append('温度')
            else:
                component_code_top5.append(list(pres_def.keys())[sort_index[i]-len(tem_def)])
                tem_or_pres.append('压力')
        data_top5=x[0][sort_index[:5]]
        diff_top5=np.around(abs(x_scaled[0][sort_index[:5]])*100/np.sum(abs(x_scaled)),decimals=4)
        # print(component_code_top5)
        # print(diff_top5)
        db = pymysql.connect("localhost","root","0502","robot" )
        cursor = db.cursor()
        sql_equipment_status="insert into equipment_status (d_create_time,c_equipment_id,设备异常程度,1st异常表计,2nd异常表计,3rd异常表计,4th异常表计,5th异常表计) values ('%s','%s',%s,'表计名:%s 属性:%s 当前示数:%s 故障贡献率:%s%%','表计名:%s 属性:%s 当前示数:%s 故障贡献率:%s%%','表计名:%s 属性:%s 当前示数:%s 故障贡献率:%s%%','表计名:%s 属性:%s 当前示数:%s 故障贡献率:%s%%','表计名:%s 属性:%s 当前示数:%s 故障贡献率:%s%%')" % (str(latest_data[0][0]),equipment,str(spe),component_code_top5[0],tem_or_pres[0],data_top5[0],str(diff_top5[0]),component_code_top5[1],tem_or_pres[1],data_top5[1],str(diff_top5[1]),component_code_top5[2],tem_or_pres[2],data_top5[2],str(diff_top5[2]),component_code_top5[3],tem_or_pres[3],data_top5[3],str(diff_top5[3]),component_code_top5[4],tem_or_pres[4],data_top5[4],str(diff_top5[4]))
        #print(sql_equipment_status)
        try:
            cursor.execute(sql_equipment_status)
            db.commit()
        except Exception as e:
        #错误回滚
            db.rollback() 
        finally:
            db.close()

        # file = r'fault_reason.txt'
        # with open(file, 'a+') as f:
        #     f.write(str(latest_data[0][0])+','+str(float(spe))+','+str(np.sort(-abs(x_scaled)*100/np.sum(abs(x_scaled))))+'%\n')


    db = pymysql.connect("localhost","root","0502","robot" )
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    #查找预测值为空且时间最远的一条数据
    sql_diagnosis="update fault_detection_value set equipment_fault_prob='"+str(float(spe))+"' where d_create_time='"+str(latest_data[0][0])+"' and c_equipment_id='"+equipment+"' and equipment_fault_prob is null and c_observation_value != 'ON' and c_observation_value != 'OFF' and c_state=1 and c_component_code not like '%REF%'"

    try:
        cursor.execute(sql_diagnosis)
        db.commit()
    except Exception as e:
    #错误回滚
        db.rollback() 
    finally:
        db.close() 


