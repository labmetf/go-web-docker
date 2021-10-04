import requests
import json
import timeit
import mysql.connector
from influxdb import InfluxDBClient
import random
import redis
from datetime import datetime

dat = []
data_influx = []
cluster = 1
cabinet = 4
dat2 = []


for x in range (1,4) :
    for y in range (1, 16) :
        data = {"cluster":cluster,
                "cabinet":cabinet,
                "module":x,
                "cell":y,
                "voltage":float(round(random.uniform(3.1, 3.3),2)),
                "temperature":float(round(random.uniform(24.5, 30.5),2)),
                "current":float(round(random.uniform(100, 110),2)),
                "soc":float(round(random.uniform(80, 100),2)),
                "soh":float(round(random.uniform(70, 90),2))}
        dat.append(data)

#---------------------------------Insert to influxdb local---------------------------------#
dbClient = InfluxDBClient('localhost', 8086, 'root', 'root', 'Battery')

for n in range (0,45):
    data2 = {"measurement":"databattery",
                            "tags": {
                                "cluster_id": int(cluster),
                                "cabinet_id": int(cabinet),
                                "module_id": int(dat[n]["module"]),
                                "cell_id": int(dat[n]["cell"]),
                            },
                            "fields":
                            {
                            "cluster_id": int(cluster),
                            "cabinet_id": int(cabinet),
                            "module_id": int(dat[n]["module"]),
                            "cell_id": int(dat[n]["cell"]),
                            "voltage":dat[n]["voltage"],
                            "temperature":dat[n]["temperature"],
                            "current":dat[n]["current"],
                            "SoC":dat[n]["soc"],
                            "SoH":dat[n]["soh"]
                            }       
                            }
    data_influx.append(data2)
    
dbClient.write_points(data_influx)


#---------------------------------Insert to influxdb pusat---------------------------------#
start_inf = timeit.default_timer()
dbClient = InfluxDBClient('192.168.10.15', 8086, 'bess', 'besspass', 'Battery')

for n in range (0,45):
    data2 = {"measurement":"databattery",
                            "tags": {
                                "cluster_id": int(cluster),
                                "cabinet_id": int(cabinet),
                                "module_id": int(dat[n]["module"]),
                                "cell_id": int(dat[n]["cell"]),
                            },
                            "fields":
                            {
                            "cluster_id": int(cluster),
                            "cabinet_id": int(cabinet),
                            "module_id": int(dat[n]["module"]),
                            "cell_id": int(dat[n]["cell"]),
                            "voltage":dat[n]["voltage"],
                            "temperature":dat[n]["temperature"],
                            "current":dat[n]["current"],
                            "SoC":dat[n]["soc"],
                            "SoH":dat[n]["soh"]
                            }       
                            }
    data_influx.append(data2)

dbClient.write_points(data_influx)
end_inf= timeit.default_timer() - start_inf
#---------------------------------Insert to mysql local---------------------------------#
mydb = mysql.connector.connect(host = 'localhost',user="admin",password='root',database="BESS")
mycursor = mydb.cursor()

for i in range(0,45):

  # sev voltage
  if dat[i]["voltage"] <= 2.1 :
    sev = 10
  elif dat[i]["voltage"] > 2.1 and dat[i]["voltage"]<= 2.2 :
    sev = 9
  elif dat[i]["voltage"] > 2.2 and dat[i]["voltage"]<= 2.3 :
    sev = 8
  elif dat[i]["voltage"] > 2.3 and dat[i]["voltage"]<= 2.4 :
    sev = 6
  elif dat[i]["voltage"] > 2.4 and dat[i]["voltage"]<= 2.6 :
    sev = 5
  elif dat[i]["voltage"] > 2.6 and dat[i]["voltage"]<= 2.8 :
    sev = 1
  elif dat[i]["voltage"] > 2.8 and dat[i]["voltage"]<= 3.0 :
    sev = 2
  elif dat[i]["voltage"] > 3.0 and dat[i]["voltage"]<= 3.3 :
    sev = 3
  elif dat[i]["voltage"] > 3.3 and dat[i]["voltage"]<= 3.4 :
    sev = 7
  elif dat[i]["voltage"] > 3.4 and dat[i]["voltage"]<= 3.5 :
    sev = 8
  elif dat[i]["voltage"] > 3.5 and dat[i]["voltage"]<= 3.65 :
    sev = 9
  elif dat[i]["voltage"] >= 3.66 :
    sev = 10

  # sev temperature
  if dat[i]["temperature"] <= 15 :
    sev1 = 8
  elif dat[i]["temperature"] > 15 and dat[i]["voltage"]<= 17 :
    sev1 = 8
  elif dat[i]["temperature"] > 17 and dat[i]["voltage"]<= 20 :
    sev1 = 7
  elif dat[i]["temperature"] > 20 and dat[i]["voltage"]<= 22 :
    sev1 = 1
  elif dat[i]["temperature"] > 22 and dat[i]["voltage"]<= 23 :
    sev1 = 2
  elif dat[i]["temperature"] > 23 and dat[i]["voltage"]<= 24 :
    sev1 = 3
  elif dat[i]["temperature"] > 24 and dat[i]["voltage"]<= 25 :
    sev1 = 4
  elif dat[i]["temperature"] > 25 and dat[i]["voltage"]<= 30 :
    sev1 = 5
  elif dat[i]["temperature"] > 30 and dat[i]["voltage"]<= 34 :
    sev1 = 6
  elif dat[i]["temperature"] > 34 and dat[i]["voltage"]<= 35 :
    sev1 = 9
  elif dat[i]["temperature"] > 36 :
    sev1 = 10
  

  # occ voltage
  if dat[i]["voltage"] <= 2.1 :
    occ = 10
  elif dat[i]["voltage"] > 2.1 and dat[i]["voltage"]<= 2.3 :
    occ = 6
  elif dat[i]["voltage"] > 2.3 and dat[i]["voltage"]<= 2.5 :
    occ = 8
  elif dat[i]["voltage"] > 2.5 and dat[i]["voltage"]<= 2.6 :
    occ = 5
  elif dat[i]["voltage"] > 2.6 and dat[i]["voltage"]<= 2.8 :
    occ = 1
  elif dat[i]["voltage"] > 2.8 and dat[i]["voltage"]<= 3.0 :
    occ = 2
  elif dat[i]["voltage"] > 3.0 and dat[i]["voltage"]<= 3.2 :
    occ = 3
  elif dat[i]["voltage"] > 3.2 and dat[i]["voltage"]<= 3.3 :
    occ = 4
  elif dat[i]["voltage"] > 3.3 and dat[i]["voltage"]<= 3.4 :
    occ = 6
  elif dat[i]["voltage"] > 3.4 and dat[i]["voltage"]<= 3.5 :
    occ = 7
  elif dat[i]["voltage"] > 3.5 and dat[i]["voltage"]<= 3.65 :
    occ = 8
  elif dat[i]["voltage"] >= 3.66 :
    occ = 10


  # occ temperature
  if dat[i]["temperature"] <= 15 :
    occ1 = 5
  elif dat[i]["temperature"] > 15 and dat[i]["voltage"]<= 16 :
    occ1 = 5
  elif dat[i]["temperature"] > 16 and dat[i]["voltage"]<= 18 :
    occ1 = 6
  elif dat[i]["temperature"] > 18 and dat[i]["voltage"]<= 20 :
    occ1 = 7
  elif dat[i]["temperature"] > 20 and dat[i]["voltage"]<= 21 :
    occ1 = 1
  elif dat[i]["temperature"] > 21 and dat[i]["voltage"]<= 22 :
    occ1 = 2
  elif dat[i]["temperature"] > 22 and dat[i]["voltage"]<= 24 :
    occ1 = 3
  elif dat[i]["temperature"] > 24 and dat[i]["voltage"]<= 25 :
    occ1 = 4
  elif dat[i]["temperature"] > 25 and dat[i]["voltage"]<= 29 :
    occ1 = 8
  elif dat[i]["temperature"] > 29 and dat[i]["voltage"]<= 34 :
    occ1 = 9
  elif dat[i]["temperature"] > 35 :
    occ1 = 10

   # det voltage
  if dat[i]["voltage"] <= 3.1 :
    det = 5
  elif dat[i]["voltage"] > 3.1 and dat[i]["voltage"]<= 3.25 :
    det = 5
  elif dat[i]["voltage"] > 3.25 and dat[i]["voltage"]<= 3.35 :
    det = 5
  elif dat[i]["voltage"] > 3.35 and dat[i]["voltage"]<= 3.45 :
    det = 5
  elif dat[i]["voltage"] > 3.45 and dat[i]["voltage"]<= 3.5 :
    det = 5
  elif dat[i]["voltage"] > 3.5 :
    det = 5

  # det temperature
  if dat[i]["temperature"] <= 15 :
    det1 = 5
  elif dat[i]["temperature"] > 15 and dat[i]["temperature"]<= 34 :
    det1 = 5
  elif dat[i]["temperature"] > 35 :
    det1 = 5
    
    
  rpn_voltage = (sev*occ*det)
  rpn_temperature = (sev1*occ1*det1)
  start_sql = timeit.default_timer()
  sql = "INSERT INTO Battery (cluster_id,cabinet_id,module_id,cell_id,voltage,temperature,current_bit,current,SoC,SoH,rpn_voltage,rpn_temperature) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
  val = (cluster,cabinet,dat[i]["module"],dat[i]["cell"],dat[i]["voltage"],dat[i]["temperature"], 0, dat[i]["current"], dat[i]["soc"], dat[i]["soh"],rpn_voltage,rpn_temperature)
  mycursor.execute(sql, val)
  mydb.commit()
  end_sql= timeit.default_timer() - start_sql


#---------------------------------Insert to mysql pusat---------------------------------#


mydb = mysql.connector.connect(host = '192.168.10.15',user="bess",password='besspass',database="BESS")
mycursor = mydb.cursor()

for i in range(0,45):

  sql = "INSERT INTO Battery (cluster_id,cabinet_id,module_id,cell_id,voltage,temperature,current_bit,current,SoC,SoH) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
  val = (cluster,cabinet,dat[i]["module"],dat[i]["cell"],dat[i]["voltage"],dat[i]["temperature"], 0, dat[i]["current"], dat[i]["soc"], dat[i]["soh"])
  mycursor.execute(sql, val)
  mydb.commit()




#---------------------------------Insert to redis---------------------------------#
start_red = timeit.default_timer()
r = redis.Redis(host='localhost', port=6379)
now = datetime.now()
date_time = now.strftime("%m-%d-%Y %H:%M:%S")

for x in range (1,4) :
    for y in range (1, 16) :
        data = {"timestamp": date_time, 
                "cluster_id":cluster,
                "cabinet_id":cabinet,
                "module_id":x,
                "cell_id":y,
                "voltage":float(round(random.uniform(3.1, 3.3),2)),
                "temperature":float(round(random.uniform(24.5, 30.5),2)),
                "current":float(round(random.uniform(100, 110),2)),
                "SoC":float(round(random.uniform(80, 100),2)),
                "SoH":float(round(random.uniform(70, 90),2))}
        dat2.append(data)
        
json_insert = json.dumps(dat2)
r.set('value', json_insert)
end_red= timeit.default_timer() - start_red
#---------------------------------Insert Time---------------------------------#
mydb = mysql.connector.connect(host = 'localhost',user="admin",password='root',database="pengukuran")
mycursor = mydb.cursor()
sql = "INSERT INTO waktu (mysql,influx,redis) VALUES (%s,%s,%s)"
val = (end_sql,end_inf,end_red)
mycursor.execute(sql, val)
mydb.commit()

print(end_sql)
print(end_inf)
print(end_red)
