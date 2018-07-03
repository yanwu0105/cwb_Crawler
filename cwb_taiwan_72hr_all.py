# coding: utf-8
# import package
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import datetime
import os
import pyodbc

# set dir 
today = str(datetime.date.today())
cwb_data = "cwb_weather_data"
if not os.path.exists(cwb_data):
    os.mkdir(cwb_data)

# connect api
import urllib.request
import zipfile 
res ="http://opendata.cwb.gov.tw/opendataapi?dataid=F-D0047-093&authorizationkey=CWB-3FB0188A-5506-41BE-B42A-3785B42C3823"
urllib.request.urlretrieve(res,"F-D0047-093.zip")
f=zipfile.ZipFile('F-D0047-093.zip')

file = ['63_72hr_CH.xml','64_72hr_CH.xml','65_72hr_CH.xml','66_72hr_CH.xml','67_72hr_CH.xml','68_72hr_CH.xml',
        '09007_72hr_CH.xml','09020_72hr_CH.xml','10002_72hr_CH.xml','10004_72hr_CH.xml','10005_72hr_CH.xml',
        '10007_72hr_CH.xml','10008_72hr_CH.xml','10009_72hr_CH.xml','10010_72hr_CH.xml','10013_72hr_CH.xml',
        '10014_72hr_CH.xml','10015_72hr_CH.xml','10016_72hr_CH.xml','10017_72hr_CH.xml','10018_72hr_CH.xml',
        '10020_72hr_CH.xml']
CITY = []
DISTRICT = []
GEOCODE = []
DAY = []
TIME = []
T = []
TD = []
WD = []
WS = []
BF = []
AT = []
Wx = []
Wx_n = []
PoP6h = []
PoP12h = []
get_day = []
RH = []
for filename in file:
    try:
        data = f.read(filename).decode('utf8')
        soup = BeautifulSoup(data,"xml")

        city = soup.locationsName.text

        a = soup.find_all("location")
        for i in range(0,len(a)):
            loc = a[i]
            district = loc.find_all("locationName")[0].text
            geocode = loc.geocode.text
            weather = loc.find_all("weatherElement")
    
            # time 
            time = weather[1].find_all("dataTime")
            for j in range(0,len(time)):
                x = time[j].text.split("T")
                DAY.append(x[0])
                time_1 = x[1].split("+")
                TIME.append(time_1[0])
                CITY.append(city)
                DISTRICT.append(district)
                GEOCODE.append(geocode)
                get_day.append(today)

            for t  in weather[0].find_all("value"):
                T.append(t.text)
            for td  in weather[1].find_all("value"):
                TD.append(td.text)
            for rh  in weather[2].find_all("value"):
                RH.append(rh.text)
            for wd  in weather[5].find_all("value"):
                WD.append(wd.text)  
            ws = weather[6].find_all("value")
            for k  in range(0,len(ws),2):
                WS.append(ws[k].text)
                BF.append(ws[k+1].text)
            for at  in weather[8].find_all("value"):
                AT.append(at.text)
            wx = weather[9].find_all("value")
            for w in range(0,len(wx),2):
                Wx.append(wx[w].text)
                Wx_n.append(wx[w+1].text)
    
            rain1 = weather[3].find_all("value")
            for l in range(0,len(rain1)):
                pop6 = rain1[l].text
                PoP6h.append(pop6)
                PoP6h.append(pop6)
            #PoP6h.append("x") #1200時去掉
            #PoP6h.append("x") #1200時去掉

            rain2 = weather[4].find_all("value")
            for m in range(0,len(rain2)):
                pop12 = rain2[m].text
                PoP12h.append(pop12)
                PoP12h.append(pop12)
                PoP12h.append(pop12)
                PoP12h.append(pop12)
    

            
        #save_name = filename.split(".")
        #save_name = save_name[0].split("_")
        #save_name = save_name[0] + "-" + save_name[1] + "-" + today + ".csv"
        #file_path = os.getcwd()
        #save_name = file_path + "/" + today + "/" + save_name

            
    except:
        break
f.close()

data = {"CITY":CITY,"DISTRICT":DISTRICT,"GEOCODE":GEOCODE,"DAY" : DAY,"TIME" : TIME,"T":T,"TD" : TD,"RH":RH,
        "WD" : WD,"WS" : WS,"BF":BF,"AT" : AT,"Wx": Wx,"Wx_n":Wx_n,"PoP6h" : PoP6h,"PoP12h" :PoP12h,"get_day":get_day}
df = pd.DataFrame(data,columns=["CITY","DISTRICT","GEOCODE","DAY","TIME","T","TD","RH","WD","WS","BF","AT","Wx","Wx_n","PoP6h","PoP12h","get_day"])

file_path = os.getcwd()
save_name = "taiwan_cwb" + today + ".csv"
save_name = file_path + "/" + cwb_data + "/" + save_name



day = np.array(df.DAY)
time = np.array(df.TIME)
DAYTIME = day + " "+ time
DAYTIME = pd.DataFrame(DAYTIME,columns=["DAYTIME"])
df =pd.concat([df,DAYTIME],1)

d_n = []
for a in day:
    a = a.split("-")[2]
    a = int(a)
    d_n.append(a)

d_n = pd.DataFrame(d_n,columns=["d_n"])
df =pd.concat([df,d_n],1)

file_day = today.split("-")
file_day = file_day[2]
file_day = int(file_day)
file_day = file_day + 1


df = df[df.d_n == file_day]
#df = df[df.d_n == 1]

df = df[df.DISTRICT != "總統府"]

df = df[["CITY","DISTRICT","GEOCODE","DAYTIME","T","TD","RH","WD","WS","BF","AT","Wx","Wx_n","PoP6h","PoP12h","get_day"]]

df.to_csv(save_name,index=False,encoding="utf_8_sig")

listdata = df.values.tolist()


server = '139.223.150.19'
username = 'iotreader'
password = 'onlyreader'
database = 'TMIOT'
driver = '{ODBC Driver 13 for SQL Server}'
connectionString = 'DRIVER={0};PORT=1433;SERVER={1};D988ATABASE={2};UID={3};PWD={4}'.format(driver, server, database, username, password)
cnxn = pyodbc.connect(connectionString)
cursor = cnxn.cursor()

for d in listdata:
    insertSql = "insert into [TMIOT].[dbo].[WEATHER_CRAWLER]([CITY],[DISTRICT],[GEOCODE],[DAYTIME],[T],[TD],[RH],[WD],[WS],[BF],[AT],[Wx],[Wx_n],[PoP6h],[PoP12h],[get_day])values (\'{0}\',\'{1}\',{2},convert(datetime,\'{3}\',120),{4},{5},{6},\'{7}\',\'{8}\',\'{9}\',{10},\'{11}\',{12},{13},{14},convert(date,\'{15}\'))".format(d[0],d[1],d[2],d[3],d[4],d[5],d[6],d[7],d[8],d[9],d[10],d[11],d[12],d[13],d[14],d[15])
    cursor.execute(insertSql)
    
cursor.commit()
cnxn.close()


