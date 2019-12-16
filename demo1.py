import os
import pymongo
# 连接数据库，并创建表
myclient = pymongo.MongoClient("mongodb://223.2.42.18:27017/")
mydb = myclient["meteorologicalData"]
count = 23595096
# 读取站点数据
isd_history = open("F:\GraduationProject\ChinaMeteorologicalData\_description\isd-history.csv", "r")
# 站点数据
site_data = list()
for line in isd_history:
    site_data.append(line)
isd_history.close()
# 读取目录下文件
# 构造用于存储的对象
num = 2018
while num <= 2019:
    mycol = mydb[str(num)]
    path = "F:\GraduationProject\ChinaMeteorologicalData\china_isd_lite_" + str(num)
    # path = "F:\GraduationProject\ChinaMeteorologicalData\china_isd_lite_2001"
    dir_name = os.listdir(path)
    for dir_item in dir_name:
        USAF = " "
        Latitude = " "
        Longitude = " "
        Elevation = ' '
        Observation_Year = " "
        Observation_Month = " "
        Observation_Day = " "
        Observation_Hour = " "
        Air_Temperature = " "
        Dew_Point_Temperature = " "
        Sea_Level_Pressure = " "
        Wind_Direction = " "
        Wind_Speed_Rate = " "
        Sky_Condition_Total_Coverage_Code = " "
        Liquid_Precipitation_Depth_Dimension_one = " "
        Liquid_Precipitation_Depth_Dimension_six = " "
        # 选定当前读取的文件,并获取到站点信息
        site_id = dir_item.split("-")[0]
        for site_item in site_data:
            site_info = site_item.split(",")
            if site_id == site_info[0]:
                if len(site_info) == 1:
                    USAF = site_info[0]
                    Latitude = ' '
                    Longitude = ' '
                    Elevation = ' '
                elif len(site_info) == 2:
                    USAF = site_info[0]
                    Latitude = site_info[1]
                    Longitude = ' '
                    Elevation = ' '
                elif len(site_info) == 3:
                    USAF = site_info[0]
                    Latitude = site_info[1]
                    Longitude = site_info[2]
                    Elevation = ' '
                elif len(site_info) == 4:
                    USAF = site_info[0]
                    Latitude = site_info[1]
                    Longitude = site_info[2]
                    Elevation = site_info[3]
                # 打开数据文件，获取数据
                test_data_dir = open(path + "\\" + dir_item, "r")
                for test_data in test_data_dir:
                    test_data = test_data.split()
                    Observation_Year = test_data[0]
                    Observation_Month = test_data[1]
                    Observation_Day = test_data[2]
                    Observation_Hour = test_data[3]
                    Air_Temperature = test_data[4]
                    Dew_Point_Temperature = test_data[5]
                    Sea_Level_Pressure = test_data[6]
                    Wind_Direction = test_data[7]
                    Wind_Speed_Rate = test_data[8]
                    Sky_Condition_Total_Coverage_Code = test_data[9]
                    Liquid_Precipitation_Depth_Dimension_one = test_data[10]
                    Liquid_Precipitation_Depth_Dimension_six = test_data[11]
                    # 所有数据获取完成，将数据插入数据库内，构造数据格式
                    mydir = {
                        "site_info": {
                            "Id": USAF,
                            "Lat": Latitude,
                            "Lon": Longitude,
                            "Elev": Elevation
                        },
                        "data": {
                            "Observation_Year": Observation_Year,
                            "Observation_Month": Observation_Month,
                            "Observation_Day": Observation_Day,
                            "Observation_Hour": Observation_Hour,
                            "Air_Temperature": Air_Temperature,
                            "Dew_Point_Temperature": Dew_Point_Temperature,
                            "Sea_Level_Pressure": Sea_Level_Pressure,
                            "Wind_Direction": Wind_Direction,
                            "Wind_Speed_Rate": Wind_Speed_Rate,
                            "Sky_Condition_Total_Coverage_Code": Sky_Condition_Total_Coverage_Code,
                            "Liquid_Precipitation_Depth_Dimension_one": Liquid_Precipitation_Depth_Dimension_one,
                            "Liquid_Precipitation_Depth_Dimension_six": Liquid_Precipitation_Depth_Dimension_six
                        }
                    }
                    count = count + 1
                    mycol.insert_one(mydir)
                    print(count)
                test_data_dir.close()
    log_path = "F:\GraduationProject\ChinaMeteorologicalData\count.txt"
    log = open(log_path, "a")
    log.write(str(num) + "年数据条数:" + str(count) + "\n")
    log.close()
    num += 1






