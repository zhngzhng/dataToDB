import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["meteorologicalData"]
mycollection = mydb["2000"]
mydict = {
  "name": "11",
  "id": "22",
  "lontitude":  "33",
  "latitude": "33",
  "data": {
    "year": "1212",
  }
}
x = mycollection.insert_one(mydict)
print(x)