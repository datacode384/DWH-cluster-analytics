import requests
import time

def getGenericData(entityType):
	externalJobNumber = int(time.time()*100)
	print(externalJobNumber)
	url = 'https://sys0290.cit.ergo.liferunoffinsuranceplatform.com/bpm-api/v1/test/batch/dwh'
	data = {"externalJobNumber":externalJobNumber,"externalAgent":"BPM-WAS- DMGR"}
	print("url", url)
	query ={"entity":entityType,"date-start":"2020-05-15T08:11:22.123"}
	response = requests.post(url, json=data,params = query,headers={"Content-Type":"application/json","accept":"application/json","Authorization":"jwt_token"})
	print("data",data)
	print(response)
	return response

getGenericData("GENERIC_TYPE")
getGenericData("GENERIC_STATUS")
