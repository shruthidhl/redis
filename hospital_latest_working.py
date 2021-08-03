import redis
import pandas as pd
from googleplaces import GooglePlaces, types, lang
import requests
import json
import geocoder
from urllib.parse import urlencode

from requests.models import Response

#hospitals= pd.read_csv("C:/Users/Dell/Documents/SRH_Course/DataEngineering1/Yellow/shruthi/HospitalData.csv")
#hospitals.head()

redis_con = redis.Redis(port=6379)
API_KEY = 'AIzaSyCl4WcOwBQrQ51RtZ8l3M-ioEYs9ooXqNo'

#
lat, lng = 49.488888, 8.469167

results='results'
geometry='geometry'
location='location'
opening_hours='opening_hours'
main_key_for_db='hospitals:Mannheim'
# Initialising the GooglePlaces constructor
google_places = GooglePlaces(API_KEY)


#placed geo co-ordinates for  Mannheim=49.4875° N, 8.4660° E
placeapi_base_url="https://maps.googleapis.com/maps/api/place/nearbysearch/json"

params = { 
           "location": f"{lat},{lng}",
            "radius":20000,
            "type":"hospital",
            "language": "en",
            "key": API_KEY,
            "sensor":"true"}
url_params = urlencode(params)
url = f"{placeapi_base_url}?{url_params}"
print("url")
url=url+'&opennow'
print(url)

#Making call to google APis
response=0
response = requests.get(url)
print("status code")
print(response.status_code)
json_data = json.loads(response.text)
#print(json_data)
print(type(json_data))
#print(len(result.json()['results']))
if response.status_code in range(200, 299):
   
    print(requests)
    
    hospital_iterator = iter(response.json()[results])
    try:
      for hospital in hospital_iterator:
        if hospital is not None:
          geometry_coordinates = hospital[geometry]
          if geometry_coordinates is not None:
           location_coordinates=geometry_coordinates[location]
           name=hospital['name']
           print(name)
           opening_hours=hospital['opening_hours']
           
           print(opening_hours)
           if opening_hours is not None:
             open_now=opening_hours['open_now']
             #check if hospital is open at that time 
             print('open_status')
             print(open_now)
             if open_now==True :
              redis_con.geoadd(main_key_for_db, location_coordinates['lat'],location_coordinates['lng'],name)
    except KeyError:
      print("Oops!  Try again...")
      print(KeyError)




#check for 1 km distance 
    
places_mannheim={
      
       #'jungbusch':'jungbusch,Hafenstrasse, 68159 Mannheim',
        'Richard-Wagner-Str':'Richard-Wagner-Str 4, 68165 Mannheim'     
     #   'Am Steingarten':'Am Steingarten 14, 68169 Mannheim'
}
      

geocode_base_url="https://maps.googleapis.com/maps/api/geocode/json"
params = {             
           "key": API_KEY,
            }

for key, value in places_mannheim.items():
  print("iterating over key and value")
  params['address']=value
  url_params = urlencode(params)
  url = f"{geocode_base_url}?{url_params}"
  print("url")
  print(url)
  try:
    #Making call to google APis
    result=0
    response= requests.get(url)
  
    if response.status_code in range(200, 299):
        
        location= response.json()['results'][0]['geometry']['location']
        print(location)
        name=response.json()['results'][0]['address_components'][0]['long_name']
        print(name)
        redis_con.geoadd(main_key_for_db, location['lat'],location['lng'],key)
        
    
  except:
    print("Oops! That was no valid number.  Try again...")

increments=[1,5,10,50]
search_results=[]
for i in increments:
 print(i)
 search_results=redis_con.georadiusbymember(main_key_for_db , key, i, unit="km",)
 for j in range(0, len(search_results)):
   search_results[j]=(search_results[j]).decode('UTF-8')
 if key in search_results:
    search_results.remove(key)
 if(len(search_results)>0):
   break
 elif(len(search_results)==0):
    continue

print('final search results')
search_results=set(search_results)
for i in search_results:
  print(i)
 
