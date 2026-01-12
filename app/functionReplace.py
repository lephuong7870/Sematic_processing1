import json
from datetime import datetime
import os  

BASE_DIR = os.path.dirname(os.path.abspath(__file__))  
DATA_DIR = os.path.join(BASE_DIR, "..", "data")     
file_mapping =  os.path.join(DATA_DIR, "mapping.json")    

def replaceFunction( data_sample  ):
    with open( file_mapping , "r", encoding="utf-8") as f:
        mapping= json.load(f)

    columnToMapping = []
    for i in mapping:
        columnToMapping.append( i )

    data = data_sample['nodes']

    for index, i in enumerate(data):
        item = {}
        temp = i['properties']
        for key, value in temp.items():
            temp_key , temp_value = key.lower() , value[0].lower()
            item[temp_key] = temp_value
            for columnName in columnToMapping:
                if columnName in temp_key: 
                    try:
                        temp_value = mapping[columnName][temp_value ] 
                        item[temp_key] = temp_value 
                    except:
                        continue
                    break 
        data[index]['properties'] = item 


    data_sample['nodes'] = data  
    return data_sample 

