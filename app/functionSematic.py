import json
from datetime import datetime
from rapidfuzz import process, fuzz
import os  

BASE_DIR = os.path.dirname(os.path.abspath(__file__))  
DATA_DIR = os.path.join(BASE_DIR, "..", "data")   
  
file_metadataTV =  os.path.join(DATA_DIR, "metadataTV.json")    

file_configTV =  os.path.join(DATA_DIR, "configTV.json"  )  
 
file_otherConfig =  os.path.join(DATA_DIR, "otherConfig.json"  )  
 
def nameColumnTV1( string ):
    with open( file_metadataTV , "r", encoding="utf-8") as f:
        configMapTV = json.load(f) 
    lstE = []
    lstV = []
    for key , value in configMapTV.items() :
        lstE.append( key ) 
        lstV.append( value )
    
    charE = ""
    nameColumn = ""
    for char in lstE : 
        try : 
            index =  string.find( char )
            if index != -1: 
                string = string.replace( char , "")
                nameColumn += f" {configMapTV[char]}" 
        except:
             pass 
    if len( nameColumn) == 0:
        return string 
    return nameColumn.strip()


class nodeData:
    def __init__( self, data ):
        self.uuid = []
        self.primaryNode = []
        self.dataNode = {}
        self.detailNode = []
        self.rawNode = data

    def processNode( self , addData  = None   ):
        if addData == None:
            rawData = self.rawNode 
            
        else:
            rawData = addData 
        ## self.uuid.append( rawData['id'])
        entity = rawData['entity'] 
        if ( entity == "Netizen" ) or  (  entity == "Citizen"  ) :

            extractNode = {}
            for ikey , ivalue  in rawData['properties'].items() : 
                try :
                    if  ( "00:00:00" in ivalue) :
                        date_obj = datetime.strptime(ivalue[:10], "%Y-%m-%d")
                        ivalue = date_obj.strftime("%d-%m-%Y")
                except:
                    pass 
                if ivalue.isdigit() == False:
                    ivalue = ivalue.lower() 
                extractNode[ivalue]  =  extractNode.get( ivalue , []) 
                extractNode[ivalue].append(ikey )
                ## get primary 
                if ( "identityid" == ikey )  and (len( ivalue) == 12)  :
                    self.primaryNode.append( ivalue )
                elif ( "nationalid" == ikey )  and (len( ivalue) == 9)  :
                    self.primaryNode.append( ivalue )
                elif "fullname" == ikey :
                    self.primaryNode.append( ivalue )
            self.dataNode.update(extractNode)
        
        else:
            extractNode = {}
            metadataValue = None
            if entity == "Vehicle" : 
                try:
                    metadataValue  = rawData['properties']['licenceplateNumber']
                except:
                    metadataValue = None 
            elif entity == "Person" :
                try:
                    metadataValue =  rawData['properties']['socialinsuranceNumber']  
                except:
                    try:
                        metadataValue =  rawData['properties']['healthinsuranceNumber']  
                    except:
                        metadataValue  =  rawData['properties']['fullname']
            elif entity ==  "Phone" : 
                try:
                    metadataValue  = rawData['properties']['phone']
                except:
                    metadataValue = None 
            elif entity ==  "FacebookProfile" : 
                try:
                    metadataValue  = rawData['properties']['fbid']
                except:
                    metadataValue = None 

            for ikey , ivalue  in rawData['properties'].items() : 
                ## convert date 
                try :
                    if  ( "00:00:00" in ivalue) :
                        date_obj = datetime.strptime(ivalue[:10], "%Y-%m-%d")
                        ivalue = date_obj.strftime("%d-%m-%Y")
                except:
                    pass 
                if ivalue.isdigit() == False:
                    ivalue = ivalue.lower()  
                extractNode[ivalue]  =  ikey 

            itemToAdd  = (  metadataValue , extractNode )
            self.detailNode.append( itemToAdd )

        return True





## check compare2List
def checkCompare2List( a  : list , b : list ):
    """
    a : listPrimarykey (new)
    b : had_exist
    
    """
    common = []
    common = list(set( a  ) & set( b ))   
    if len( common ) == 0 :
        only_letters = [s for s in a if all(ch.isalpha() or ch.isspace() or ch.isdigit() for ch in s)]
        longest_str_a = max(only_letters, key=len, default=None)

        only_letters = [s for s in b if all(ch.isalpha() or ch.isspace()  for ch in s)]
        longest_str_b = max(only_letters, key=len, default=None)
        scoremapping = fuzz.ratio(longest_str_a , longest_str_b)  
        if scoremapping >=  70:
            return True 
    else:
        return True 
    return False

## get list Primary of Node 
## return list 
def  getPrimaryOfNode( data : dict ):
    lst = []

    for key, value in data.items():
        if value.isdigit() == False:
            value = value.lower() 

        if ( "identityid" == key )   and (len( value) == 12)  :
            lst.append( value )
        elif ( "nationalid" == key )   and (len( value) == 9)  :
            lst.append( value )
        elif  (  "fullname" == key ) or  ( "owner" == key ) : 
            lst.append( value )
    return lst 

def functionMain( data  ):

    all_people = ""
    node_main  = data['nodes']

    dataAllNode = []
    for i in node_main:
        if len( dataAllNode ) == 0:
            temp = nodeData( i )
            temp.processNode()
            dataAllNode.append( temp )
        else:
            ## So sánh element Node trong ALL
            data = i['properties']
            lstPrimary =  getPrimaryOfNode( data)
            checkMerge = False
            for j in dataAllNode:
                had_exist = j.primaryNode 
                checkMerge = checkCompare2List( lstPrimary , had_exist )
                if checkMerge :       
                    j = j.processNode( i )
                    break
            if checkMerge == False :
                temp = nodeData( i )
                temp.processNode()
                dataAllNode.append( temp )


    for i in dataAllNode:
        if len( i.primaryNode) > 0:

            data_all_identity =   i.detailNode
            data_primary = i.primaryNode
            identityid  = None 
            nationalid  = None 
            try:   
                fullname  = min([x for x in data_primary if x.replace(' ', '').isalpha()])
                fullname  = fullname.title()
            except:
                pass 

            try: 
                identityid  = [x for x in data_primary  if x.isdigit()  and  len(x) == 12 ][0] 
            except:
                pass 

            try:
                nationalid  = [x for x in data_primary  if x.isdigit()  and  len(x) == 9 ][0] 
            except:
                pass 

            data = i.dataNode

            infor_families = [ "mother"  , "father" , "couple"  , "child" , "householder"]
            infor_address = [ "residential" , "hometown" , "address"  ,  "placeofbirth"]
            dct_families = {}
            dct_metadata = {}
            dct_address  = {}
            for key, value in data.items():
                ## get families 
                families = False
                address  = False 
                for value_i in value:
                    for infor in infor_families:
                        if infor in value_i:
                            char_i = value_i.replace( infor , "")
                            try :
                                a , b = char_i.split('.')
                                index = (a.replace('[' , '').replace(']' , ''))
                                dct_families[infor] = dct_families.get( infor , {})
                                dct_families[infor][index] = dct_families[infor].get( index , {})
                                dct_families[infor][index][b] = key 
                                families = True 
                                break
                            except:
                                break 
                    for infor in infor_address:
                        if infor in value_i:
                            char_i = value_i.replace( infor , "")
                            try :
                                a , b = char_i.split('.')
                                index = (a.replace('[' , '').replace(']' , ''))
                                dct_address[infor] = dct_address.get( infor , {})
                                dct_address[infor][index] = dct_address[infor].get( index , {})
                                dct_address[infor][index][b] = key 
                                address = True  
                                break
                            except:
                                break 


                if (families == False) and (address == False) :
                    try:
                        dct_metadata[value[0]] = key
                    except:
                        pass

            data1 = dct_families 
            families_final = {}
            families_final["child"]  = []
            for i in data1 : 
                if  ( i == "mother" ) or ( i == "father") or ( i == "couple") or (i == "householder") :
                    families_final[i] = data1[i]["0"]
                elif i == "child" :
                    for child in data1[i]:
                        families_final["child"].append( data1[i][child ])

            ## FAMILIES
            text_families= f"Thông tin thành viên gia đình gồm: "
            mapTV = {
                "mother"  : "có mẹ tên" ,
                "father" : "có cha tên" , 
                "child" : "có con tên" ,
                "couple" : "có vợ/chồng tên" 
            }
            for i in infor_families:
                try:
                    text = ""
                    if i == "child" :
                        for child_j  in families_final[i]:
                            text = f"""{mapTV[i]} là {child_j['fullname'].title()}, """ 
                            text_families += text 
                    elif i == "householder" :
                        text = f"""thuộc chủ hộ gia đình của {families_final[i]["fullname"].title()}, """
                        text_families += text 
                
                    elif i in families_final :
                        text = f"""{mapTV[i]} là {families_final[i]["fullname"].title()}, """
                        text_families += text 
                
                except:
                    continue
            text_families = text_families.strip(' ,')
    
            ## ADDRESS 

            mappingAddress = {
                "residential" : "địa chỉ thường trú" , 
                "address" : "địa chỉ hiện tại" ,
                "hometown" : "địa chỉ quê quán" , 
                "placeofbirth" : "địa chỉ nơi sinh"

            }
            data1 = dct_address 
            address_final = {}
            for i in data1:
                text = ""
                add  = data1[i]["0"]
                if "detail" in add:
                    text += data1[i]["0"]["detail"] + ", "
                if "commune" in add:
                    text += data1[i]["0"]["commune"] + ", "
                if "district" in add:
                    text += data1[i]["0"]["district"] + ", " 
                if "province" in add:
                    text += data1[i]["0"]["province"] + ", " 
                text = text.strip(', ')
                address_final[text] = address_final.get( text , [])
                address_final[text].append( i )
            
            text_address_final = " Thông tin chi tiết về địa chỉ sinh sống, làm việc: "
            for key, value in address_final.items():
                if len( value ) > 1:
                    try:
                        combine = ""
                        for value_i in value:
                            combine += mappingAddress[value_i] + ", "
                        combine = combine.strip(", ")
                        text_address_final += f"{combine}: {key.title()}, "
                    except:
                        pass 
                else:
                    value = value[0]
                    map_here = mappingAddress[value]
                    text_address_final += f"{map_here}: {key.title()}, "
        
            text_address_final = text_address_final.strip(', ')

            ## Lấy thông tinh định danh 
            for key, value in dct_metadata.items():
                if  ',' in value:
                    value = value.split(',')
                    dct_metadata[key] = value 
                else:
                    dct_metadata[key] = [value ]
        
            getID_metadata = []
            for key, value in dct_metadata.items():
                if isinstance( value , list):
                    getID_metadata += value 
                else:
                    getID_metadata.append( value )
            all_had_exist = getID_metadata  + data_primary
        
            
            all_detail = []
            for i in  data_all_identity:
                a,data  =  i
                try:
                    infor_delete_metadata  = []
                    code_infor = None
                    if 'socialinsurancenumber' in data.values():
                        try:
                            lst1 = dct_metadata['socialinsurancenumber']  
                            lst2 = list(data.keys()) 
                            ll = list(set(lst1) & set(lst2))
                            code_infor = list(set(ll) - set(data_primary)) 
                            try:
                                code_infor = code_infor[0]
                            except:
                                pass 
                            if len(ll) == 0:
                                for i, j in data.items():
                                    if j == 'socialinsurancenumber':
                                        code_infor =  i 
                                        dct_metadata['socialinsurancenumber'].append(code_infor)
                                        break
                                        
                        except:
                            pass  
                        temp = {}
                        for key,value in data.items():
                            if key not in all_had_exist:
                                temp[value] = key 
                        infor_delete_metadata  = [ "socialinsurancenumber" , code_infor  , temp ] 
                        all_detail.append( infor_delete_metadata )

                    elif 'healthinsurancenumber'  in data.values():
                        try:
                            lst1 = dct_metadata['healthinsurancenumber']  
                            lst2 = list(data.keys()) 
                            ll = list(set(lst1) & set(lst2))
                            code_infor = list(set(ll) - set(data_primary)) 
                            try:
                                code_infor = code_infor[0]
                            except:
                                pass 
                            if len(ll) == 0:
                                for i, j in data.items():
                                    if j == 'healthinsurancenumber':
                                        code_infor =  i 
                                        dct_metadata['healthinsurancenumber'].append(code_infor)
                                        break 
                        except:
                            pass 
                        temp = {}
                        for key,value in data.items():
                            if key not in all_had_exist:
                                temp[value] = key 
                        infor_delete_metadata  = [ "healthinsurancenumber" , code_infor  , temp ] 
                        all_detail.append( infor_delete_metadata )

                    elif 'licenceplatenumber' in data.values():
                        try:
                            lst1 = dct_metadata['licenceplatenumber']  
                            lst2 = list(data.keys()) 
                            ll = list(set(lst1) & set(lst2))
                            code_infor = list(set(ll) - set(data_primary)) 
                            try:
                                code_infor = code_infor[0]
                            except:
                                pass 
                            if len(ll) == 0:
                                for i, j in data.items():
                                    if j == 'licenceplatenumber':
                                        code_infor =  i 
                                        dct_metadata['licenceplatenumber'].append(code_infor)
                                        break 
                        except:
                            pass 
                        temp = {}
                        for key,value in data.items():
                            if key not in all_had_exist:
                                temp[value] = key 
                        infor_delete_metadata  = [ "licenceplatenumber" , code_infor  , temp ] 
                        all_detail.append( infor_delete_metadata )  
                    elif 'fbid' in data.values():
                        try:
                            lst1 = dct_metadata['facebookid']  
                            lst2 = list(data.keys()) 
                            ll = list(set(lst1) & set(lst2))
                            code_infor = list(set(ll) - set(data_primary)) 
                            try:
                                code_infor = code_infor[0]
                            except:
                                pass 
                            if len(ll) == 0:
                                for i, j in data.items():
                                    if j == 'fbid':
                                        code_infor =  i 
                                        dct_metadata['facebookid'].append(code_infor)
                                        break 
                        except:
                            pass 
                        temp = {}
                        for key,value in data.items():
                            if key not in all_had_exist:
                                temp[value] = key 
                        infor_delete_metadata  = [ "facebookid" , code_infor  , temp ] 
                        all_detail.append( infor_delete_metadata )  

                    elif 'phone' in data.values():
                        try:
                            lst1 = dct_metadata['phone']  
                            lst2 = list(data.keys()) 
                            ll = list(set(lst1) & set(lst2))
                            code_infor = list(set(ll) - set(data_primary)) 
                            try:
                                code_infor = code_infor[0]
                            except:
                                pass 
                            if len(ll) == 0:
                                for i, j in data.items():
                                    if j == 'phone':
                                        code_infor =  i 
                                        dct_metadata['phone'].append(code_infor)
                                        break 
                        except:
                            pass 
                        temp = {}
                        for key,value in data.items():
                            if key not in all_had_exist:
                                temp[value] = key 
                        infor_delete_metadata  = [ "phone" , code_infor  , temp ] 
                        all_detail.append( infor_delete_metadata )  

                    else:
                        infor_label =  "other"
                        for i, j in data.items():
                            if "social" in j:
                                code_infor = "other"
                                infor_label =  "socialinsurancenumber"   
                                break 
                            elif "phone" in j:
                                code_infor = "other"
                                infor_label =  "phone"  
                                break 

                        for key,value in data.items():
                            if key not in all_had_exist:
                                temp[value] = key 
                        infor_delete_metadata  = [ infor_label , code_infor  , temp ] 
                        all_detail.append( infor_delete_metadata ) 
                except:
                    pass 

            
            text_identity_final = ""
            if fullname != None:
                text_identity_final = f"Người tên {fullname.title()}"

            if identityid != None:
                string = f", có mã định danh cá nhân CCCD là {identityid}"
                text_identity_final += string 
            if nationalid != None :
                string = f", có mã định danh cá nhân CMND là {nationalid}"
                text_identity_final += string 
            
            with open(file_configTV , "r", encoding="utf-8") as f:
                configTV = json.load(f)
            for key , value in configTV.items():
                try: 
                    string = dct_metadata.get(key , "")[0]
                    if len( string) > 0 and ( "number" in i):
                        text_identity_final += f" , { value  }: { string.upper()}"
                    elif len(string)  > 0 and (  string != "chưa có thông tin" ) : 
                        text_identity_final += f" , {value  }: { string }" 
                except :
                    continue 
                
        

            all_text_identity_detail = [ ]
            if len(data_all_identity ) > 0 :
                had_field_exist =  {}
                for  i in all_detail: 
                    text_here ="" 
                    a, codeID , data = i 
                    
                    if "socialinsurancenumber" == a:
                        text_here = "thông tin đăng kí bảo hiểm xã hội, địa chỉ làm việc, tên và địa chỉ công ty có thể gồm:"
                    elif "healthinsurancenumber"  == a:
                        text_here = "thông tin đăng kí bảo hiểm y tế, tên và địa chỉ làm việc, tên và địa chỉ công ty có thể gồm:" 
                    elif "phone"  == a :
                        if codeID == None:
                            codeID = ""
                        text_here = f"""Số điện thoại { codeID.upper() } đăng kí sử dụng gồm các thông tin liên quan như:""" 
                    elif "licenceplatenumber"  == a :
                        if codeID != None :
                            text_here = f"Phương tiện, xe sở hữu với biển số {codeID.upper()} gồm các thông tin liên quan:" 
                        else:
                            text_here = f"Phương tiện, xe sở hữu gồm các thông tin liên quan:" 
                    elif "other" == a  :
                        text_here = f"một số thông tin khác gồm: "
                    had_field_exist[codeID] = had_field_exist.get( codeID , [])
                    for key, value in data.items():    
                        try:
                            nameTV =  nameColumnTV1( key )
                            if (nameTV == key) or  ( key in had_field_exist[codeID ] )  :
                                continue 
                            else:
                                text = f" {nameTV}:  {value },"
                                text_here += text
                        except:
                            continue
                        had_field_exist[codeID].append( key )
                    
                        
                    all_text_identity_detail.append(text_here.strip(', '))
            
            text_finally_here = f"{ text_identity_final}. { text_families  } . { text_address_final  }"

            if len( all_text_identity_detail ) >  0 :
                for i in all_text_identity_detail : 
                    text_finally_here += f". {i}"
        
            all_people += f"{ text_finally_here }. " 
        else:
            text_finally_here = ""
            def nameColumnTV( string ):
                with open( file_otherConfig , "r", encoding="utf-8") as f:
                    configMapTV = json.load(f) 
                lstE = []
                lstV = []
                for key , value in configMapTV.items() :
                    lstE.append( key ) 
                    lstV.append( value )
                
                charE = ""
                nameColumn = ""
                for char in lstE : 
                    try : 
                        index =  string.find( char )
                        if index != -1: 
                            string = string.replace( char , "")
                            nameColumn += f" {configMapTV[char]}" 
                    except:
                        pass 
                if len( nameColumn) == 0:
                    return string 
                return nameColumn.strip()
            data3 =  i.detailNode 

            for i in data3 :
                try:
                    text_here = "Một số thông tin liên quan khác: "
                    a ,dct_data  = i 
                    for key , value in dct_data.items( ):
                        nameColumn = nameColumnTV( value ) 
                        text_here += f"{nameColumn} : {key}, "
                    text_here =  text_here.strip(', ')  
                    text_finally_here += f". {text_here }"
                except:
                    continue 
                all_people += f"{ text_finally_here }. " 
           
    return all_people 


