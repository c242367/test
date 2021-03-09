########################################################################################################################
#Script Name: DPGCCEPH_Convert_JSON_To_CSV_HCP.py
#Developer Name: Kuldeeep  Tyagi
#DESCRIPTION: This script will convert the HCP JSON file into csv file with required attributes and logic for CODS Ruleset
#Version: 1.0 by TCS - Initial version
########################################################################################################################
import sys
import json
import csv
import time

timestr = time.strftime("%Y%m%d%H%M%S")
# GCCEPH_INBOUND="/mkt/ephub/srcfiles"
GCCEPH_INBOUND="C:/Users/L001053/Desktop/Projects_Lilly/MDM/HCP"
#GCCEPH_LOG="/mkt/crml4/log"
json_file_path = GCCEPH_INBOUND + "/POC_HCP_INPUT_FILE_2.json"
list_ProfileFields = ['Title',"FirstName","MiddleName","LastName","SecondaryLastName","PreferredName","Gender","GenderValue","OriginalSource","FormerName","PreferredName","Name","AlternateName","NameInitials","SuffixName","PrefixCode","Prefix","PrefixValue","Salutation",'CountryCode','CountryCodeValue',"ClassificationCode","ClassificationCodeValue",'SubClassificationCode','SubClassificationCodeValue',"SourceBlockStatus","ActiveFlag","EffectiveStartDate","EffectiveEndDate",'WinnerFlag','HCPReferenceId',"HCPUniqueId","EntityID","SourceType","Crosswalks","CrosswalkValue"]
# list_ProfileFields = ['HCPReferenceId']

list_SpecialtyFields = ["SpecialtyCode","SpecialtyType","Specialty","EffectiveStartDate","EffectiveEndDate","Rank","ActiveFlag","DataValidationStatusCode","HCPUniqueId","EntityID","SourceType","Crosswalks","CrosswalkValue"]
list_SegmentationFields = ["SegmentationCode","SegmentationTypeCode","CustomerODSIdentifier","Type","Segmentation","EffectiveStartDate","EffectiveEndDate","DataValidationStatusCode","HCPUniqueId","EntityID"]
list_EmailFields = ["Type", "Rank","EffectiveStartDate","EffectiveEndDate","Email","DataValidationStatusCode","ActiveFlag","HCPUniqueId","EntityID","SourceType","Crosswalks","CrosswalkValue"]
list_IdentifierFields = ["EffectiveStartDate","ID","Type",'SubType',"Rank",'MergeDate','CountryCode','CountryCodeValue',"EffectiveEndDate","DataValidationStatusCode","Status",'WinnerFlag','HCPReferenceId',"HCPUniqueId","EntityID","SourceType","Crosswalks","CrosswalkValue"]
# list_IdentifierFields = ['EffectiveStartDate']


list_PhoneFields = ["Type","Rank","EffectiveStartDate","EffectiveEndDate","Number","HCPUniqueId","CountryCode","DataValidationStatusCode","ActiveFlag","HCPUniqueId","EntityID","SourceType","Crosswalks","CrosswalkValue"]
list_OverrideFields = ['WinnerFlag','XREFMerge','HCPReferenceId','CountryCode','CountryCodeValue','SubClassificationCodeValue','GenderValue',"CountryCodeValue","CountryValue","CredentialCodeValue",'SpecialtyValue','ClassificationCodeValue','SegmentationValue','PrefixValue',"CustomerODSIdentifier","GCCID","HCPUniqueId","CountryCode","EntityID","RecordType"]
list_InactiveFields = ["InactiveCustODSID","HCPUniqueId","EntityID","SourceType","Crosswalks","CrosswalkValue"]
list_ErrorFields = ["ErrorID","ErrorReason"]
#Setting variables
IDField = "HCPUniqueId"
EntityIDField="EntityID"
RecordTypeField="RecordType"
var_CustODSID = ""
list_temp_inactiveCustIDs = []
processed_line_count = 0
error_line_count = 0
base_record_count = 0
error_record_count = 0
inactive_record_count = 0

#Setting output objects
output_ProfileList = []
output_SpecialtyList = []
output_SegmentationList = []
output_IdentifiersList =[]
output_EmailList =[]
output_PhoneList =[]
output_ErrorList = []
output_InactiveList = []

"""
Function to fetch uris of Reltio attributes. Paramter reuired to pass here are Data and Field.
example : For below dictionary, this function will return value "entities/1BLDJSQ9/attributes/CountryCode/1p3pL8cP7" for Country code paramter 
data = {"crosswalks": [
"attributes": ["entities/1BLDJSQ9/attributes/CountryCode/1p3pL8cP7",
               "entities/1BLDJSQ9/attributes/MatchInstitutes/1p3pL9ElR",
               "entities/1BLDJSQ9/attributes/MatchNameInitials/1p3pL88ZJ"]
               ]}
field =  "attributes/CountryCode"              
"""

def crosswalk_attributes(field, data,mode):
    final_data = []
    for i, j in enumerate(data):
        if mode =="Identifiers" and '/'+field in j and "/attributes/Identifiers/" in j:
            final_data.append(data[i])
        elif (field == "CountryCode/" or field == 'WinnerFlag/' or field =='HCPReferenceId/' or field == "HCPUniqueId/" or mode !="Identifiers") and field in j:
            final_data.append(data[i])
    return final_data

"""
Function club mutiple values if found in a list
Output will be like :
['value1'] or ['value1','value2','value3']
"""

def club_attribute_values(filter_value, uris, all_values, key_value,field =''):
    try:
        values = []
        for val in uris:
            value = ''
            if(field == 'HCPReferenceId'):
                value = (str([x[filter_value] for x in all_values if (val in x[key_value] and x['ov'] == True) ])[3:-2])
                HCPReferenceId = value
            elif field =="FirstName" or field =="MiddleName" or field =="LastName" or field =="SecondaryLastName" or field =="PreferredName" or field =="FormerName" or field =="Name" or field =="AlternateName" or field =="NameInitials" or field =="SuffixName" or field =="PrefixCode" or field =="Prefix" or field =="Salutation":
                for datavalue in all_values:
                    if datavalue[key_value] == val:
                        value = datavalue[filter_value]
            else:
                value = (str([x[filter_value] for x in all_values if val in x[key_value]])[3:-2])
            # value = value.replace('[u','').replace(']','').replace("'","")
            if value:
                values.append(value)
        return ("$#".join(values))
    except:
        return ("$#".join(''))

def club_addressPhone_values(uris, all_values, key_value):
    values = []
    for val in uris:
        value = ([x for x in all_values if val in x[key_value]])
        if value:
            for valA in value:
                values.append(valA)

    return values

def add_Record(mode):
    global error_record_count
    global output_ErrorList
    # HCPReferenceId = ''
    count=0
    list_temp_rank = []
    if mode == "Profile":
        json_section = [{"value":all_attributes}]
        profile_crosswalks = all_crosswalks
        list_FieldsToRead = list_ProfileFields
        list_AppendTo = output_ProfileList
    elif mode == "Specialities" and all_attributes.get("Specialities"):
        json_section = all_attributes["Specialities"]
        list_FieldsToRead = list_SpecialtyFields
        list_AppendTo = output_SpecialtyList

    elif mode == "Segmentation" and all_attributes.get("Segmentation"):
        json_section = all_attributes["Segmentation"]
        list_FieldsToRead = list_SegmentationFields
        list_AppendTo = output_SegmentationList

    elif mode == "Email" and all_attributes.get("Email"):
        json_section = all_attributes["Email"]
        list_FieldsToRead = list_EmailFields
        list_AppendTo = output_EmailList

    elif mode == "Phone" and all_attributes.get("Phone"):
        json_section = all_attributes["Phone"]
        list_FieldsToRead = list_PhoneFields
        list_AppendTo = output_PhoneList

    elif mode == "Identifiers" and all_attributes.get("Identifiers"):
        json_section = all_attributes["Identifiers"]
        list_FieldsToRead = list_IdentifierFields
        list_AppendTo = output_IdentifiersList
    
    else:
        return 0
        
    

    for record in json_section:
        # print("record")
        record_value = record["value"]
        # print(record_value)
        count_attributes=0
        count_attributes = len([x for x in all_crosswalks])
        HCPReferenceId = ''
        # print('count_attributes'+str(count_attributes))
        for i in range(count_attributes):
            crosswalk_record = ([x for x in all_crosswalks])[i]
            cross_type = str(crosswalk_record["type"]).replace('configuration/sources/','')
            cross_value = crosswalk_record["uri"]
            CrosswalkValue = crosswalk_record['value']
            record_dictionary = {}
            for field in list_FieldsToRead:
                if (type(record_value) is dict and record_value.get(field)):
                    if field == "Zip":
                        record_dictionary[field] = record_value[field][0]["label"]
                    elif field == "HCPReferenceId" and record_value.get('HCPReferenceId'):
                        for data in record_value.get('HCPReferenceId'):
                            if data.get('ov') and data['ov']:
                                HCPReferenceId = data['value']
                    else:
                        if crosswalk_record.get("attributes"):
                                attributes = crosswalk_record["attributes"]
                                # print('attributes '+str(attributes))
                                if attributes != []:
                                    Rank_uri = crosswalk_attributes(field+'/', attributes,mode)
                                    # print('Rank_uri '+str(Rank_uri))
                                    lookupcode =""
                                    Rank_val =""
                                    if field == 'CountryCode' or field == 'SubClassificationCode' or field == 'Gender' or field == "CountryCode" or field == "Country" or field == "CredentialCode" or field == 'Specialty' or field == 'ClassificationCode' or field =='Segmentation' or  field=='Prefix' or (field == "Type" and mode == "Segmentation"):
                                        Rank_val = club_attribute_values('lookupCode', Rank_uri, record_value[field], 'uri',field)
                                        if len(Rank_val) > 0:
                                            record_dictionary[field+'Value']=club_attribute_values('value', Rank_uri, record_value[field], 'uri',field)
                                            Rank_val = club_attribute_values('lookupCode', Rank_uri, record_value[field], 'uri',field)
                                        else:
                                            record_dictionary[field+'Value'] = club_attribute_values('value', Rank_uri, record_value[field], 'uri',field)
                                            Rank_val = ''   
                                    else: 
                                        Rank_val = club_attribute_values('value', Rank_uri, record_value[field], 'uri',field)
                                    # print('Rank_val '+str(Rank_val))
                                    list_temp_rank.append(Rank_val)
                                else:
                                    error_record_count = error_record_count + 1
                                    output_ErrorList.append({"ErrorID":cross_value,"ErrorReason":"CODS refRelation attributeURIs missing"})
                        record_dictionary[field] = '|'.join(list_temp_rank)
                        list_temp_rank = []
                elif field in list_OverrideFields:
                    if field == "HCPReferenceId" and all_attributes.get('HCPReferenceId'):
                        for data in all_attributes.get('HCPReferenceId'):
                            if data.get('ov') and data['ov']:
                                HCPReferenceId = data['value']
                    elif field == IDField or field == 'CountryCode'  or   field == 'WinnerFlag' or field == 'XREFMerge' or field == 'HCPReferenceId':
                        if crosswalk_record.get("attributes"):
                            attributes = crosswalk_record["attributes"]
                            # print('attributes '+str(attributes))
                            if attributes != [] and all_attributes.get(field): 
                                Rank_uri = crosswalk_attributes(field+'/', attributes,mode)
                                if field == 'CountryCode':
                                    record_dictionary[field] = club_attribute_values('lookupCode', Rank_uri, all_attributes[field], 'uri',field)
                                    record_dictionary[field+'Value'] = club_attribute_values('value', Rank_uri, all_attributes[field], 'uri',field)
                                else:
                                    record_dictionary[field] = club_attribute_values('value', Rank_uri, all_attributes[field], 'uri',field)
                            else:
                                record_dictionary[field]= ''
                        else:
                            record_dictionary[field]= None
                    elif field == EntityIDField:
                        record_dictionary[field] = EntityIDfieldValue
                    elif field == RecordTypeField:
                        record_dictionary[field] = RecordTypefieldValue
                    elif field == 'CountryCodeValue' or field == 'SubClassificationCodeValue' or field == 'GenderValue' or field == "CountryCodeValue" or field == "CountryValue" or field == "CredentialCodeValue" or field == 'SpecialtyValue' or field == 'ClassificationCodeValue' or field =='SegmentationValue' or  field=='PrefixValue':
                        if(record_dictionary.get(field)):
                            continue
                        else:
                            record_dictionary[field]=None
                    else:
                        record_dictionary[field] = None

                elif field == "MatchInstitutes":
                    record_dictionary[field] = "NOT POPULATED"
                    
                else:
                    record_dictionary[field] = None
            record_dictionary['SourceType']=cross_type
            record_dictionary["Crosswalks"]=cross_value
            record_dictionary['HCPReferenceId']=HCPReferenceId
            record_dictionary["CrosswalkValue"]= CrosswalkValue
            if(mode == "Identifiers" and record_dictionary.get('Type') and len(record_dictionary['Type']) == 0 and record_dictionary.get('ID') and len(record_dictionary['ID']) == 0 ):
                continue
            else:
                list_AppendTo.append(record_dictionary)
        
    return 1
    
def get_IdentifierStatus():

    global var_CustODSID
    global var_GCCID
    retValue = ""
    countCustId = 0
    
    if all_attributes.get("Identifiers"):
        for record in all_attributes["Identifiers"]:
            identifier = record["value"]                
            if identifier.get("Type") and identifier["Type"][0]["value"] == "Customer ODS Cust ID":
                if identifier.get("ID"):
                    countCustId = countCustId + 1
                    temp_CustODSID = identifier["ID"][0]["value"]
                else:
                    pass
            elif identifier.get("Type") and identifier.get("ID") and identifier["Type"][0]["value"] == "GCC Customer ID":
                var_GCCID = identifier["ID"][0]["value"]
            else:
                pass    
    
        if countCustId == 1:            
            retValue = "Exist"
            var_CustODSID = temp_CustODSID
        
        elif countCustId > 1:
            retValue = "Multiple"
        
        else:
            retValue = "Reltio"
            var_CustODSID = "" 
    
    else:
        retValue = "Reltio"
    
    return retValue
    
def create_CSVFiles(mode):

    global header_List
    # print('File Creting')
    if mode == "Profile":
        csv_file_name = GCCEPH_INBOUND + "/Denormalization_POC_Source_EPH_HCP_profile.csv"
        output_List = output_ProfileList
        header_List = list_ProfileFields

    elif mode == "Address":
        csv_file_name = GCCEPH_INBOUND + "/Denormalization_POC_Source_EPH_HCP_address.csv"
        output_List = output_AddressList
        header_List = list_AddressFields

    elif mode == "AddressPhone":
        csv_file_name = GCCEPH_INBOUND + "/Denormalization_POC_Source_EPH_HCP_address_phone.csv"
        output_List = output_AddressPhoneList
        header_List = list_AddressPhoneFields

    elif mode == "Specialities":
        csv_file_name = GCCEPH_INBOUND + "/Source_EPH_HCP_specialty.csv"
        output_List = output_SpecialtyList
        header_List = list_SpecialtyFields

    elif mode == "Segmentation":
        csv_file_name = GCCEPH_INBOUND + "/Source_EPH_HCP_segmentation.csv"
        output_List = output_SegmentationList
        header_List = list_SegmentationFields

    elif mode == "Email":
        csv_file_name = GCCEPH_INBOUND + "/Denormalization_POC_Source_EPH_HCP_email.csv"
        output_List = output_EmailList
        header_List = list_EmailFields

    elif mode == "Phone":
        csv_file_name = GCCEPH_INBOUND + "/Denormalization_POC_Source_EPH_HCP_phone.csv"
        output_List = output_PhoneList
        header_List = list_PhoneFields

    elif mode == "Identifiers":
        csv_file_name = GCCEPH_INBOUND + "/Denormalization_POC_Source_EPH_HCP_Identifiers.csv"
        output_List = output_IdentifiersList
        header_List = list_IdentifierFields
    
    elif mode == "Inactive":
        csv_file_name = GCCEPH_INBOUND + "/Denormalization_POC_Source_EPH_HCP_Inactive.csv"
        output_List = output_InactiveList
        header_List = list_InactiveFields   

    elif mode == "Error":
        csv_file_name = GCCEPH_INBOUND + "/Denormalization_POC_Source_EPH_HCP_Error.csv"
        output_List = output_ErrorList
        header_List = list_ErrorFields
        
    else:
        return 0   
    if output_List:
        try:
            with open(csv_file_name, 'w+') as f:
                # , encoding="utf8"
                header1 = output_List[0].keys()
                header =[]
                for value in header1:
                    header.append(value)
                header.sort()  
                # sorted(header)
                writer = csv.DictWriter(f, header, quoting=csv.QUOTE_ALL)
                writer.writeheader()
                countsucc = 0
                countfail = 0
                for row in output_List:
                    try:
                        data ={}
                        for key in header:
                            if row.get(key):
                                # print(row[key].encode("ascii"))
                                data[key] = row[key]
                            else:
                                data[key] = None
                        writer.writerow(data) 
                    except UnicodeDecodeError:  
                        print ("Unicode error")
                    except Exception as e:
                        countfail = countfail + 1
                        print('Fail Number'+str(countfail))
                        print ("Error in writing the record in the file")
                        print (e)
                        continue
                                      
        except UnicodeDecodeError:  
            print ("Unicode error")
        except Exception as e:
            print ("Error in writing the record in the file")
            print (e)
        return 1
    else:
        try:
            with open(csv_file_name, 'w+') as f:
                header = header_List
                # header.sort()
                writer = csv.DictWriter(f, header, quoting=csv.QUOTE_ALL)
                writer.writeheader()
                    
        except UnicodeDecodeError:  
            print ("Unicode error")     
        except Exception as e:
            print ("Error in writing the blank file")
            print (e)
            return 0

#try:

with open(json_file_path) as in_file:
    # data = json.load(in_file)
    # print(data)
    try:
        count = 0
        for line in in_file:
            try:
                count  = count + 1
                try:
                    processed_line_count = processed_line_count + 1
                    json_line = line[:line.rindex('}') + 1]
                    record = json.loads(json_line)
                    for i in record:
                        print(i)
                except UnicodeDecodeError:  
                    print ("Unicode error")  
                except Exception as e:         
                    error_line_count = error_line_count + 1
                    print ("Warning: Error in reading the json file")
                    print (e)
                    continue
                # print(record.keys())
                if (record.get("attributes")):
                    all_attributes = record["attributes"]
                    all_crosswalks = record["crosswalks"]
                    if record.get("uri"):
                        EntityIDfieldValueTemp = record["uri"]
                    else:
                        EntityIDfieldValueTemp = ''
                    # print('Count -->',count,' <<<<---   Data convert start for Entity ID   --->>>>',EntityIDfieldValueTemp)
                    if record.get("type"):
                        RecordTypefieldValueTemp = record["type"]
                    else:
                        RecordTypefieldValueTemp = ''
                    EntityIDfieldValueVar= str(EntityIDfieldValueTemp.encode("ascii"))
                    #print(EntityIDfieldValueVar.find("/"))
                    #print(25:len(EntityIDfieldValueVar))
                    EntityIDfieldValue=EntityIDfieldValueVar[EntityIDfieldValueVar.find("/")+1:len(EntityIDfieldValueVar)][0:-1]
                    RecordTypefieldValueVar= str(RecordTypefieldValueTemp.encode("ascii"))
                    RecordTypefieldValueT=RecordTypefieldValueVar[RecordTypefieldValueVar.find("/")+1:len(RecordTypefieldValueVar)][0:-1]
                    RecordTypefieldValue=RecordTypefieldValueT[RecordTypefieldValueT.find("/")+1:len(RecordTypefieldValueT)]
                    list_temp_inactiveCustIDs = []
                    var_CustODSID = ""
                    var_GCCID = ""
                    # var_status = get_IdentifierStatus()
                    # if (var_status == "Exist") or (var_status == "Reltio") :
                    base_record_count = base_record_count + 1  
                    add_Record("Profile")
                    add_Record("Specialities")
                    add_Record("Segmentation")
                    add_Record("Identifiers")
                    add_Record("Email")
                    add_Record("Phone")
                    # print('<<<<---   Data convert End for Entity ID   --->>>>',EntityIDfieldValueTemp)
                else:
                    continue       
            except Exception as e:         
                error_line_count = error_line_count + 1
                print ("Warning: Error in reading the json file")
                # print(line)

                print (e)
                continue
    except Exception as e:         
        error_line_count = error_line_count + 1
        print ("Warning: Error in reading the json file")
        print (e)
                
create_CSVFiles("Profile")
create_CSVFiles("Specialities")
create_CSVFiles("Segmentation")
create_CSVFiles("Identifiers")
create_CSVFiles("Email")
create_CSVFiles("Phone")
create_CSVFiles("Error")
create_CSVFiles("Inactive")
print ("Total Lines: " + str(processed_line_count))
print ("Error Lines: " + str(error_line_count))
print ("Base Record Count: " + str(base_record_count))
print ("Error Record Count: " + str(error_record_count))
