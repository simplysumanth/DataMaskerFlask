import pandas as pd
from modules.helper import isdate, insert_dict_folder, access_key_id, secret_access_key, region_name, accountId, bucket, get_buckets
from modules.macro import macro

def read_data_file(path):
    df = pd.read_csv(path)
    column_details_list = []
    cols_list=list(df.columns)
    for col in cols_list:
        column_details= {}
        column_details['column_name'] = col
        series=df[col]

        if pd.api.types.is_float_dtype(series):
            masking_rules = ["none","random_float",'translate',"encrypt"]
            column_details['masking_rules'] = masking_rules
            attributes = ["none",'unique','not unique']
            column_details['attributes'] = attributes

        elif pd.api.types.is_integer_dtype(series):
            masking_rules = ["none",'random_int','translate',"encrypt"]
            column_details['masking_rules'] = masking_rules
            attributes = ["none",'unique','not unique']
            column_details['attributes'] = attributes

        elif pd.api.types.is_string_dtype(series):
            if isdate(str(df[col][0])):
                masking_rules = ["none","date_mask"]
                column_details['masking_rules'] = masking_rules
                column_details['attributes'] = "date"
            else:
                masking_rules = ["none","encrypt",'translate']
                column_details['masking_rules'] = masking_rules
        
        else:
            masking_rules = ["none","encrypt",'translate']
            column_details['masking_rules'] = masking_rules
       
        column_details_list.append(column_details)
       
    return column_details_list, cols_list

def store_config_json(full_masking_req, file):
    insert_dict_folder(full_masking_req,'data/config/', access_key_id, secret_access_key, region_name, accountId,[] , bucket,f'{file}{macro["config"]}')

def read_data_from_s3(access_key_id, secret_access_key, region_name, account_id):
    buckets = get_buckets(access_key_id, secret_access_key, region_name, account_id, "", "" ,"")
