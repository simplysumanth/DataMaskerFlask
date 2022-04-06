from modules.macro import macro
import pandas as pd
from modules.helper import find_files, insert_dict_folder


def prepare_config(file,Macie_List,default,custom,df): 
    access_key_id='AKIAYV2B3AOU3RCKJPVT'
    secret_access_key='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'
    region_name='us-east-1'
    accountId='596601668521'
    name=''
    s3=[]
    bucket='datamasking123'
    
    
    print("**********::"+file+"::******************")
    
    config_dict={}
    config_dict["ENCRYPTED_MAP_ALPH_NUM"]=macro["ENCRYPTED_MAP_ALPH_NUM"]
    config_dict["MAP_ENCRYPT_KEY"]=macro["MAP_ENCRYPT_KEY"]
    df=df
    num=-1
    
    if type(df)==type(num):
        try:
            
            df=pd.read_csv(find_files.find_files(file+'.csv','C:'))
            print("hiii")
        except:
            print(f"data file {file} not found !")
       
    print("********************************************************")
    
    total_cols=list(df.columns)
    pii_cols_list_index,rules,chars_list,format_list=0
    #cols_check_box.open_check_box(Macie_List,total_cols,file,default,custom,df)
    
    
    if pii_cols_list_index!=-1:
        format_list=list(filter(None, format_list))
        dic_list=[]
        date_index=0
        for i in range(len(total_cols)):
               col=total_cols[i]
               series=df[col]
               dic={}
               dic['col']=col
               if rules[i]=='random_float':
                   dic['rule']='random_float'
                   if chars_list[i]=='unique':
                       dic['unique']='True'
                   else:
                       dic['unique']='False'
                   
               elif rules[i]=='random_int':
                   dic['rule']='random_int'
                   if chars_list[i]=='unique':
                       dic['unique']='True'
                   else:
                       dic['unique']='False'
                   
               else:
                   if rules[i]=="date_mask":
                       dic['rule']="date_mask"
                       dic["input"]=format_list[date_index]
                       dic["output"]=format_list[date_index+1]
                       date_index+=2
                   else:                
                       if rules[i] == 'encrypt':
                           dic['rule']="vlife_encrypt"
                       elif rules[i]=='translate':    
                           dic['rule']="vlife_translate"
               dic_list.append(dic)
               
               
               config_dict['MASKING_RULE']=dic_list
               insert_dict_folder(config_dict,'data/config/', access_key_id, secret_access_key, region_name, accountId,[] , bucket,f'{file}{macro["config"]}')               
               
