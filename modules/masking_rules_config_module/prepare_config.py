from modules.macro import macro
import pandas as pd
from modules.helper import find_files


def prepare_config(file,Macie_List,default,custom): 
    
    print("**********::"+file+"::******************")
    
    config_dict={}
    config_dict["ENCRYPTED_MAP_ALPH_NUM"]=macro["ENCRYPTED_MAP_ALPH_NUM"]
    config_dict["MAP_ENCRYPT_KEY"]=macro["MAP_ENCRYPT_KEY"]
    
    try:
        df=pd.read_csv(find_files.find_files(f'{file}.csv','C:'))
    except:
        print(f"data file {file} not found !")
    total_cols=list(df.columns)
    pii_cols_list_index,rules,chars_list,format_list=cols_check_box.open_check_box(Macie_List,total_cols,file,default,custom)
    
    
    if pii_cols_list_index!=-1:
        format_list=list(filter(None, format_list))
        dic_list=[]
        date_index=0
        for i in range(len(total_cols)):
            #if pii_cols_list_index[i]==1:
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
               create_dir('data/config')
               f=open('data/config/'+file+macro['config'],'w')
               json.dump(config_dict,f,indent=4)
               f.close()
