from os import read
import json
from botocore.retries import bucket
from flask import Flask, render_template,request, redirect, flash
from flask.helpers import url_for
from flask_bootstrap import Bootstrap5
from modules.masking_rules_config_module.column_selector import read_data_file, store_config_json
from modules.helper import contents_in_s3, get_buckets, get_df, check_override
from modules.macro import macro
from modules.pii_col_gen_module.macie_pii_identifier import main
import ast
import pandas as pd
import pprint

app = Flask(__name__)
bootstrap = Bootstrap5(app)

@app.route('/', methods=['POST', 'GET'])
@app.route('/home',methods=['POST', 'GET'])
def home():
   return render_template('home.html')

@app.route('/piicolumnGen')
def pii_column_gen():
   return render_template("pii_column_generator/columns_gen.html")


@app.route('/piicolumnGen/maciepiiIdentifier', methods=["GET","POST"])
def pii_macie_identifier():
   if request.method == "POST":
      return main(request.form['access_key_id'], request.form['secret_access_key'], request.form['region_name'], request.form['accountId'], request.form['jobName'], request.form['s3BucketName'])


@app.route('/maskingDataGen')
def data_gen():
   return render_template("masking_data_generator/data_gen.html")


@app.route('/maskingRulesGen/SelectType')
def rules_gen_select_type():
   return render_template("masking_rules_config_generator/select_type.html")

@app.route('/maskingRulesGen/SelectFile', methods =["GET", "POST"])
def rules_gen_select_file():
   if request.method == "POST":
      csvFileData = request.form['csvFileDetails']
      path = f"data/{csvFileData}"
      file_name = csvFileData[0:-4]
      #False
      return redirect(url_for('rules_gen_configGenerator', path = path))
   else:
      return render_template("masking_rules_config_generator/select_file.html")

@app.route('/maskingRulesGen/SelectFromS3-1', methods =["GET", "POST"])
def rules_gen_select_from_s3_1():
   if request.method == "POST":
      access_key_id = request.form['access_key_id']
      secret_access_key = request.form['secret_access_key']
      region_name = request.form['region_name']
      account_id = request.form['account_id']

      buckets=get_buckets(access_key_id, secret_access_key, region_name, account_id, "", "", "")
      print(buckets)
      bucket_details = {}
      bucket_details["access_key_id"] = access_key_id
      bucket_details["secret_access_key"] = secret_access_key
      bucket_details['region_name'] = region_name
      bucket_details['account_id'] = account_id
      bucket_details['buckets'] = buckets
      return redirect(url_for("rules_gen_select_from_s3_2" ,bucket_details = bucket_details))

   else:
     return render_template("masking_rules_config_generator/select_s3_config_1.html") 

@app.route('/maskingRulesGen/SelectFromS3-2', methods =["GET", "POST"])
def rules_gen_select_from_s3_2():
   bucket_details= ast.literal_eval(request.args.get('bucket_details'))
   buckets = bucket_details['buckets']
   if request.method == "POST":
      bucket = request.form['bucket_select']
      files = contents_in_s3(bucket_details["access_key_id"], bucket_details["secret_access_key"], bucket_details['region_name'], bucket_details['account_id'], "",bucket, "")
      print(files)
      return redirect(url_for('rules_gen_select_from_s3_3', bucket = bucket, files = files, bucket_details = bucket_details))
   else:
      return render_template("masking_rules_config_generator/select_s3_config_2.html", buckets = buckets)
      
@app.route('/maskingRulesGen/SelectFromS3-3', methods =["GET", "POST"])
def rules_gen_select_from_s3_3():
   bucket_details= ast.literal_eval(request.args.get('bucket_details'))

   buckets = bucket_details['buckets']

   bucket = request.args.get('bucket')

   files_list = request.args.getlist('files')
   
   if request.method == "POST":
      file = request.form['file_select']
      print(file)
      df = get_df(bucket_details["access_key_id"], bucket_details["secret_access_key"], bucket_details['region_name'], bucket_details['account_id'], "",bucket, file)
      df_dict = df.to_dict()
      file_name = file.split("/")[-1][0:-4]
      print(file_name)
      return redirect(url_for('rules_gen_configGenerator', df = df_dict, file_name = file_name))
   else:
      return render_template("masking_rules_config_generator/select_s3_config_3.html", files = files_list)


@app.route('/maskingRulesGen/configGenerator', methods =["GET", "POST"])
def rules_gen_configGenerator():
   
   if request.args.get('path'):
      path = request.args.get('path')
      file_name = (path.split('/')[-1]).split('.')[0]
      print(file_name)
      columns, cols_list = read_data_file(macie_list = [],cols_list = [],file = path ,default = [],custom = [],df = -1)
      override = False
      override = check_override(file_name,[],[],[],-1)

   elif request.args.get('df'):
      df = request.args.get('df')
      print(type(df))
      df = ast.literal_eval(df)
      print(type(df))
      df = pd.DataFrame.from_dict(df)
      print(df)
      print(type(df))
      file_name = request.args.get('file_name')
      columns, cols_list = read_data_file(macie_list = [],cols_list = [],file = file_name ,default = [],custom = [],df = df)
      override = False
      override = check_override(file_name,[],[],[],df)

   if request.method == "POST":
      if request.form['button'] == 'generate':
         full_masking_req = []
         dic = {}
         n_columns = len(cols_list)
         for i in range(n_columns):
            column_mask_req = {}
            try:
               column_mask_req["col"] = request.form[f'check_{i}']
               if request.form[f'masking_rules_{i}'] == "translate" or request.form[f'masking_rules_{i}'] == "encrypt":
                  column_mask_req["rule"] = str("vlife_") + request.form[f'masking_rules_{i}']
               else:
                  column_mask_req["rule"] = request.form[f'masking_rules_{i}']
               try:
                  if request.form[f'attribute_{i}'] == 'unique':
                     column_mask_req["unique"] = True
                  elif request.form[f'attribute_{i}'] == 'not unique':
                     column_mask_req["unique"] = False
                  else:
                     column_mask_req['attribute'] = request.form[f'attribute_{i}']
               except:
                  pass
               try:
                  column_mask_req["input"] = request.form[f'input_format_{i}']
                  column_mask_req["output"] = request.form[f'output_format_{i}']
               except:
                  pass
            except:
               pass
            if column_mask_req != {}:
               full_masking_req.append(column_mask_req)

         selected_cols = []
         for i in full_masking_req:
            selected_cols.append(i['col'])

         for i in cols_list:
            if i not in selected_cols:
               full_masking_req.append({'col': i})

         
         dic["ENCRYPTED_MAP_ALPH_NUM"] = macro["ENCRYPTED_MAP_ALPH_NUM"]
         dic["MAP_ENCRYPT_KEY"] = macro["MAP_ENCRYPT_KEY"]
         dic['MASKING_RULE'] = full_masking_req
         

         store_config_json(dic, file_name )
         return redirect(url_for('home'))

      elif request.form['button'] == 'cancel':
         return redirect(url_for('home'))
   else:
      return render_template("masking_rules_config_generator/config_generator.html", cols = columns, enumerate = enumerate, override = override)

@app.route('/maskingRulesGen/SelectFromMacie-1', methods =["GET", "POST"])
def rules_gen_select_from_macie_1():
   if request.method == 'POST':
      macie_data_file = request.form['macieDataFile']
      macie_data_file = f"data/{macie_data_file}"
      print(macie_data_file)
      return redirect(url_for('rules_gen_select_from_macie_2',  macie_data_file = macie_data_file))
   return render_template('masking_rules_config_generator/select_macie_config_1.html') 

@app.route('/maskingRulesGen/SelectFromMacie-2', methods =["GET", "POST"])
def rules_gen_select_from_macie_2():
   macie_data_file = request.args.get('macie_data_file')
   
   data = json.load(open(macie_data_file, 'r'))
   
   files=[]
   for file_index in range(len(data)):
      files.append(data[file_index]["resourcesAffected"]["s3Object"]["key"])

   if request.method == 'POST':
      macie_config_file = request.form['macie_file_select']
      print(macie_config_file)
      return redirect(url_for('rules_gen_configGenerator_for_macie',files = files, macie_data_file = macie_data_file,  macie_config_file = macie_config_file))
   return render_template('masking_rules_config_generator/select_macie_config_2.html',files = files)


@app.route('/maskingRulesGen/configGeneratorForMacie', methods =["GET", "POST"])
def rules_gen_configGenerator_for_macie():
   files = request.args.getlist('files')

   macie_data_file = request.args.get('macie_data_file')
   
   data = json.load(open(macie_data_file, 'r'))

   file = request.args.get('macie_config_file')

   # #print(files)
   print("--------------------------------------------------------")
   pp = pprint.PrettyPrinter(width=41, compact=True)
   pp.pprint(data)
   # print("-----------------------------------------------------")
   # print(file)

   print("FIle in macie-gen", file)
   print("FIles in macie-gen", files)
   file_index=files.index(file)

   print("FIle index in macie-gen", file_index)
   macie_list=[]
   default=[]
   custom=[]
   try:
         #default identifiers
         default_sensitive_data=data[file_index]['classificationDetails']["result"]["sensitiveData"][0]['detections']
         for i in range(len(default_sensitive_data)):
            macie_list.append(default_sensitive_data[i]['occurrences']['cells'][0]['columnName'])
            default.append(default_sensitive_data[i]['occurrences']['cells'][0]['columnName'])
   except:
         pass
   
   #custom identifiers
   print("---------------------------------------------------------------------------------------")
   if data[file_index]['classificationDetails']["result"]["customDataIdentifiers"]["totalCount"]>0:
         custom_sensitive_data=data[file_index]['classificationDetails']["result"]["customDataIdentifiers"]['detections']
         for i in range(len(custom_sensitive_data)):
            cols_list=[]
            for j in range(len(custom_sensitive_data[i]['occurrences']['cells'])):
               cols_list.append(custom_sensitive_data[i]['occurrences']['cells'][j]['columnName'])
            set1=set(cols_list)
            for col in set1:
               macie_list.append(col)
               custom.append(col)
   
   print("Mcie list : ", macie_list)
   print("Custom: ",custom)
   print("Default:", default)

   file=data[file_index]["resourcesAffected"]["s3Object"]["key"]
   file=file[0:-4]
   path=(data[file_index]["resourcesAffected"]["s3Object"]["path"]).split('/')
   bucket=path[0]
   path='/'.join(path[1::])

   print("Final File:", file)
   print("Final Path:", path)

   access_key_id='AKIAYV2B3AOU3RCKJPVT'
   secret_access_key='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'
   region_name='us-east-1'
   accountId='596601668521'
   name=''
   s3=[]
   #bucket='datamasking123'

   #print(contents_in_path(path,access_key_id, secret_access_key, region_name, accountId, s3,bucket,file))
   print("Final Bucket: ", bucket)
   df=get_df(access_key_id, secret_access_key, region_name, accountId, s3, bucket,path)
   
   columns, cols_list = read_data_file(macie_list = macie_list,cols_list = [],file = path ,default = default,custom = custom,df = df)
   print("COl lost :",cols_list)
   print("Columns: ", columns)

   override = False
   override = check_override(file,[],[],[],df)

   if request.method == "POST":
      if request.form['button'] == 'generate':
         full_masking_req = []
         dic = {}
         n_columns = len(cols_list)
         for i in range(n_columns):
            column_mask_req = {}
            try:
               column_mask_req["col"] = request.form[f'check_{i}']
               if request.form[f'masking_rules_{i}'] == "translate" or request.form[f'masking_rules_{i}'] == "encrypt":
                  column_mask_req["rule"] = str("vlife_") + request.form[f'masking_rules_{i}']
               else:
                  column_mask_req["rule"] = request.form[f'masking_rules_{i}']
               try:
                  if request.form[f'attribute_{i}'] == 'unique':
                     column_mask_req["unique"] = True
                  elif request.form[f'attribute_{i}'] == 'not unique':
                     column_mask_req["unique"] = False
                  else:
                     column_mask_req['attribute'] = request.form[f'attribute_{i}']
               except:
                  pass
               try:
                  column_mask_req["input"] = request.form[f'input_format_{i}']
                  column_mask_req["output"] = request.form[f'output_format_{i}']
               except:
                  pass
            except:
               pass
            if column_mask_req != {}:
               full_masking_req.append(column_mask_req)

         selected_cols = []
         for i in full_masking_req:
            selected_cols.append(i['col'])

         for i in cols_list:
            if i not in selected_cols:
               full_masking_req.append({'col': i})

         
         dic["ENCRYPTED_MAP_ALPH_NUM"] = macro["ENCRYPTED_MAP_ALPH_NUM"]
         dic["MAP_ENCRYPT_KEY"] = macro["MAP_ENCRYPT_KEY"]
         dic['MASKING_RULE'] = full_masking_req
         

         store_config_json(dic, file )
         return redirect(url_for('home'))
      
      elif request.form['button'] == 'cancel':
         return redirect(url_for('home'))
   
   else:
      return render_template('masking_rules_config_generator/config_generator_for_macie.html',override = override, macie_list = macie_list, custom = custom, default=default, cols = columns, enumerate = enumerate)

if __name__ == '__main__':
   app.run(debug=True)
