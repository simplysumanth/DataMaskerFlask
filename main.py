from os import read
from botocore.retries import bucket
from flask import Flask, render_template,request, redirect, flash
from flask.helpers import url_for
from flask_bootstrap import Bootstrap5
from modules.masking_rules_config_module.column_selector import read_data_file, store_config_json
from modules.helper import contents_in_s3, get_buckets, get_df
from modules.macro import macro
from modules.pii_col_gen_module.macie_pii_identifier import main
import ast
import pandas as pd

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
      columns, cols_list = read_data_file(macie_list = [],cols_list = [],file = path ,default = [],custom = [],df = -1)
   
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


   if request.method == "POST":
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
   else:
      return render_template("masking_rules_config_generator/config_generator.html", cols = columns, enumerate = enumerate)


@app.route('/columnGen')
def columns_gen():
   return render_template("pii_column_generator/columns_gen.html")

if __name__ == '__main__':
   app.run(debug=True)
