from os import read
from flask import Flask, render_template,request, redirect, flash
from flask.helpers import url_for
from flask_bootstrap import Bootstrap5
from modules.masking_rules_config_module.column_selector import read_data_file

app = Flask(__name__)
bootstrap = Bootstrap5(app)

@app.route('/')
@app.route('/home')
def home():
   return render_template('home.html')

@app.route('/maskingDataGen')
def data_gen():
   return render_template("masking_data_generator/data_gen.html")

@app.route('/maskingRulesGen/SelectType')
def rules_gen_select_type():
   return render_template("masking_rules_config_generator/select_type.html")

@app.route('/maskingRulesGe3e', methods =["GET", "POST"])
def rules_gen_select_file():
   if request.method == "POST":
      csvFileData = request.form['csvFileDetails']
      print(csvFileData)
      path = f"data/{csvFileData}"
      print(path)
      return redirect(url_for('rules_gen_configGenerator', path = path))
   else:
      return render_template("masking_rules_config_generator/select_file.html")

@app.route('/maskingRulesGen/configGenerator', methods =["GET", "POST"])
def rules_gen_configGenerator():
   if request.method == "POST":
      columns = 9
      full_masking_req = []
      for i in range(columns):
         column_mask_req = {}
         try:
            column_mask_req["checked"] = request.form[f'check_{i}']
            column_mask_req["masking_rule"] = request.form[f'masking_rules_{i}']
            try:
               column_mask_req["attribute"] = request.form[f'attribute_{i}']
            except:
               pass
            try:
               column_mask_req["attribute"] = [request.form[f'input_format_{i}'], request.form[f'output_format_{i}']]
            except:
               pass
         except:
            pass
         if column_mask_req != {}:
            full_masking_req.append(column_mask_req)
         
      print(full_masking_req)
      return redirect(url_for('rules_gen_configGenerator'))
   else:
      path = request.args.get('path')
      print(path)
      columns = read_data_file(path)
      return render_template("masking_rules_config_generator/config_generator.html", cols = columns, enumerate = enumerate)


@app.route('/columnGen')
def columns_gen():
   return render_template("pii_column_generator/columns_gen.html")

if __name__ == '__main__':
   app.run(debug=True)
