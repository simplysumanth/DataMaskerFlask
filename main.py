from os import read
from flask import Flask, render_template
from flask_bootstrap import Bootstrap5
from modules.masking_rules_config_module.column_selector import read_file

app = Flask(__name__)
bootstrap = Bootstrap5(app)

@app.route('/')
@app.route('/home')
def home():
   return render_template('home.html')

@app.route('/maskingDataGen')
def data_gen():
   return render_template("masking_data_generator/data_gen.html")

@app.route('/maskingRulesGen/Selection')
def rules_gen_selection():
   return render_template("masking_rules_config_generator/selection.html")

@app.route('/maskingRulesGen/configGenerator')
def rules_gen_configGenerator():
   columns = read_file()
   return render_template("masking_rules_config_generator/config_generator.html", cols = columns)


@app.route('/columnGen')
def columns_gen():
   return render_template("pii_column_generator/columns_gen.html")

if __name__ == '__main__':
   app.run(debug=True)
