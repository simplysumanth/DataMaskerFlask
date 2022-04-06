from flask import Flask, render_template
from flask_bootstrap import Bootstrap5
import pandas as pd

app = Flask(__name__)
bootstrap = Bootstrap5(app)

@app.route('/')
@app.route('/home')
def home():
   return render_template('home.html')

@app.route('/maskingDataGen')
def masking_data_gen():
   return render_template("masking_data_generator/data_gen.html")

@app.route('/maskingRulesGen')
def masking_rules_gen():
   df = pd.read_csv("maskdata.csv")
   return render_template("masking_rules_config_generator/rules_gen.html", df = df)

@app.route('/columnGen')
def masking_columns_gen():
   return render_template("pii_column_generator/columns_gen.html")

if __name__ == '__main__':
   app.run(debug=True)
