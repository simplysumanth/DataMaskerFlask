from flask import Flask, render_template
from flask_bootstrap import Bootstrap5

app = Flask(__name__)
bootstrap = Bootstrap5(app)

@app.route('/')
@app.route('/home')
def hello_world():
   return render_template('home.html')

@app.route('/maskingDataGen')
def masking_data_gen():
   return render_template("masking_data_generator/data_gen.html")

@app.route('/maskingRulesGen')
def masking_rules_gen():
   return render_template("masking_rules_config_generator/rules_gen.html")

@app.route('/columnGen')
def masking_columns_gen():
   return render_template("pii_column_generator/columns_gen.html")

if __name__ == '__main__':
   app.run(debug=True)
