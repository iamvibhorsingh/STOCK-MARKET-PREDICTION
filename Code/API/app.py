#!flask/bin/python
from flask import Flask,jsonify
from flask import make_response
from flask import request
from prediction import get_prediction,get_prediction_on_open 
import os
os.environ['JAVA_HOME'] = "C:\progra~2\Java\jdk1.8.0_231"
os.environ["PYSPARK_SUBMIT_ARGS"] = "--master local[2] pyspark-shell"


app = Flask(__name__)

@app.route('/predict')
def index():
	openp = request.args.get('open')
	return jsonify({'task': get_prediction_on_open(openp)}), 200

if __name__ == '__main__':
    app.run(debug=True)

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)
