#!/usr/bin/env python

import os
import pymongo
from flask import Flask, abort, request, jsonify, g, url_for
from pymongo import MongoClient
from pymongo import ASCENDING, DESCENDING
from bson import ObjectId

import json

app = Flask(__name__)


client=MongoClient("mongodb://usrname:pwd@host:port/db")

db = client.randr_poc

cust_collection = db.customer
cust_collection.create_index([('mobileno', pymongo.ASCENDING)], unique=True)



@app.route('/api/create_customer', methods=['POST'])
def create_customer():
		customer_list = request.get_json()
		
		id_check=cust_collection.find( {'mobileno': customer_list['mobileno']}).limit(1)
		response = []
		
		for document in id_check:
		  document['_id'] = str(document['_id'])
		  response.append(document['name'])
		  	
		if not response:
		   cust_collection.insert_one(customer_list).inserted_id
		   return ('', 204)
		else:
		   return ('Existing User')


@app.route('/api/getcustomer', methods=['GET'])
def get_customer():
    all_tasks = cust_collection.find()
    response = []
    for document in all_tasks:
        document['_id'] = str(document['_id'])
        response.append(document)
    return json.dumps(response)
	
		
@app.route('/api/update_cust_loc', methods=['POST'])
def update_cust_loc():
		loc_list = request.get_json()
		latitude=loc_list['latitude']
		longitude=loc_list['longitude']
		id_check=cust_collection.find( {'mobileno': loc_list['mobileno']}).limit(1)
		
		
		for document in id_check:
		  document['_id'] = str(document['_id'])
		  cust_collection.find_one_and_update({"_id" : ObjectId(document['_id'])},{"$set":  {"latitude" : latitude,"longitude":longitude}},upsert=True)
		  
		return ('', 204)

if __name__ == '__main__':
    app.run(debug=True)