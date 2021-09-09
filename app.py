#!/usr/bin/env python

import os
import pymongo
from flask import Flask, abort, request, jsonify, g, url_for
from pymongo import MongoClient
from pymongo import ASCENDING, DESCENDING
from bson import ObjectId
from math import sin, cos, sqrt, atan2, radians
import datetime
import json

app = Flask(__name__)


client=MongoClient("mongodb://usrname:pwd@host:port/db")

db = client.randr_poc
ven_collection = db.vendor
#ven_collection.drop()
ven_collection.create_index([('mobileno', pymongo.ASCENDING)], unique=True)

ven_book_collection = db.ven_book
#ven_book_collection.drop()
ven_book_collection.create_index([('mobileno', pymongo.ASCENDING)], unique=True)

cust_collection = db.customer
cust_collection.create_index([('mobileno', pymongo.ASCENDING)], unique=True)

cust_book_collection = db.cus_book
cust_book_collection.create_index([('mobileno', pymongo.ASCENDING)], unique=True)


@app.route('/api/create_vendor', methods=['POST'])
def create_vendor():
		vendor_list = request.get_json()
		
		id_check=ven_collection.find( {'mobileno': vendor_list['mobileno']}).limit(1)
		response = []
		
		for document in id_check:
		  document['_id'] = str(document['_id'])
		  response.append(document['name'])
		  	
		if not response:
		   ven_collection.insert_one(vendor_list).inserted_id
		   my_dict={"mobileno":vendor_list['mobileno'],"bookings":0}
		   ven_book_collection.insert_one(my_dict).inserted_id
		   return ('', 200)
		else:
		   #my_dict={"mobileno":vendor_list['mobileno'],"bookings":0}
		   #ven_book_collection.insert_one(my_dict).inserted_id
		   return ('Existing User')

		#return jsonify({'vendor': vendor_list})


@app.route('/api/getvendor', methods=['GET'])
def get_vendor():
    all_tasks = ven_collection.find()
    response = []
    for document in all_tasks:
        document['_id'] = str(document['_id'])
        response.append(document)
    return json.dumps(response)
	
@app.route('/api/getvendor_bookings', methods=['GET'])
def get_vendor_book():
    all_tasks = ven_book_collection.find()
    response = []
    for document in all_tasks:
        document['_id'] = str(document['_id'])
        response.append(document)
    return json.dumps(response)
	
@app.route('/api/getvendor_bookings/<mobileno>', methods=['GET'])
def get_ven_book_byid(mobileno):
		all_tasks = ven_book_collection.find()
		response = []
		for document in all_tasks:
			document['mobileno'] = str(document['mobileno'])
			response.append(document['bookings'])
		return jsonify({'bookings': response})

@app.route('/api/getcust_bookings', methods=['GET'])
def get_cust_book():
    all_tasks = cust_book_collection.find()
    response = []
    for document in all_tasks:
        document['_id'] = str(document['_id'])
        response.append(document)
    return json.dumps(response)	
	
@app.route('/api/getcust_bookings/<mobileno>', methods=['GET'])
def get_cust_book_byid(mobileno):
		all_tasks = cust_book_collection.find()
		response = []
		for document in all_tasks:
			document['mobileno'] = str(document['mobileno'])
			response.append(document['bookings'])
		return jsonify({'bookings': response})

@app.route('/api/get_nearby_ven',methods=['GET'])
def get_vendor_dis():
		R = 6373.0
		cust_loc_list=request.get_json()
			
		id_check=ven_collection.find()
		response = []
		distance=[]
		vendor_dist_list=[]
		final_list=[]
		
		for document in id_check:
				document['_id'] = str(document['_id'])
				another_list={'name':document['name'],'mobileno':document['mobileno'],'latitude':document['latitude'],'longitude':document['longitude'],'items':document['items']}
				response.append(another_list)
		
		for i in range(len(response)):
			
			lat2 = radians(abs(cust_loc_list['latitude']))
			lon2 = radians(abs(cust_loc_list['longitude']))
			lat1=  radians(abs(response[i]['latitude']))
			lon1=  radians(abs(response[i]['longitude']))
			dlon = lon2 - lon1
			dlat = lat2 - lat1
			a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
			c = 2 * atan2(sqrt(a), sqrt(1 - a))
			
			vendor_dist_list={'name':response[i]['name'],'mobileno':response[i]['mobileno'] ,'distance':R*c,'items':response[i]['items']}
			distance.append(vendor_dist_list)

		for j in range(len(distance)):
			print(distance[j])
			if distance[j]['distance'] <= 1:
				final_list.append(distance[j])
				#final_list.append(temp_list) 
		return json.dumps(final_list)

@app.route('/api/update_items', methods=['POST'])
def update_items():
		item_list = request.get_json()
		upd_items=item_list['items']
		id_check=ven_collection.find( {'mobileno': item_list['mobileno']}).limit(1)
		response = []
		
		for document in id_check:
		  document['_id'] = str(document['_id'])
		  ven_collection.find_one_and_update({"_id" : ObjectId(document['_id'])},{"$set":  {"items" : upd_items}},upsert=True)
		  
		return ('', 200)
		
@app.route('/api/update_ven_loc', methods=['POST'])
def update_ven_loc():
		loc_list = request.get_json()
		latitude=loc_list['latitude']
		longitude=loc_list['longitude']
		id_check=ven_collection.find( {'mobileno': loc_list['mobileno']}).limit(1)
		
		
		for document in id_check:
		  document['_id'] = str(document['_id'])
		  ven_collection.find_one_and_update({"_id" : ObjectId(document['_id'])},{"$set":  {"latitude" : latitude,"longitude":longitude}},upsert=True)
		  
		return ('', 200)
		
@app.route('/api/update_ven_slot', methods=['POST'])
def update_ven_slot():
		slot_list = request.get_json()
		startEpooch=slot_list['startEpooch']
		endEpooch=slot_list['endEpooch']
		id_check=ven_collection.find( {'mobileno': slot_list['mobileno']}).limit(1)
		startIST=datetime.datetime.fromtimestamp(int(startEpooch)).strftime('%Y-%m-%d %H:%M:%S')
		endIST=datetime.datetime.fromtimestamp(int(endEpooch)).strftime('%Y-%m-%d %H:%M:%S')
		
		for document in id_check:
		  document['_id'] = str(document['_id'])
		  ven_collection.find_one_and_update({"_id" : ObjectId(document['_id'])},{"$set":  {"startEpooch" : startIST,"endEpooch":endIST}},upsert=True)
		  
		return ('', 200)


@app.route('/api/book_order',methods=['POST'])
def book_order():

		item_list = request.get_json()
		upd_items=item_list['items']
		ven_id_check=ven_book_collection.find( {'mobileno': item_list['ven_mobileno']}).limit(1)
		#cus_id_check=cust_book_collection.find( {'mobileno': item_list['cust_mobileno']}).limit(1)
		startIST=datetime.datetime.fromtimestamp(int(item_list['startEpooch'])).strftime('%Y-%m-%d %H:%M:%S')
		endIST=datetime.datetime.fromtimestamp(int(item_list['endEpooch'])).strftime('%Y-%m-%d %H:%M:%S')
		
		for document in ven_id_check:
		  document['_id'] = str(document['_id'])
 
		  ven_book_collection.update({"_id" : ObjectId(document["_id"])},{"$inc" : {"bookings" : 1}},upsert=True)
		  ven_book_collection.update({"_id" : ObjectId(document["_id"])},{"$set": {"startTime" :startIST ,"endTime":endIST}},upsert=True)
		  
		return ('', 200)
		
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
		
@app.route('/', methods=['GET'])
def home():
    return jsonify({'msg': 'This is the Home'})


@app.route('/test', methods=['GET'])
def test():
    return jsonify({'msg': 'This is a Test'})


if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True)