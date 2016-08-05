#! usr/bin/env/python
# -*- coding: utf8 -*-
__author__ = "james"

from ebaysdk.trading import Connection
import MySQLdb
import multiprocessing
import datetime

item_queue = multiprocessing.Queue()

def selectItemFromDB():
	query = "select itemid from category_items where datediff(now(),starttime)<70"
	try:
		con = MySQLdb.Connection(host ='192.168.0.134',user='root',passwd='',db='ebaydata')
		cur = con.cursor()
		cur.execute(query)
		for item in cur.fetchall():
			 item_queue.put(item[0])
		con.close()
		print "Data have benn putted into the QUEUE"

	except Exception as e:
		print e

def input_item_details(details_dict):
	insert_query = "insert into item_details (itemid,userid,quantitysold,starttime,currentprice,shippingcost,location,hitcount,categoryname,created_date) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
	check_query = "select itemid from item_details where itemid=%s"
	update_query = "update item_details set deltasold=%s-quantitysold,quantitysold=%s where itemid=%s"
	try:
		con = MySQLdb.Connection(host="192.168.0.134",user="root",passwd="",db="ebaydata")
		cur = con.cursor()
		con.set_character_set('utf8')
		cur.execute('SET NAMES utf8;')
		cur.execute('SET CHARACTER SET utf8;')
		cur.execute('SET character_set_connection=utf8;')
		if cur.execute(check_query,(details_dict['itemid'],)):
			cur.execute(update_query,(details_dict['quantitysold'],details_dict['quantitysold'],details_dict['itemid']))
			print "%s: upadting %s" % (datetime.datetime.now(),details_dict["itemid"])
			con.commit()
		else: 
			cur.execute(insert_query,(
				details_dict['itemid'],details_dict['userid'],
				details_dict['quantitysold'],details_dict['starttime'],
				details_dict['currentprice'],details_dict['shippingcost'],
				details_dict['location'],details_dict['hitcount'],
				details_dict['categoryname'],datetime.datetime.now()
					)
				)
			con.commit()
			print "%s: putting %s" % (datetime.datetime.now(),details_dict['itemid'])
		con.close()
	except Exception as e:
		print e



def get_item(item):
	api = Connection(config_file='ebay.yaml')
	details_dict = dict()
	try: 
		response = api.execute('GetItem',{"ItemID":item})
		details = response.reply.Item
		details_dict['itemid'] = item
		details_dict['userid'] =details.Seller.UserID
		details_dict['quantitysold'] =details.SellingStatus.QuantitySold
		details_dict['starttime'] = details.ListingDetails.StartTime
		details_dict['currentprice'] =details.SellingStatus.CurrentPrice.value
		try:
			details_dict['shippingcost'] = details.ShippingDetails.ShippingServiceOptions[0].ShippingServiceCost.value
		except: details_dict['shippingcost'] = 0
		details_dict['location'] = details.Location
		details_dict['hitcount'] =0
		details_dict['categoryname'] =details.PrimaryCategory.CategoryName
	except Exception as e:
		print e

	return details_dict


def test_get_item(item):
	api = Connection(config_file='ebay.yaml')
	details_dict = dict()
	try: 
		response = api.execute('GetItem',{"ItemID":item,"OutputSelector":"Seller","DetailLevel":"ReturnAll"})
		details = response.reply.Item
		print details
	except Exception as e:
		print e

def test_get_user(item):
	api = Connection(config_file='ebay.yaml')
	details_dict = dict()
	try: 
		response = api.execute('GetUser',{"UserID":item,"DetailLevel":"ReturnAll"})
		details = response.reply
		print details
	except Exception as e:
		print e


def handle():
	while True:
		try:
			item = item_queue.get()
			details = get_item(item)
			input_item_details(details)
		except Exception as e:
			print e


def main():
	selectItemFromDB()
	handle_list = list()	
	for i in range(60):
		handle_pro = multiprocessing.Process(target = handle, args=())
		handle_list.append(handle_pro)
	for pro in handle_list:
		pro.start()
		print "get_item proccess is starting...."
	for pro in handle_list:
		pro.join()
		print "get_item process is done!!!!!!"
if __name__  == "__main__":
	# main()
	test_get_user("onepiece2u")
