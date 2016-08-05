# -*-coding:utf-8-*-
__author__ = 'James Chow'
'''get the items in given category using ebaysdk.
 Finally,I can know a lot about the category.
 I can know the top 100*100 listings.
 I can know the changes of these listings.
 it is amazing.
'''
from ebaysdk.finding import Connection
import uuid
from requests import Request
from ebaysdk import UserAgent
import MySQLdb
import datetime
from gevent import monkey; monkey.patch_socket()
import gevent
import multiprocessing
kw_page_queue = multiprocessing.Queue()

class MyConnection(Connection):
    """rewrite the header so that I can get items of other ebay site"""
    def build_request(self, verb, data, verb_attrs, files=None):
        self.verb = verb
        self._request_dict = data
        self._request_id = uuid.uuid4()
        url = self.build_request_url(verb)
        headers = self.build_request_headers(verb)
        headers.update({'User-Agent': UserAgent,
                        'X-EBAY-SDK-REQUEST-ID': str(self._request_id),
                        'X-EBAY-SOA-GLOBAL-ID':'EBAY-US'})
        # EBAY-GB

        # if we are adding files, we ensure there is no Content-Type header already defined
        # otherwise Request will use the existing one which is likely not to be multipart/form-data
        # data must also be a dict so we make it so if needed

        requestData = self.build_request_data(verb, data, verb_attrs)
        if files:
            del(headers['Content-Type'])
            if isinstance(requestData, basestring):
                requestData = {'XMLPayload':requestData}

        request = Request(self.method,
            url,
            data=requestData,
            headers=headers,
            files=files,
        )
        self.request = request.prepare()

def findByCategoey():
    '''get the 100*100 listings sorted in Bestmatch order in Cagtegory 63862'''
    api=Connection(config_file='D:\ebay_cofig\ebay.yaml')
    response=api.execute('findItemsByCategory',{'categoryId':'63862','sortOrder':'BestMatch'})
    print response.reply
    # for i in response.reply.searchResult.item:
    #     print i


def findIneBayStores():
    '''get the 100*100 listings sorted in Newlisted order in ebay store'''
    api=Connection(config_file='E:\ebaycofig\ebay.yaml')
    response=api.execute('findItemsIneBayStores',{'storeName':'yooocart','paginationInput':{'pageNumber':100},'sortOrder':'StartTimeNewest'})
    print response.reply


def findBykeywords():
    '''get the 100*100 listings using keywords and sort it with Bestmatch and filter it
    with listingtype'''
    api=MyConnection(config_file='D:\ebay_cofig\ebay.yaml')
    response=api.execute('findItemsByKeywords',{'keywords':'iphone 6 case cover',
                                                'sortOrder':'BestMatch',
                                                'paginationInput':{'pageNumber':1},
                                                'itemFilter':[
                                                    #{'name':'StartTimeFrom', 'value':'20015-08-04T19:09:02.768Z'},{'name':'StartTimeTo','value':'2015-10-10T19:09:02.768Z'},
                                                    {'name':'ListingType','value':'FixedPrice'},]
                                                })
    print response.reply


def find_advanced(key_words, page, owner):
    "get the 100*100 listings in Category 20349 and limited using keywords"

    api = Connection(config_file='ebay.yaml')
    input_para = {
        # 'categoryId': category_id,
        'keywords': key_words,
        'sortOrder': 'BestMatch',
        'outputSelector': 'SellerInfo',
        'paginationInput': {'pageNumber': 0},
        'itemFilter': [
            {'name': 'ListingType', 'value': 'FixedPrice'},
            {'name': 'AvailableTo', 'value': 'US'}
        ]
    }
    input_para['paginationInput']['pageNumber'] = page
    response = api.execute('findItemsAdvanced', input_para)
    try:
        items = response.reply.searchResult.item
        for item in items:
            input_single(item.itemId,item.listingInfo.startTime,owner)
    except Exception as e:
        print e


def get_kw(owner):
    try:
        con = MySQLdb.Connection(host='192.168.0.134',user='root', passwd='',db='ebaydata' )
        cur = con.cursor()
        query = "select categoryId,key_words from category_kw_dict where owner='%s'" % owner
        print query
        cur.execute(query)
        id_kw = cur.fetchall()
        for single in id_kw:
            for page in range(1, 101):
                single_with_page = list(single)
                single_with_page.append(page)
                kw_page_queue.put(single_with_page)
                # yield single_with_page
        cur.close()
        con.close()
    except Exception as e:
        print e


def input_item(item_list,owner):
    try:
        con = MySQLdb.Connection(host='192.168.0.134', user='root', passwd='', db='ebaydata')
        cur = con.cursor()
        check_query = "select * from " + owner + "_category_items where itemid=%s"
        if item_list:
            for item in item_list:
                if not cur.execute(check_query, (item,)):
                    query = "insert into " + owner + "_category_items values (%s,%s,now())"
                    cur.execute(query, (item,))
                    con.commit()
                    print "%s：get item %s" % (datetime.datetime.now(), item)
        con.close()
    except Exception as e:
        print e


def input_single(item,starttime,owner):
	try:
		con = MySQLdb.Connection(host='192.168.0.134', user='root', passwd='', db='ebaydata')
		cur = con.cursor()
		check_query = "select * from " + owner + "_category_items where itemid=%s"
		if item:
			if not cur.execute(check_query,(item,)):
				query = "insert into " + owner + "_category_items values (%s,%s,now())"
				cur.execute(query,(item,starttime))
				con.commit()
				print "%s：get item %s" % (datetime.datetime.now(), item)
			else: print "%s already exists in the %s_category_items" % (item,owner)
		con.close()
	except Exception as e:
		print e


def handle(kw_page):
	item_list = find_advanced(kw_page[1],kw_page[2])
	# input_item(item_list)
	if item_list:
		for item in item_list:
			input_single(item)


def gevnet_main():
	kws = get_kw()
	jobs = [gevent.spawn(handle,kw_page) for kw_page in kws ]
	gevent.wait(jobs)


def main():
    id_kw = get_kw()
    for first in id_kw:
        item_list = find_advanced(first[1], first[2])
        input_item(item_list)


def mut_handle(owner):
	while True:
		try:
			kw = kw_page_queue.get()
			find_advanced(kw[1],kw[2],owner)
		except Exception as e:
			print e

def muti(owner):
	handle_list = list()
	get_kw(owner)
	for i in range(1,60):
		find_process = multiprocessing.Process(target=mut_handle,args=(owner,))
		handle_list.append(find_process)
	for pro in handle_list:
		pro.start()
		print "find_process starting....."
	




if __name__ == '__main__':
	muti('chy')
