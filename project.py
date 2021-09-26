#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from email.parser import Parser
import re
import time
from datetime import datetime, timezone, timedelta

def date_to_dt(date):
    def to_dt(tms):
        def tz():
            return timezone(timedelta(seconds=tms.tm_gmtoff))
        return datetime(tms.tm_year, tms.tm_mon, tms.tm_mday, 
                      tms.tm_hour, tms.tm_min, tms.tm_sec, 
                      tzinfo=tz())
    return to_dt(time.strptime(date[:-6], '%a, %d %b %Y %H:%M:%S %z'))



def merge_email_date(rdd):
    #Extracting the email address and date sepeartely, merging them into a single list and 
    # returning the corresponding rdd. 
    rdd1 = list(rdd.map(lambda x: re.findall('\S+@[a-z.]*enron.com', x)).collect())
    date = list(rdd.map(lambda x: re.findall('Date.*', x)).map(lambda x: str(x[0])).map(lambda x: x[6:]).collect())

    for i in range(len(date)):
        rdd1[i] = (rdd1[i],date[i])
    
    return sc.parallelize(rdd1)



# /!\ All functions below must return an RDD object /!\

# T1: replace pass with your code
def extract_email_network(rdd):
    rdd = merge_email_date(rdd)
    
    #from assignment 2 
    local = '[a-zA-Z][a-zA-Z0-9.!#$%&\'\*+-/=?^_`{|}~]+'
    domain = '([\w-]+(\.)?[a-z]+)+'
    email_regex = local+'@'+domain
    valid_email = lambda s: True if re.compile(email_regex).fullmatch(s) else False

    rdd1 = rdd.filter(lambda x: len(x[0])!=1)
    rdd2 = rdd1.map(lambda x: (x[0][0],list(x[0][1:]),x[-1]))
    rdd3 = rdd2.map(lambda x: [(x[0],i,date_to_dt(x[-1])) for i in x[1]])
    rdd4 = rdd3.map(lambda x: [i for i in x if i[0]!=i[1]])
    rdd5 = rdd4.flatMap(lambda x: x).distinct()
    rdd6 = rdd5.filter(lambda x: valid_email(x[1]))
    return rdd6

# T2: replace pass with your code            
def get_monthly_contacts(rdd):

    rdd1 = rdd.map(lambda x: (x[0],x[1],str(x[2].month)+"/"+str(x[2].year)+""))
    rdd2 = rdd1.map(lambda x: (str(x[0])+"_"+str(x[2])+"",x[1])) #merging the sender with the month/year to use as key
    rdd3 = rdd2.map(lambda x: (x[0],1)).reduceByKey(lambda x,y: x+y) #grouping with the same key and getting the count 
    rdd4 = rdd3.map(lambda x: (x[0].split("_"),x[1])) #finally spliting the sender and month
    rdd5 = rdd4.map(lambda x: (x[0][0],x[0][1],x[1]))
    return rdd5

# T3: replace pass with your code
def convert_to_weighted_network(rdd, drange=None):

    rdd1 = rdd.map(lambda x: (str(x[0])+"_"+str(x[1]),datetime(x[2].year, x[2].month, x[2].day)))
    if drange == None: #conditon when there is no date range passed. 
        rdd3 = rdd1.map(lambda x: (x[0],1)).reduceByKey(lambda x,y: x+y)
        #similar to what we did in Q1.2 merge sender and month/year and use as key to group and count
    else: #when date range is passed 
        d1 = datetime(drange[0].year, drange[0].month, drange[0].day)
        d2 = datetime(drange[1].year, drange[1].month, drange[1].day)
        rdd2 = rdd1.filter(lambda x: (d1 < x[1] and x[1] < d2))
        rdd3 = rdd1.map(lambda x: (x[0],1)).reduceByKey(lambda x,y: x+y)
        #similar to what we did in Q1.2 merge sender and month/year and use as key to group and count
    rdd4 = rdd3.map(lambda x: (x[0].split("_"),x[1]))
    rdd5 = rdd4.map(lambda x: (x[0][0],x[0][1],x[1]))
    return rdd5

# T4.1: replace pass with your code
def get_out_degrees(rdd):
    
    rdd_sender_count = rdd.map(lambda x: (x[0],x[2])).collect()#list with sender and count
    rdd_receiver = rdd.map(lambda x: (x[1],0)).collect()#list with receiver and zero
    rdd_sender_count.extend(rdd_receiver)
    rdd = sc.parallelize(rdd_sender_count) # convert extended list to rdd
    rdd = rdd.map(lambda x: (x[0],x[1])).reduceByKey(lambda x,y: x+y)
    rdd2 = rdd.sortBy(lambda x: [x[1], x[0]]).collect()
    rdd2.reverse()
    rdd3 = sc.parallelize(rdd2) #reverse list and convert to rdd
    rdd4 = rdd3.map(lambda x: (x[1],x[0])) #swap count and sender
    return rdd4

# T4.2: replace pass with your code         
def get_in_degrees(rdd):
    
    rdd_receiver_count = rdd.map(lambda x: (x[1],x[2])).collect()#list with receiver and count
    rdd_sender = rdd.map(lambda x: (x[0],0)).collect()#list with sender and zero
    rdd_receiver_count.extend(rdd_sender)
    rdd = sc.parallelize(rdd_receiver_count)
    rdd = rdd.map(lambda x: (x[0],x[1])).reduceByKey(lambda x,y: x+y)
    rdd2 = rdd.sortBy(lambda x: [x[1], x[0]]).collect()
    rdd2.reverse()
    rdd3 = sc.parallelize(rdd2)#reverse list and convert to rdd
    rdd4 = rdd3.map(lambda x: (x[1],x[0]))#swap receiver and count
    return rdd4


#Below function and code is for Question 2 

def num_of_node(rdd, drange=None):
    rdd7 = rdd.map(lambda x: (str(x[0])+"_"+str(x[1]),datetime(x[2].year, x[2].month, x[2].day)))
    if drange == None:
        rdd8 = rdd7.map(lambda x: (x[0],1)).reduceByKey(lambda x,y: x+y)
    else:
        d1 = datetime(drange[0].year, drange[0].month, drange[0].day)
        d2 = datetime(drange[1].year, drange[1].month, drange[1].day)
        rdd8 = rdd7.filter(lambda x: (d1 < x[1] and x[1] < d2))
        rdd8 = rdd8.map(lambda x: (x[0],1)).reduceByKey(lambda x,y: x+y)
    rdd9 = rdd8.map(lambda x: (x[0].split("_"),x[1]))
    rdd10a = rdd9.map(lambda x: x[0][0])
    rdd10b = rdd9.map(lambda x: x[0][1])
    rdd11 = rdd10a.union(rdd10b)
    rdd11 = rdd11.distinct()
    return rdd11.count()

months = [10,11,12,1,2,3,4,5,6,7,8,9]
year = 2000
months_limit = [1,2,3,4,5,6,7,8,9]

outd = []
intd = []
num_of_nodes = []
for i in range(len(months)):
    if months[i] in months_limit:
        year = 2001
        
    rdd_network=convert_to_weighted_network(
        extract_email_network(
        utf8_decode_and_filter(sc.sequenceFile(
                '/user/ufac001/project2021/samples/enron20.seq'))), 
                (datetime(2000, 9, 1, tzinfo = timezone.utc), 
                 datetime(year, months[i], 1, tzinfo = timezone.utc)))
    nodes=num_of_node(
        extract_email_network(
        utf8_decode_and_filter(sc.sequenceFile(
                '/user/ufac001/project2021/samples/enron20.seq'))), 
                (datetime(2000, 9, 1, tzinfo = timezone.utc), 
                 datetime(year, months[i], 1, tzinfo = timezone.utc)))
    
    out = sc.parallelize(get_out_degrees(rdd_network).take(1))
    ind = sc.parallelize(get_in_degrees(rdd_network).take(1))
    
    outd.append(out.map(lambda x: (x[0])).collect())
    intd.append(ind.map(lambda x: (x[0])).collect())
    num_of_nodes.append(nodes)


outd_flat_list = [i for item in outd for i in item]
intd_flat_list = [i for item in intd for i in item]

import matplotlib.pyplot as plt
import numpy as np


fig = plt.figure()
ax = plt.axes()
ax.plot(num_of_nodes,outd_flat_list)
ax.plot(num_of_nodes,intd_flat_list)
plt.ylabel("kmax")
plt.xlabel("No. of Nodes")
plt.title("Visualisation of kmax v No. of Nodes")
plt.legend("OI")