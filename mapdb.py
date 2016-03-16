#!/usr/bin/env python
"""
Purpose: read the OSM map data from the mongodb server and calculate some statistics.
"""
import pprint
import json
import csv
import sys
import os

def read_data( coll_name ):
    data = []
    filename = coll_name+'.json'
    with open(filename) as f:
        data = json.load(f)
    pprint.pprint(data)
    return data
                
        
#Return the database corresponding to our collection
## Assumes that the following command has already inserted our data at the command line:
# mongoimport --db <db_name> --collection <coll_name> --file <coll_name>.json
def get_db(db_name, coll_name):
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client[db_name]
    collection = db[coll_name]
#     print('one example db entry')
#     print (collection.find_one())
    return db

#pipeline to count users
def make_pipeline_to_count_users():
    pipeline = []
    #aggregate by field "source" and count number of entries stored in "count"
    pipeline.append( {"$group" : {  "_id" : "$created.user", 
                                    "count" : { "$sum" : 1 }}
                     } )
    #sort by the "count" field. -1 means descending sort.
    pipeline.append( {"$sort" : { "count" : -1 } } )
    return pipeline

#pipeline to count nodes/ways.  
def make_pipeline_to_count_nodes_ways():
    pipeline = []
    #aggregate by field "source" and count number of entries stored in "count"
    pipeline.append( {"$group" : {  "_id" : "$type", 
                                    "count" : { "$sum" : 1 }}
                     } )
    #sort by the "count" field. -1 means descending sort.
    pipeline.append( {"$sort" : { "count" : -1 } } )
    return pipeline


#pipeline to count amenities
def make_pipeline_to_count_amenities( amenity ):
    pipeline = []

    #the "match" is to compare a field to a value or do a logical op.
    #WARNING: we don't use {$eq, <value>} when checking equality to a scalar, we can just write <value> instead.
    pipeline.append( {"$match" : {"amenity" : amenity}
                    } )     

    #create the fields 'refs', 'name' and 'user'.
    pipeline.append( {"$project" : {  
                                    "refs" : "$node_refs",
                                    "name" : "$name",
                                    "user" : "$created.user"                                               
                       }
                     } )  

    #sort by the "name" field. -1 means ascending sort.
    pipeline.append( {"$sort" : { "name" : 1 } } )

    return pipeline


# applies the pipeline aggregation to the collection to produce a list of results.
def sources(pipeline, collection):
    return [doc for doc in collection.aggregate(pipeline)]


if __name__ == '__main__':  
    db_name = 'users' 
#     coll_name = 'sample-london2.osm'
    coll_name = 'sample-test.osm'
    statinfo = os.stat(coll_name + '.json')
    filesize = statinfo.st_size/(1024)**2
    print("JSON File size is: %s MB" % filesize )
    
    db = get_db(db_name, coll_name)
    collection = db[coll_name]

    if collection.find_one() == None:
        read_data( coll_name )
        db.db_name.insert(data) 
        collection = db[coll_name]
        print('test: one example db entry for collection:', coll_name)
        print (collection.find_one())
               
    print("Count the nodes and ways:")
    pipeline = make_pipeline_to_count_nodes_ways()
    result = sources(pipeline, db[coll_name])
    sum = 0
    for i in range(len(result)):
        sum += result[i]['count']
        print('Count of %s: %d' % (result[i]['_id'], result[i]['count']) )
    print('Count of all records:', sum )
   
    pipeline = make_pipeline_to_count_users()
    result = sources(pipeline, db[coll_name])
    print('\nCount of all unique users:', len(result) )
    print('Top ten users by created content:')
    for i in range(10):
        print('User %s created nodes/ways: %d' % (result[i]['_id'], result[i]['count']) )

    amenity = 'hospital'
    pipeline = make_pipeline_to_count_amenities( amenity )
    result = sources(pipeline, db[coll_name])
    print('\nHospitals (named)')
    sum = 0
    for i in range(len(result)):
        if 'name' in result[i].keys():
            sum += 1
            print('%s: User %s created %d node references.' % (result[i]['name'], result[i]['user'], len(result[i]['refs'])) )
    print('Number of named hospitals:', sum)
    
    amenity = 'cafe'
    pipeline = make_pipeline_to_count_amenities( amenity )
    result = sources(pipeline, db[coll_name])
    print('\nCafes:')
    print('Number of cafes:', len(result))

    for i in range(len(result)):
        bName = bRefs = bUser = False
        if 'name' in result[i].keys():
            bName = True
        if 'refs' in result[i].keys():
            bRefs = True   
        if 'user' in result[i].keys():
            bUser = True           
        if bName and bRefs and bUser:  
            print('%s: User %s created %d node references.' % (result[i]['name'], result[i]['user'], len(result[i]['refs'])) )
        elif bName and bUser:
            print('%s: User %s created the tag.' % (result[i]['name'], result[i]['user']) )
        elif bName:
             print('%s: Unknown creator.' % (result[i]['name']) )

