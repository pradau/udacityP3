#!/usr/bin/env python
# -*- coding: utf-8 -*-
import xml.etree.cElementTree as ET
import pprint
import re
import codecs
import json
import os
import sys

from collections import defaultdict

"""
Purpose: to transform the input openstreet map data into the form desired for input to the database and write it to a json format file.
The saved data file will be used by mongoimport later on to import the shaped data into MongoDB. 

The output should be a list of dictionaries that look like this:
{
"id": "2406124091",
"type: "node",
"visible":"true",
"created": {
          "version":"2",
          "changeset":"17206049",
          "timestamp":"2013-08-03T16:43:42Z",
          "user":"linuxUser16",
          "uid":"1219059"
        },
"pos": [41.9757030, -87.6921867],
"address": {
          "housenumber": "5157",
          "postcode": "60625",
          "street": "North Lincoln Ave"
        },
"amenity": "restaurant",
"cuisine": "mexican",
"name": "La Cabana De Don Luis",
"phone": "1 (773)-271-5176"
}


"""

# Regular expression compiled patterns.
#   "lower", for tags that contain only lowercase letters and are valid.
#   "lower_colon", for otherwise valid tags with a colon in their names.
#   "problemchars", for tags with problematic characters.
lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
startsCanadianPostalCode = re.compile(r'^[A-Za-z][0-9][A-Za-z]')
endsCanadianPostalCode = re.compile(r'[0-9][A-Za-z][0-9]')
street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)
street_type_number_re = re.compile(r'[^A-Za-z_-][0-9]+')

#list of the fields that will be retained for indicating how the tag was created.
CREATED = [ "version", "changeset", "timestamp", "user", "uid"]

#street types that are acceptable.
expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road", 
            "Trail", "Parkway", "Commons"]

# Dictionary having the street types we want to change as keys, and the corresponding standard types as values.
mapping = { "St": "Street",
            "St.": "Street",
            "Rd": "Road",
            "Rd.": "Road",
            "Ave": "Avenue",
            "Ave.": "Avenue",
            "S": "South",
            "N": "North",
            "E": "East",
            "W": "West"
            }

directions = ["North", "South", "East", "West" ]


#Find the street type (e.g. Ave.) in the street name (e.g. "4th Baldwin Ave."). If one is found, add it to the list of street types.
def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected:
            street_types[street_type].add(street_name)

#Return True if the input element is a street address.
def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")

#Open a street map file, and check all the nodes and ways for street addresses. Build a dictionary street_types that contains all the street types found in the input file.
#  This is for developing a strategy for cleaning the street type data.
def audit(osmfile):
    osm_file = open(osmfile, "r")
    street_types = defaultdict(set)
    for event, elem in ET.iterparse(osm_file, events=("start",)):
        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])

    return street_types

# This is used in the case that we want to update the word (street type) prior to the last word in the street name.
#  e.g. Wonderland Rd. South will be updated to Wonderland Road South.
#  e.g. Hwy 92 will be updated to Highway 92
def update_previous_name( name, mapping, street_type ):
    #eliminate excess white space that often precedes the number.
    name_before = name[:-len(street_type)].strip()
    fixedname = update_name(name_before, mapping)
    return ( fixedname + " " + street_type )


#Look for a street type in name, and if necessary standardize it using the mapping dictionary to transform it.
# Return the new street name with the standardized street type.
def update_name(name, mapping):
    m = street_type_re.search(name)
    if m:
        street_type = m.group()
        # if the  street type is a number (e.g. "Road 88"), then update the preceding word (which is likely a Street, Rd, Lane etc.) and ignore the number.
        m2 = street_type_number_re.match(street_type)
        if m2:
#             print('#old name:',name)
            name = update_previous_name( name, mapping, street_type )
#             print('#new name:',name)
        #if we find a street type that requires transformation
        elif street_type in mapping.keys():
#             print('old name:',name)
            new_street_type = mapping[street_type]
            name = name[:-len(street_type)].strip() + " " + new_street_type
            #If the street type is a direction (e.g. North) then we know to check the previous name as a street type. e.g. Wonderland Rd. S should be Wonderland Road South.
            if (new_street_type in directions):
                update_previous_name( name, mapping, new_street_type )
#             print('new name:',name)
    return name


#determine if the element is part of an address.
# Return: 
#   key - fieldname for this part of the address e.g. postcode.
#   value - field value for this part of the address. e.g. N6G 5E3
def is_address(elem):
    if elem.attrib['k'][:5] == "addr:":
        key = elem.attrib['k'][5:]
        value = elem.attrib['v']
        return key,value,True
    else:
        return None, None, False

# standardize the postal code to the Canadian format, otherwise return as null ''.
# I'm discarding any non-Canadian postal code because we are only concerned with obtaining correct postal codes for Canada.
def audit_postcode(postcode):
    newpostcode = ''
    print('old postcode:',postcode)
    if len(postcode) >= 6:
        m = startsCanadianPostalCode.split(postcode)
        #check that it begins with correct first 3 digits e.g. N6G
        if m and len(m) >= 2:
            #end will contain the 2nd part of the postal code but may also contain spaces.
            end = m[1].strip()
            #truncate the final group to 3 chars in case it has extra characters.
            end = end[:3]
            #check that it ends with correct pattern for 3 chars e.g. 5E2
            m2 = endsCanadianPostalCode.search(end)
            if m2:
                #insert single space between first and last group of characters to obtain a code like 'N6G 5E3' if length indicates incorrect number of whitespaces.
                if len(postcode) != 7:
                    postcode = postcode[:3] + ' ' + end
                #postal codes should have upper case letters.
                newpostcode = postcode.upper()
    print('new postcode:', newpostcode)
    return newpostcode
    

# determine what type of tag (key) we have for determining further processing.
# Returns 4 booleans, whose values are mutually exclusive (i.e. only one is true).
#  bLower is True for tags that contain only lowercase letters and are valid.
#  bLowerColon is True for otherwise valid tags with a colon in their names.
#  bProblemChars is True for tags with problematic characters.
#  bOther is True for all other tags.
def audit_tag( tag):
    bLower = False
    bLowerColon = False
    bProblemChars = False
    bOther = False
    m = lower.search(tag)
    if m:
        bLower = True
    else:
        m2 = lower_colon.search(tag)
        if m2:
            bLowerColon = True
        else:
            m3 = problemchars.search(tag)
            if m3:
                bProblemChars = True
    if not(bLower or bLowerColon or bProblemChars):
        bOther = True

    return bLower, bLowerColon, bProblemChars, bOther

#return True if the tags (assumed to be from a way) indicate it has lanes, therefore a road and not some other type of 'way'.
def is_road( element ):
    for tag in element.iter("tag"):
        if tag.attrib['k'] == 'lanes':
            if float(tag.attrib['v'][0])> 0:
                return True
    return False

# Determine if the input element is a "node" or "way" and if it is then parse to construct a node dictionary in 
#  the form we want to save to file. That dictionary will have substructures for 
#  address - information about the address of the map element.
#  node_ref_list - the list of nodes identifying the node or way.
#  pos - the map coordinates in order latitude, longitude.
#  created - information about the creation of the node/way.
def shape_element(element):
    node = {}
    address = {}
    node_ref_list = []
    pos = []
    created = {}
    
    if element.tag == "node" or element.tag == "way" :
        for tag in element.iter("tag"):
            #find address
            key, value, bIsAddress = is_address(tag)
            if bIsAddress:    
                bLower, bLowerColon, bProblemChars, bOther = audit_tag(key)
                if bLower:
                    if key == 'postcode':
                        value = audit_postcode( value )
                    elif key == 'street':
                        value = update_name( value, mapping )
                    address[key] = value
            else:
                key = tag.attrib['k']
                bLower, bLowerColon, bProblemChars, bOther = audit_tag(key)
                value = tag.attrib['v']
                
                if bLower or bLowerColon:
                    #if this is a road then the name will be updated like the street names in the address.
                    if key == 'name' and element.tag == 'way':
                        #check that this is truly a road by seeing if there is a lanes tag
                        if is_road( element ):
                            value = update_name( value, mapping )
                
                    node[key] = value
                    
        #find node refs
        for tag in element.iter("nd"):
            node_ref_list.append( tag.attrib['ref'] )
         
        #attributes of node or way directly   
        for key,value in element.attrib.items():
            bLower, bLowerColon, bProblemChars, bOther = audit_tag(key)
            if bLower:
                value = element.attrib[key]
                if key in CREATED:
                    created[key] = value
                elif key == 'lat':
                    pos.insert(0, float(value))
                elif key == 'lon':
                    pos.append(float(value))
                else:
                    node[key] = value
        
        #check if these special arrays exist, if so then insert into node.
        if (len(address) > 0 ):
            node["address"] = address
        if (len(node_ref_list) > 0 ):
            node["node_refs"] = node_ref_list
        if (len(created) > 0 ):
            node["created"] = created
        if (len(pos) > 0 ):
            node["pos"] = pos
        #type
        node['type'] = element.tag
                    
        return node
    else:
        return None


#Read the input Openstreet map file (*.osm), extract and shape the data required, write it out as a json file.
# file_in  - input osm file.
# pretty - True to make the json file human-readable but less compact due to extra indents and line returns.
# file_out - output filename, which is same as input with json extension.
# Return: data - which is the list of dictionaries containing all data that is written out to file.
def process_map(file_in, pretty = False):
    file_out = "{0}.json".format(file_in)
    data = []
    with codecs.open(file_out, "w") as fo:
        for _, element in ET.iterparse(file_in):
            # transform the element tag into dictionary el with the required information and formatting. el will be None if this tag has nothing we want to save.
            el = shape_element(element)
            if el:
                #append the formatted dictionary to data, and append to the output json file.
                data.append(el)
                if pretty:
                    fo.write(json.dumps(el, indent=2)+"\n")
                else:
                    fo.write(json.dumps(el) + "\n")
    return data


def main():
#     filename = 'sample-test.osm'
    filename = 'sample-london2.osm'
#     filename = 'london_ontario_canada.osm'
    #check for street types that need auditing. These lines were used to check for possible street types that need different cleaning strategies.
#     st_types = audit(filename)
#     pprint.pprint(dict(st_types))
#     sys.exit()
    #If the file is over 50MB in size the output file will be written in the compact format
    statinfo = os.stat(filename)
    filesize = statinfo.st_size/(1024)**2
    if filesize > 50:
        pretty = False
    else:
        pretty = True
    print("File size is: %s MB" %(filesize) )
    data = process_map(filename, pretty)
    #pprint.pprint(data)

#     i = 0
#     pprint.pprint( data[i] )

#     for i in range(10):
#         print('index',i)
#         pprint.pprint( data[i] )


# if the program is called directly from the command line then main() will be executed.
if __name__ == "__main__":
    main()