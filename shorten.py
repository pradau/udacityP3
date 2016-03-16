#!/usr/bin/env python
# -*- coding: utf-8 -*-
# This code is used to shorten the osm openstreet map file to approx. 1/10 its size.
# WARNING: It requires python2.7

import xml.etree.ElementTree as ET  # Use cElementTree or lxml if too slow

OSM_FILE = "london_ontario_canada.osm"  # Replace this with your osm file
SAMPLE_FILE = "sample-london2.osm"


def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag

    Reference:
    http://stackoverflow.com/questions/3095434/inserting-newlines-in-xml-file-generated-via-xml-etree-elementtree-in-python
    """
    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


with open(SAMPLE_FILE, 'wb') as output:
    output.write('<?xml version="1.0" encoding="UTF-8"?>\n')
    output.write('<osm>\n  ')

    # Write every 10th top level element
    for i, element in enumerate(get_element(OSM_FILE)):
        if i % 9 == 0:
            output.write(ET.tostring(element, encoding='utf-8'))

    output.write('</osm>')