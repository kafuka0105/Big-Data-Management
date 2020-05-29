import csv
import sys
from pyspark import SparkContext

def createIndex(shapefile):
    import rtree
    import fiona.crs
    import geopandas as gpd
    zones = gpd.read_file(shapefile).to_crs(fiona.crs.from_epsg(2263))
    index = rtree.Rtree()
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def findZone(p, index, zones):
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if zones.geometry[idx].contains(p):
            return idx
    return None

def process_drug_keywords(txtfile):
    import csv
    drug_words=[]
    with open(txtfile) as file:
        reader = csv.reader(file)
        for row in reader:
            drug_words.append(row)
    return drug_words

def toCSV(_, records):
    for tractid, ratio in records:
        yield ','.join((str(tractid),str(ratio)))

def preprocessedtw(row):
    import re
    #remove emojis
    row = row.encode('ascii', 'ignore').decode('ascii')
    #remove links
    row = re.sub(r"http\S+", "", row)
    #remove punctuations
    punctuation = '[,.!?*-_+=\/|@#$%^&]'
    row = re.sub(punctuation, ' ', row)
    row = re.sub('  ', ' ', row)
    return row

def find_tweet_zone(pid, records):
    import csv
    import pyproj
    import shapely.geometry as geom
    
    drug_words = process_drug_keywords('drug_illegal.txt') + process_drug_keywords('drug_sched2.txt')
    
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)    
    index, zones = createIndex('500cities_tracts.geojson')    
    
    reader = csv.reader(records, delimiter='|')

    for row in reader:
        if len(row) >=6:
            drug_related_tw = set()
            try:
                tweet_point = geom.Point(proj(float(row[2]), float(row[1])))
                tweet_zone_idx = findZone(tweet_point, index, zones)
                tweet_zone = zones.plctract10[tweet_zone_idx] # census_tract zone
                zone_pop = zones.plctrpop10[tweet_zone_idx] # population
            except:
                continue

            text = row[5].lower() # lower case
            text_tw = preprocessedtw(text)
            text_tw = ' '+text_tw+' ' # adding space in the front and tail of each tweet for later use.

            for words in drug_words:
                for i in words:
                    if ' '+i+' ' in text_tw: 
                        drug_related_tw.add('found')

            if tweet_zone:    
                if 'found' in drug_related_tw:
                    yield ((tweet_zone, zone_pop), 1)

if __name__=='__main__':                    
    sc = SparkContext()
    sc.textFile(sys.argv[1]) \
        .mapPartitionsWithIndex(find_tweet_zone) \
        .reduceByKey(lambda x,y: x+y) \
        .map(lambda x: (x[0][0], x[1]/x[0][1])) \
        .sortByKey() \
        .mapPartitionsWithIndex(toCSV) \
        .saveAsTextFile(sys.argv[2])