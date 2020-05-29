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
    for word, count in records:
        yield ','.join((str(word),str(count)))

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

def docfre(pid, records):
    import csv 
    
    reader = csv.reader(records, delimiter='|')
    
    DF = {}

    for row in reader:
        if len(row) >=6:

            text = row[5].lower() # lower case
            text_tw = preprocessedtw(text)
            
            for i in set(text_tw.strip().split(' ')):
                DF[i] = DF.get(i,0) + 1
    return DF.items()

def top3fre(pid, records):
    import csv
    import pyproj
    import shapely.geometry as geom
    
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)    
    index, zones = createIndex('500cities_tracts.geojson')
    
    drug_words = process_drug_keywords('drug_illegal.txt') + process_drug_keywords('drug_sched2.txt')   
    
    reader = csv.reader(records, delimiter='|')
    
    documentfre = documentfrequency_bc.value
    top3 = {}

    for row in reader:
        if len(row) >=6:
            drug_related_tw = set()
            tmp = {}
            try:
                tweet_point = geom.Point(proj(float(row[2]), float(row[1])))
                tweet_zone_idx = findZone(tweet_point, index, zones)

            except:
                continue

            text = row[5].lower() # lower case
            text_tw = preprocessedtw(text)
            text_tw = ' '+text_tw+' ' # adding space in the front and tail of each tweet for later use.
            
            for words in drug_words:
                for i in words:
                    if ' '+i+' ' in text_tw: 
                        drug_related_tw.add('found')

            if tweet_zone_idx and ('found' in drug_related_tw):
                for i in set(text_tw.strip().split(' ')):
                    tmp.update({i:documentfre[i]})
                
                top3pairs = sorted(tmp.items(), key = lambda i:i[1])[:3]
                
                for w in top3pairs:
                    top3[w[0]] = top3.get(w[0],0) + 1

    return top3.items()
            
if __name__=='__main__':                    
    sc = SparkContext()                
    
    rdd = sc.textFile(sys.argv[1])
    documentfrequency = rdd.mapPartitionsWithIndex(docfre) \
                .reduceByKey(lambda x,y: x+y) \
                .collectAsMap()

    documentfrequency_bc = sc.broadcast(documentfrequency)

    top3counts = rdd.mapPartitionsWithIndex(top3fre) \
                .reduceByKey(lambda x,y: x+y) \
                .map(lambda x: (x[1],x[0])) \
                .sortByKey(ascending=False) \
                .map(lambda x: (x[1],x[0])) \
                .take(100)

    sc.parallelize(top3counts).mapPartitionsWithIndex(toCSV).saveAsTextFile(sys.argv[2])