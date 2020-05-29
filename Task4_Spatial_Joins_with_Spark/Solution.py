import csv
import sys
from pyspark import SparkContext
    
def createIndex(shapefile):
    '''
    This function takes in a shapefile path, and return:
    (1) index: an R-Tree based on the geometry data in the file
    (2) zones: the original data of the shapefile
    
    Note that the ID used in the R-tree 'index' is the same as
    the order of the object in zones.
    '''
    import rtree
    import fiona.crs
    import geopandas as gpd
    zones = gpd.read_file(shapefile).to_crs(fiona.crs.from_epsg(2263))
    index = rtree.Rtree()
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def findZone(p, index, zones):
    '''
    findZone returned the ID of the shape (stored in 'zones' with
    'index') that contains the given point 'p'. If there's no match,
    None will be returned.
    '''
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if zones.geometry[idx].contains(p):
            return idx
    return None

def toCSV(_, records):
    for boro,[(Top1_Count, Top1_Name),(Top2_Count, Top2_Name),(Top3_Count, Top3_Name)] in records:
        yield ','.join((boro,Top1_Name,str(Top1_Count),Top2_Name,str(Top2_Count),Top3_Name,str(Top3_Count)))

def processTrips(pid, records):
    '''
    Our aggregation function that iterates through records in each
    partition, checking whether we could find a zone that contain
    the pickup location.
    '''
    import csv
    import pyproj
    import shapely.geometry as geom
    
    # Create an R-tree index
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)    
    index, zones = createIndex('neighborhoods.geojson')    
    
    # Skip the header
    if pid==0:
        next(records)
    reader = csv.reader(records)
    counts = {}
    
    for row in reader:
        try:
            end = geom.Point(proj(float(row[9]), float(row[10])))
            start = geom.Point(proj(float(row[5]), float(row[6])))
            
            match_end = findZone(end, index, zones)
            match_start = findZone(start, index, zones)
        except:
            continue
            
        if match_start and match_end:
            counts[(zones.borough[match_start], zones.neighborhood[match_end])] = counts.get((zones.borough[match_start], zones.neighborhood[match_end]), 0) + 1

    return counts.items()

if __name__=='__main__':
    sc = SparkContext()
    sc.textFile(sys.argv[1]) \
        .mapPartitionsWithIndex(processTrips) \
        .reduceByKey(lambda x,y: x+y) \
        .map(lambda x: (x[0][0],(x[1],x[0][1]))) \
        .groupByKey() \
        .map(lambda x: (x[0], sorted(x[1], reverse = True)[:3])) \
        .sortByKey() \
        .mapPartitionsWithIndex(toCSV) \
        .saveAsTextFile(sys.argv[2])
