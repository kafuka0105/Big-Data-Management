import csv
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
    
def findID(House, St_name, County, dicttgt):
    #perform match here
    
    key = (St_name,County)
    
    if key in dicttgt.keys():
        for i in dicttgt[key]:
            try:
                if type(House) != tuple:
                    if (House % 2 == 1):
                        if House >= i[1] and House <= i[2]:
                            return int(i[0])
                    elif (House % 2 == 0): 
                        if House >= i[3] and House <= i[4]:
                            return int(i[0])
                elif type(House) == tuple:
                    if (House[1] % 2 == 1): 
                        if House >= i[1] and House <= i[2]:
                            return int(i[0])
                    elif (House[1] % 2 == 0): 
                        if House >= i[3] and House <= i[4]:
                            return int(i[0])
            except:
                pass
            continue

    return None

def toCSV(_, records):
    for physicalid, (count15, count16, count17, count18, count19, OLS) in records:
        yield ','.join((str(physicalid), str(count15), str(count16), str(count17), str(count18), str(count19), str(OLS)))
        
def OLS(_, records):
    import numpy as np
    import sklearn
    from sklearn.linear_model import LinearRegression
    for physicalid, (count15, count16, count17, count18, count19) in records:
        Y = np.array([count15, count16, count17, count18, count19]).reshape(-1, 1)
        X = np.array([2015,2016,2017,2018,2019]).reshape(-1, 1)
        reg = LinearRegression().fit(X, Y)
        OLS_COEF = reg.coef_
        
        yield physicalid, (count15, count16, count17, count18, count19, float(OLS_COEF))

def make_tuple(row):
    import re
    alphabet = re.compile(r'[A-Za-z]')
    
    try:
        if any(c.isalpha() for c in row) == True:
            row = re.sub(alphabet, '', row)
            row = row.strip()
            if '-' in row:
                row = tuple(map(int, row.split('-')))
            elif ('-' not in row) and row.isdigit():
                row = int(row)
            elif ' ' in row:
                row = tuple(map(int, row.split(' ')))
            else:
                pass
        
        elif any(c.isalpha() for c in row) == False:
            if '-' in row:
                row = tuple(map(int, row.split('-')))
            elif ('-' not in row) and row.isdigit():
                row = int(row)
            elif ' ' in row:
                row = tuple(map(int, row.split(' ')))
            else:
                pass
        
        else:
            pass
    except:
        pass
    return row
        
def processViolation(pid, records):
    import csv
    
    # Skip the header
    if pid==0:
        next(records)
    reader = csv.reader(records)
    counts = {}
    physicalid_dict = allIds_dict_bc.value
    dicttgt2 = dicttgt_bc.value
    county_dic = {'NY':1, 'MAN':1, 'MH':1, 'MN':1, \
                  'NEWY':1, 'NEW Y':1, 'BRONX': 2, 'BX':2, \
                  'BK':3,'K':3,'KING':3,'KINGS':3,\
                  'Q':4,'QN':4,'QNS':4,'QU':4,'QUEEN':4,\
                  'R':5,'RICHMOND':5}
    
    for row in reader:
        if len(row) == 43 and row[23] != '':
            
            try:
                year, House, St_name, County = row[4][-2:], row[23], row[24].lower(), county_dic.get(row[21])
                House = make_tuple(House)
                
                ID = findID(House, St_name, County, dicttgt2)
            except:
                continue
            
            if year >= '15' and year <= '19':
                if ID in physicalid_dict.keys():
                    counts[(ID,'15')] = 0
                    counts[(ID,'16')] = 0
                    counts[(ID,'17')] = 0
                    counts[(ID,'18')] = 0
                    counts[(ID,'19')] = 0
                    physicalid_dict.pop(ID)

                if ID:
                    counts[(ID,year)] = counts.get((ID,year),0) + 1
    
    return counts.items()

def process_scl(pid, records):

    for stname1, stname2, boro, pid, llow, lhigh, rlow, rhigh in records:
        
        try:
            stname1 = stname1.lower()
            stname2 = stname2.lower()
            boro = int(boro)
            pid = int(pid)
        except:
            continue
        
        if llow == 'None' or lhigh == 'None' or rlow == 'None' or rhigh == 'None':
            continue
        
        llow = make_tuple(llow)
        lhigh = make_tuple(lhigh)
        rlow = make_tuple(rlow)
        rhigh = make_tuple(rhigh)
        
        yield stname1, stname2, boro, pid, llow, lhigh, rlow, rhigh

def merge_two_dicts(x, y):
    z = x.copy()
    z.update(y)
    return z

if __name__=='__main__':
    sc = SparkContext()
    spark = SparkSession(sc)

    df = spark.read.csv('hdfs:///tmp/bdm/nyc_cscl.csv', header=True, multiLine=True, escape='"')
    dfnew = df[['FULL_STREE', 'ST_LABEL', 'BOROCODE', 'PHYSICALID', 'L_LOW_HN', 'L_HIGH_HN','R_LOW_HN','R_HIGH_HN']]
    #dfnew = dfnew.na.drop()
    rdd = dfnew.rdd.map(lambda row: [str(c) for c in row])
    
    dict1 = rdd.mapPartitionsWithIndex(process_scl) \
        .map(lambda x: ((x[0],x[2]),[[x[3],x[4],x[5],x[6],x[7]]])) \
        .reduceByKey(lambda x,y: x + y) \
        .collectAsMap()
    dict2 = rdd.mapPartitionsWithIndex(process_scl) \
        .map(lambda x: ((x[1],x[2]),[[x[3],x[4],x[5],x[6],x[7]]])) \
        .reduceByKey(lambda x,y: x + y) \
        .collectAsMap()

    dicttgt = merge_two_dicts(dict1, dict2)
    
    allphysicalid = rdd.map(lambda x: (int(x[3]))).collect()
    allphysicalid = set(allphysicalid)
    
    dicttgt_bc = sc.broadcast(dicttgt)

    allIds_dict = {el:0 for el in allphysicalid}
    allIds_dict_bc = sc.broadcast(allIds_dict)

    allIds_list = [(el,0) for el in allphysicalid]
    rdd2 = sc.parallelize(allIds_list)

    sc.textFile('hdfs:///tmp/bdm/nyc_parking_violation/') \
            .mapPartitionsWithIndex(processViolation) \
            .reduceByKey(lambda x,y: x+y) \
            .sortByKey() \
            .map(lambda x: (x[0][0],(x[0][1],x[1]))) \
            .reduceByKey(lambda x,y: x+y) \
            .map(lambda x: (x[0],(x[1][1],x[1][3],x[1][5],x[1][7],x[1][9]))) \
            .mapPartitionsWithIndex(OLS) \
            .fullOuterJoin(rdd2) \
            .sortByKey() \
            .map(lambda x: (x[0],(0,0,0,0,0,0) if x[1][0] is None else x[1][0])) \
            .mapPartitionsWithIndex(toCSV) \
            .saveAsTextFile(sys.argv[1])
