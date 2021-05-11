import csv
import pyodbc
import random
import numpy as np

[x for x in pyodbc.drivers() if x.startswith('GaussMPP')]
cnxn = pyodbc.connect('DRIVER={GaussMPP};SERVER=localhost;PORT=26000;DATABASE=postgres;UID=omm;PWD=gauss@333')
crsr = cnxn.cursor()

file = "cols_insert.csv"
with open(file, "w", newline='') as csvFile:
    writer = csv.writer(csvFile, delimiter=',')
    writer.writerow(
        ['keyFieldSize', 'nonKeyFieldSize', 'numOfFixedLengthField', 'numOfVarLengthField', 'numOfRows', 'timePerRow'])
csvFile.close()

file = "cols_select.csv"
with open(file, "w", newline='') as csvFile:
    writer = csv.writer(csvFile, delimiter=',')
    writer.writerow(
        ['keyFieldSize', 'nonKeyFieldSize', 'numOfFixedLengthField', 'numOfVarLengthField', 'numOfRows', 'timePerRow'])
csvFile.close()

for MAX in range(0, 100):
    print(MAX)
    crsr.execute("drop table if exists test")

    a = [0,1,2,3,4,5,6,7,8,9,10,11]
    u = random.randint(0,12)  
    b = random.sample(a,u)
    n = random.randint(1, 12)
    b.sort()

    lambd = 0.5
    keyFieldSize = 16
    nonKeyFieldSize = 0
    numOfRows = 0

    y = ['L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE',
         'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT']

    lamda = 0.5
    m = [int(random.expovariate(lamda) + 5), int(random.expovariate(lamda) + 9),
         int(random.expovariate(lamda) + 4), int(random.expovariate(lamda) + 4),
         int(random.expovariate(lamda) + 1), int(random.expovariate(lamda) + 1),
         int(random.expovariate(lamda) + 19), int(random.expovariate(lamda) + 19),
         int(random.expovariate(lamda) + 19), int(random.expovariate(lamda) + 17),
         int(random.expovariate(lamda) + 7), int(random.expovariate(lamda) + 44)]
    p = ['varchar', 'char']

    for i in range(u):
        nonKeyFieldSize += m[b[i]]

    numOfFixedLengthField = 0
    numOfVarLengthField = 0

    sql = 'CREATE TABLE test(L_ORDERKEY varchar(20),L_PARTKEY varchar(20),L_SUPPKEY varchar(20),L_LINENUMBER varchar(' \
          '20) '
    for i in b:
        t = random.randint(0, 1)
        sql += ',' + y[i] + ' ' + p[t] + '(' + str(m[i]) + ')'
        if t == 0:
            numOfVarLengthField += 1
        if t == 1:
            numOfFixedLengthField += 1
    sql += ') with (orientation=column)'
    print(sql)

    print(numOfVarLengthField)
    print(numOfFixedLengthField)

    sql4 = "explain analyze SELECT * FROM test where L_ORDERKEY=" + str(random.randint(0, 10000))
    sql5 = "SELECT * FROM test where L_ORDERKEY=" + str(random.randint(0, 10000))

    file = "%02d.csv" % (n)
    with open(file, 'r') as rcsvFile:
        reader = csv.reader(rcsvFile)
        crsr.execute(sql)
        first = next(reader)
        first = next(reader)
        print(first)
        for m in range(1, 10000):
            sql2 = "INSERT INTO test VALUES("
            sql2 += first[0] + ','
            sql2 += first[1] + ','
            sql2 += first[2] + ','
            sql2 += first[3]
            for i in b:
                print(first[i+4])	
                sql2+=',' + "'" +first[i+4] + "'"
            sql2 += ')'
            crsr.execute(sql2)
            first = next(reader)
            cnxn.commit()

        insert = [0] * 10000
        ins = 0
        for i in range(1, 10000):
            a = ""
            sql3 = "explain analyze " + sql2
            rows = crsr.execute(sql3)
            for j in rows:
                a = a + str(j)

            a = a[a.index("Total runtime: ") + len("Total runtime: "): a.index(" ms")]
            print(a)
            insert[ins] = a
            ins += 1
        select = [0] * 100
        numOfRowss = [0] * 100
        ins = 0
        for i in range(0, 100):
            b = ""
            si = random.randint(0, 10000)
            sql4 = "explain analyze SELECT * FROM test where L_ORDERKEY=" + str(si)
            sql5 = "SELECT * FROM test where L_ORDERKEY=" + str(si)
            rows = crsr.execute(sql5)
            for item in rows:
                numOfRowss[i] += 1 
            rows = crsr.execute(sql4)
            for j in rows:
                b = b + str(j)      
            b = b[b.index("Total runtime: ") + len("Total runtime: "): b.index(" ms")]
            print(b)
            select[ins] = b
            ins += 1
    rcsvFile.close()

    file = "cols_insert.csv"
    with open(file, "a+", newline='') as csvFile:
        writer = csv.writer(csvFile, delimiter=',')
        sum = 0.0
        for i in range(0, 10000):
            sum += float(insert[i])
        a = str(sum / 10000)
        writer.writerow([keyFieldSize, nonKeyFieldSize, numOfFixedLengthField, numOfVarLengthField, 1, a])
    csvFile.close()

    file = "cols_select.csv"
    with open(file, "a+", newline='') as csvFile:
        writer = csv.writer(csvFile, delimiter=',')
        sum = 0.0
        for i in range(0, 100):
            writer.writerow([keyFieldSize, nonKeyFieldSize, numOfFixedLengthField, numOfVarLengthField, str(numOfRowss[i]), str(float(select[i]))])
    csvFile.close()

crsr.commit()
crsr.close()
cnxn.close()
