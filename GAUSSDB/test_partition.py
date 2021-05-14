import pyodbc
import csv

if __name__ == '__main__':
    [x for x in pyodbc.drivers() if x.startswith('GaussMPP')]
    cnxn = pyodbc.connect('DRIVER={GaussMPP};SERVER=localhost;PORT=26000;DATABASE=postgres;UID=omm;PWD=gauss@333')
    crsr = cnxn.cursor()

    m = 16
    n = 6001215
    num = 12
    nnum = int(n / num)
    nnnum = int(n - num * nnum)

    y = [0] * n * m

    rows = crsr.execute("SELECT L_ORDERKEY FROM LINEITEM order by L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER")
    index = 0
    for item in rows:
        a = 0 + index * m
        if a < nnum * m:
            y[a] = item[0]
            index += 1

    rows = crsr.execute("SELECT L_PARTKEY FROM LINEITEM order by L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER")
    index = 0
    for item in rows:
        a = 1 + index * m
        if (a < nnum * m):
            y[a] = item[0]
            index += 1

    rows = crsr.execute("SELECT L_SUPPKEY FROM LINEITEM order by L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER")
    index = 0
    for item in rows:
        a = 2 + index * m
        if (a < nnum * m):
            y[a] = item[0]
            index += 1

    rows = crsr.execute("SELECT L_LINENUMBER FROM LINEITEM order by L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER")
    index = 0
    for item in rows:
        a = 3 + index * m
        if (a < nnum * m):
            y[a] = item[0]
            index += 1

    rows = crsr.execute("SELECT L_QUANTITY FROM LINEITEM order by L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER")
    index = 0
    for item in rows:
        a = 4 + index * m
        if (a < nnum * m):
            y[a] = item[0]
            index += 1

    rows = crsr.execute("SELECT L_EXTENDEDPRICE FROM LINEITEM order by L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER")
    index = 0
    for item in rows:
        a = 5 + index * m
        if (a < nnum * m):
            y[a] = item[0]
            index += 1
    print(5)
    rows = crsr.execute("SELECT L_DISCOUNT FROM LINEITEM order by L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER")
    index = 0
    for item in rows:
        a = 6 + index * m
        if (a < nnum * m):
            y[a] = item[0]
            index += 1

    rows = crsr.execute("SELECT L_TAX FROM LINEITEM order by L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER")
    index = 0
    for item in rows:
        a = 7 + index * m
        if (a < nnum * m):
            y[a] = item[0]
            index += 1

    rows = crsr.execute("SELECT L_RETURNFLAG FROM LINEITEM order by L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER")
    index = 0
    for item in rows:
        a = 8 + index * m
        if (a < nnum * m):
            y[a] = item[0]
            index += 1

    rows = crsr.execute("SELECT L_LINESTATUS FROM LINEITEM order by L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER")
    index = 0
    for item in rows:
        a = 9 + index * m
        if (a < nnum * m):
            y[a] = item[0]
            index += 1

    rows = crsr.execute("SELECT L_SHIPDATE FROM LINEITEM order by L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER")
    index = 0
    for item in rows:
        a = 10 + index * m
        if (a < nnum * m):
            y[a] = item[0]
            index += 1
    print(10)
    rows = crsr.execute("SELECT L_COMMITDATE FROM LINEITEM order by L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER")
    index = 0
    for item in rows:
        a = 11 + index * m
        if (a < nnum * m):
            y[a] = item[0]
            index += 1

    rows = crsr.execute("SELECT L_RECEIPTDATE FROM LINEITEM order by L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER")
    index = 0
    for item in rows:
        a = 12 + index * m
        if (a < nnum * m):
            y[a] = item[0]
            index += 1

    rows = crsr.execute("SELECT L_SHIPINSTRUCT FROM LINEITEM order by L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER")
    index = 0
    for item in rows:
        a = 13 + index * m
        if (a < nnum * m):
            y[a] = item[0]
            index += 1

    rows = crsr.execute("SELECT L_SHIPMODE FROM LINEITEM order by L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER")
    index = 0
    for item in rows:
        a = 14 + index * m
        if (a < nnum * m):
            y[a] = item[0]
            index += 1

    print(15)
    rows = crsr.execute("SELECT L_COMMENT FROM LINEITEM order by L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER")
    index = 0
    for item in rows:
        a = 15 + index * m
        if (a < nnum * m):
            y[a] = item[0]
            index += 1

    for i in range(num):
        file = "%02d.csv" % (i + 1)
        with open(file, "w", newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=',')
            writer.writerow(['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER',
                             'L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX',
                             'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE', 'L_COMMITDATE',
                             'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT'])
            for j in range(0, index - 1):
                writer.writerow(
                    [y[j * num * m + 0], y[j * num * m + 1], y[j * num * m + 2], y[j * num * m + 3], y[j * num * m + 4],
                     y[j * num * m + 5], y[j * num * m + 6], y[j * num * m + 7], y[j * num * m + 8], y[j * num * m + 9],
                     y[j * num * m + 10], y[j * num * m + 11], y[j * num * m + 12], y[j * num * m + 13],
                     y[j * num * m + 14], y[j * num * m + 15]])
            if (index * 12) < n:
                writer.writerow(
                    [y[nnum * num * m + 0], y[nnum * num * m + 1], y[nnum * num * m + 2], y[nnum * num * m + 3],
                     y[nnum * num * m + 4], y[nnum * num * m + 5], y[nnum * num * m + 6], y[nnum * num * m + 7],
                     y[nnum * num * m + 8], y[nnum * num * m + 9], y[nnum * num * m + 10], y[nnum * num * m + 11],
                     y[nnum * num * m + 12], y[nnum * num * m + 13], y[nnum * num * m + 14], y[nnum * num * m + 15]])
            csvfile.close()


    crsr.commit()
    crsr.close()
    cnxn.close()
