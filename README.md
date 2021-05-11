本项目主要基于华为的OpenGuass数据库，使用TPCH数据集的LINEITEM表进行操作。


主要实现了数据分区操作，以及分析Gauss数据库的插入，点查询速度。

建表语句：

CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,

                        L_PARTKEY     INTEGER NOT NULL,

                        L_SUPPKEY     INTEGER NOT NULL,

                        L_LINENUMBER  INTEGER NOT NULL,

                        L_QUANTITY    DECIMAL(15,2) NOT NULL,

                        L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,

                        L_DISCOUNT    DECIMAL(15,2) NOT NULL,

                        L_TAX         DECIMAL(15,2) NOT NULL,

                        L_RETURNFLAG  CHAR(1) NOT NULL,

                        L_LINESTATUS  CHAR(1) NOT NULL,

                        L_SHIPDATE    DATE NOT NULL,

                        L_COMMITDATE  DATE NOT NULL,

                        L_RECEIPTDATE DATE NOT NULL,

                        L_SHIPINSTRUCT CHAR(25) NOT NULL,

                        L_SHIPMODE     CHAR(10) NOT NULL,

                        L_COMMENT      VARCHAR(44) NOT NULL);

使用方法：

(1).先运行test_partition.py，产生12个数据分区（均匀分布）

(2).接下来运行row.py和colomn.py，产生rows_insert.csv、rows_select.csv、cols_insert.csv、cols_select.csv。
