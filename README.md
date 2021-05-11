本项目主要基于华为的OpenGuass数据库，使用TPCH数据集的LINEITEM表进行操作。

主要实现了数据分区操作，以及分析Gauss数据库的插入，点查询速度。

使用方法：

(1).先运行test_partition.py，产生12个数据分区（均匀分布）

(2).接下来运行row.py和colomn.py，产生rows_insert.csv、rows_select.csv、cols_insert.csv、cols_select.csv。
