# 1. automated-table-ddl-creation
- Creating automated tables for use in Aggregation Layer of BigQuery and to reduce manual effort of writing csv to table ddl. 
- This activity has reduced resource focusing on writing SQL Queries(DDL) by 100%.
- The Tables are created from a CSV file as an input and the Output is the SQL Table DDL File and can be run in any Google BigQuery Instance.

# 2. Structure to be created:

```
DROP TABLE `datafabric.bw.ZCO_AD054`;
CREATE OR REPLACE TABLE `datafabric.bw.ZCO_AD054`
(
   CO_AREA STRING NOT NULL OPTIONS(description="관리회계 영역"),
   COMP_CODE STRING NOT NULL OPTIONS(description="회사 코드"),
   ZCOVERS STRING NOT NULL OPTIONS(description="경영계획버전"),
   FISCVARNT STRING NOT NULL OPTIONS(description="회계연도 변형"),
   FISCYEAR NUMERIC OPTIONS(description="회계연도"),
   CALYEAR NUMERIC OPTIONS(description="년도"),
   CALMONTH NUMERIC OPTIONS(description="년월"),
   ZITYPE STRING OPTIONS(description="항목구분"),
   ZLVCD1 STRING OPTIONS(description="투자구분1"),
   ZLVCD2 STRING OPTIONS(description="투자구분2"),
   ZLVCD3 STRING OPTIONS(description="투자구분3"),
   ZLVCD4 STRING OPTIONS(description="투자구분4"),
   ZPATYP STRING OPTIONS(description="계획유형"),
   WBS_ELEMT STRING OPTIONS(description="작업 분석 구조 요소(WBS 요소)"),
   RECORDMODE STRING OPTIONS(description="RECORDMODE"),
   HALFYEAR1 NUMERIC OPTIONS(description="반기"),
   CALQUARTER NUMERIC OPTIONS(description="년분기"),
   CALMONTH2 NUMERIC OPTIONS(description="월"),
   CURRENCY STRING OPTIONS(description="통화 키"),
   AMOUNT FLOAT64 OPTIONS(description="금액"),
   PTT_DT DATE OPTIONS(description="파티션 키"), 
   ETL_LOAD_DTTM STRING OPTIONS(description="ETL적재일시")
)
PARTITION BY PTT_DT 
CLUSTER BY CO_AREA, COMP_CODE, ZCOVERS, FISCVARNT
OPTIONS(description="[CO-투자] 투자월보 계획대비실적. Partition 키: CALMONTH")
;
```

# 3. Installing
This Automated Framework was created on **Python 3.8.5** and uses some external libraries listed below:

### a) CSV
### b) Pandas
### c) Numpy

# 4. Build/Run Command
Use following commands to build/Run the project from the project root. 
### CSV Sheet (Excel File which has the Table_Name and Columns row by row)
### Above CSV sheet can be found in input/test.csv (keep your own, but preserve headers of the original file)
### Output can be found in Output_DDL/test.sql
````
python .\table_creation_main.py
````

