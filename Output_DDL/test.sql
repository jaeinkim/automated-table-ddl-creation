DROP TABLE `emart-datafabric.bw.ZCA_AD001`;
CREATE OR REPLACE TABLE `emart-datafabric.bw.ZCA_AD001`
(
   BNAME STRING NOT NULL OPTIONS(description="사용자 마스터 레코드의 사용자 이름"),
   ZBIZTPCD STRING NOT NULL OPTIONS(description="업태"),
   ZMCH_L2 STRING NOT NULL OPTIONS(description="중분류"),
   RECORDMODE STRING OPTIONS(description="RECORDMODE"),
   ZBIZTPNM STRING OPTIONS(description="코드설명"),
   ZMCH_L2_NM STRING OPTIONS(description="중분류명"),
   ZMCH_L1 STRING OPTIONS(description="대분류"),
   ZMCH_L1_NM STRING OPTIONS(description="대분류명"),
   ZTEAMCD STRING OPTIONS(description="팀코드"),
   ZTEAMNM STRING OPTIONS(description="팀명"),
   ZDICD STRING OPTIONS(description="담당코드"),
   ZDINM STRING OPTIONS(description="담당명"),
   ZHQCD STRING OPTIONS(description="본부코드"),
   ZHQNM STRING OPTIONS(description="본부명"),
   PUR_FLG STRING OPTIONS(description="발주 가능 여부"),
   LOEVM STRING OPTIONS(description="삭제 지시자"),
   ZCRNAME STRING OPTIONS(description="생성인"),
   ZCRSDATE DATE OPTIONS(description="생성일자"),
   ZCRSTIME TIME OPTIONS(description="생성시간"),
   ZCRLDATE DATE OPTIONS(description="생성일자(로컬)"),
   ZCRLTIME TIME OPTIONS(description="생성시간(로컬)"),
   ZUDNAME STRING OPTIONS(description="변경인"),
   ZUDSDATE DATE OPTIONS(description="변경일자"),
   ZUDSTIME TIME OPTIONS(description="변경시간"),
   ZUDLDATE DATE OPTIONS(description="변경일자(로컬)"),
   ZUDLTIME TIME OPTIONS(description="변경시간(로컬)"),
   ZONLO STRING OPTIONS(description="ABAP System Field: Time Zone of Current User"),
   LASTCHANGEDAT FLOAT64 OPTIONS(description="LASTCHANGEDAT"),
   ODQ_CHANGEMODE STRING OPTIONS(description="Change Mode for a Data Record in the Delta"),
   ODQ_ENTITYCNTR FLOAT64 OPTIONS(description="Number of Data Units (Data Records for Example)"),
   PTT_DT DATE OPTIONS(description="파티션 키"), 
   ETL_LOAD_DTTM STRING OPTIONS(description="ETL적재일시")
)
CLUSTER BY BNAME, ZBIZTPCD, ZMCH_L2
OPTIONS(description="[CA] 사용자별 권한")
;

DROP TABLE `emart-datafabric.bw.ZCO_AD001`;
CREATE OR REPLACE TABLE `emart-datafabric.bw.ZCO_AD001`
(
   MANDT STRING NOT NULL OPTIONS(description="클라이언트"),
   KOKRS STRING NOT NULL OPTIONS(description="관리회계 영역"),
   BUKRS STRING NOT NULL OPTIONS(description="회사 코드"),
   ZPRCTR STRING NOT NULL OPTIONS(description="손익센터"),
   ZPMCHLV STRING OPTIONS(description="대분류"),
   ZMCH_L2 STRING OPTIONS(description="중분류"),
   ZBPYM STRING OPTIONS(description="YYYYMM"),
   RECORDMODE STRING OPTIONS(description="RECORDMODE"),
   WAERS STRING OPTIONS(description="통화"),
   ZBPSALESD01 FLOAT64 OPTIONS(description="매출실적 1일"),
   ZBPSALESD02 FLOAT64 OPTIONS(description="매출실적 2일"),
   ZBPSALESD03 FLOAT64 OPTIONS(description="매출실적 3일"),
   ZBPSALESD04 FLOAT64 OPTIONS(description="매출실적 4일"),
   ZBPSALESD05 FLOAT64 OPTIONS(description="매출실적 5일"),
   ZBPSALESD06 FLOAT64 OPTIONS(description="매출실적 6일"),
   ZBPSALESD07 FLOAT64 OPTIONS(description="매출실적 7일"),
   ZBPSALESD08 FLOAT64 OPTIONS(description="매출실적 8일"),
   ZBPSALESD09 FLOAT64 OPTIONS(description="매출실적 9일"),
   ZBPSALESD10 FLOAT64 OPTIONS(description="매출실적 10일"),
   ZBPSALESD11 FLOAT64 OPTIONS(description="매출실적 11일"),
   ZBPSALESD12 FLOAT64 OPTIONS(description="매출실적 12일"),
   ZBPSALESD13 FLOAT64 OPTIONS(description="매출실적 13일"),
   ZBPSALESD14 FLOAT64 OPTIONS(description="매출실적 14일"),
   ZBPSALESD15 FLOAT64 OPTIONS(description="매출실적 15일"),
   ZBPSALESD16 FLOAT64 OPTIONS(description="매출실적 16일"),
   ZBPSALESD17 FLOAT64 OPTIONS(description="매출실적 17일"),
   ZBPSALESD18 FLOAT64 OPTIONS(description="매출실적 18일"),
   ZBPSALESD19 FLOAT64 OPTIONS(description="매출실적 19일"),
   ZBPSALESD20 FLOAT64 OPTIONS(description="매출실적 20일"),
   ZBPSALESD21 FLOAT64 OPTIONS(description="매출실적 21일"),
   ZBPSALESD22 FLOAT64 OPTIONS(description="매출실적 22일"),
   ZBPSALESD23 FLOAT64 OPTIONS(description="매출실적 23일"),
   ZBPSALESD24 FLOAT64 OPTIONS(description="매출실적 24일"),
   ZBPSALESD25 FLOAT64 OPTIONS(description="매출실적 25일"),
   ZBPSALESD26 FLOAT64 OPTIONS(description="매출실적 26일"),
   ZBPSALESD27 FLOAT64 OPTIONS(description="매출실적 27일"),
   ZBPSALESD28 FLOAT64 OPTIONS(description="매출실적 28일"),
   ZBPSALESD29 FLOAT64 OPTIONS(description="매출실적 29일"),
   ZBPSALESD30 FLOAT64 OPTIONS(description="매출실적 30일"),
   ZBPSALESD31 FLOAT64 OPTIONS(description="매출실적 31일"),
   PTT_DT DATE OPTIONS(description="파티션 키"), 
   ETL_LOAD_DTTM STRING OPTIONS(description="ETL적재일시")
)
PARTITION BY PTT_DT 
CLUSTER BY MANDT, KOKRS, BUKRS, ZPRCTR
OPTIONS(description="[BP] 매출/이익실적(대/중,손익센터,일별-매출계획 Outbound I/F). Partition 키: ZBPYM")
;

