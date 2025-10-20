import connectorx as cx

dsn = "oracle://BU_SNAP_EMPH:Change.Pass.BU123!!!@10.63.133.38:1521/buprd1.buocisubpridbph.buocivcnphxdr.oraclevcn.com"
sql = "SELECT * FROM SYSADM.PS_STDNT_FA_TERM FETCH FIRST 10 ROWS ONLY"
df = cx.read_sql(dsn, sql)
print(df.head())

