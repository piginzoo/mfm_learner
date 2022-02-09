from datasource import datasource_factory

ds = datasource_factory.get()
df = ds.index_weight("000905.SH","20080101","20220209")
print(df)
print("合计：",len(df))
# python -m test.toy.test_database_datasource