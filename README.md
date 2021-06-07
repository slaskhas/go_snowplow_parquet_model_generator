### Handle large Parquet files unloaded from Redshift

When I cleaned up some old data from Redshift , specifically [Snowplow](https://snowplowanalytics.com/) data I ended up wanting to access the information locally on my Mac, long after I exported it. This turned out to be a bit tricky.

IF I used the (awesome) sqlite3 library, it failed because of the timestamp 
data format. Other tools couln't handle the size because they tried to keep all in memory. Instead I switched to use the Go library at
`https://github.com/xitongsys/parquet-go` and write some routines.

### INPUT Data

To export .parquet data from Redshift and store it on s3 you might do something like this:

```
unload ('select * from atomic.events 
where etl_tstamp < ''2020-07-01 00:00:00.000''')
   to 's3://my-snowplow-dump-bucket/00dump/evt2020_'
   PARQUET
   iam_role 'arn:aws:iam::123456789012:role/RedshiftUnLoadRole';`
```
Ending up with a set of files like

`evt2020_0000_part_00.parquet, 
evt2020_0001_part_00.parquet`

Each with the size up to the max size 6.2 GB .

### Hands on

If you copied these files down to your local machine you can generate a
model file for their structure by cloning this repo and doing:

```go run model_generator.go -file=./evt2020_0000_part_00.parquet > model.go```

then try to read the content by doing:


``` go run try_the_model.go model.go -file=./evt2020_0000_part_00.parquet ```

In this sample code I do a little conversion to make the timestamp fields a bit easier to read.

### Now the fun starts

From this you should be able to build out your code to export into other databases or file formats. Go to town.


