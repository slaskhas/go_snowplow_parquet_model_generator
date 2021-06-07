
package main

import (
	"encoding/json"
	"encoding/binary"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"flag"
	"time"
	"fmt"
)

//// taken from https://github.com/xitongsys/parquet-go/blob/master/types/converter.go

const (
	JULIAN_DAY_OF_EPOCH int64 = 2440588
	MICROS_PER_DAY      int64 = 3600 * 24 * 1000 * 1000
)


func fromJulianDay(days int32, nanos int64) time.Time {
	nanos = ((int64(days)-JULIAN_DAY_OF_EPOCH)*MICROS_PER_DAY + nanos/1000) * 1000
	sec, nsec := nanos/time.Second.Nanoseconds(), nanos%time.Second.Nanoseconds()
	t := time.Unix(sec, nsec)
	return t.UTC()
}

func INT96ToTime(int96 string) time.Time {
	nanos := binary.LittleEndian.Uint64([]byte(int96[:8]))
	days := binary.LittleEndian.Uint32([]byte(int96[8:]))

	return fromJulianDay(int32(days), int64(nanos))
}

/////
var (
	parquetFile = "./to-2020-09-30.parquet"
)


func main() {
	flag.StringVar(&parquetFile, "file", "./to-2020-09-30.parquet", "input filename")
	flag.Parse()
	

	fr, err := local.NewLocalFileReader(parquetFile)
	if err != nil {
		fmt.Println("Can't open file", err)
		return
	}

	pr, err := reader.NewParquetReader(fr, nil, 1)
	if err != nil {
		fmt.Println("Can't create parquet reader", err)
		return
	}

	num := int(pr.GetNumRows())
	
	fmt.Println("Rows: ", num)

	for i := 0; i < 1; i++ {
		evts := make([]Event,1)

		if err = pr.Read(&evts); err != nil {
			fmt.Println("Read error", err)
			return
		}
		evt := evts[0]
		evt.Etl_tstamp = INT96ToTime(evt.Etl_tstamp).String()

		if ( evt.Collector_tstamp != "" ) {evt.Collector_tstamp =  INT96ToTime(evt.Collector_tstamp).String()}
		if evt.Derived_tstamp != "" { evt.Dvce_created_tstamp = INT96ToTime(evt.Dvce_created_tstamp).String() }
		if evt.Dvce_sent_tstamp != "" { evt.Dvce_sent_tstamp = INT96ToTime(evt.Dvce_sent_tstamp).String() }
		if evt.Refr_dvce_tstamp != "" { evt.Refr_dvce_tstamp  = INT96ToTime(evt.Refr_dvce_tstamp ).String() }
		if evt.Derived_tstamp != "" {evt.Derived_tstamp = INT96ToTime(evt.Derived_tstamp).String() }
		if evt.True_tstamp != "" { evt.Collector_tstamp =  INT96ToTime(evt.True_tstamp).String() }
		jsonBs, err := json.Marshal(evt)
		if err != nil {
			fmt.Println("Can't to json", err)
			return
		}

		fmt.Println(string(jsonBs))
	}
	
	pr.ReadStop()
	fr.Close()
}
