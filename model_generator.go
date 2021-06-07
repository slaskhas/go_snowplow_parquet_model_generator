package main

import (
	"strings"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"fmt"
	"reflect"
	"flag"
)


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

	pr, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		fmt.Println("Can't create parquet reader", err)
		return
	}


	res, err := pr.ReadByNumber(1)

	if err != nil {
		fmt.Println("Can't read", err)
		return
	}
	res0 := res[0]

	t := reflect.TypeOf(res0)

	fmt.Printf("package main\n\n\ntype Event struct {\n")

	for i := 0; i < t.NumField(); i++ {
		tp := fmt.Sprintf("%+v",t.Field(i).Type)
		tpt := strings.Replace(tp, "*", "", 1)
		namedc := strings.ToLower(t.Field(i).Name)
		speca := ""
		if (tpt == "string") {
			speca = fmt.Sprintf("`parquet:\"name=%s, type=UTF8, encoding=PLAIN_DICTIONARY\"`",namedc)
		} else
		if (tpt == "int32") {
			speca = fmt.Sprintf("`parquet:\"name=%s, type=INT32\"`",namedc)
		} else
		if (tpt == "float64") {
			speca = fmt.Sprintf("`parquet:\"name=%s, type=DOUBLE\"`",namedc)
		} else
		if (tpt == "bool") {
			speca = fmt.Sprintf("`parquet:\"name=%s, type=BOOLEAN\"`",namedc)
		}

		fmt.Printf("     %+v  %s %s\n", t.Field(i).Name,tpt,speca)
	}
	fmt.Printf("}\n")
	pr.ReadStop()
	fr.Close()
}
