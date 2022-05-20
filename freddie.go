package main

import (
	"flag"
	"github.com/invertedv/chutils"
	"github.com/invertedv/freddie/joined"
	"log"
)

// TODO: consider removing validation in static/monthly and putting it in joined...?  or on arrays: take max of elements
// TODO: consider changing validation to field names  key:val  upb:0;ltv:1....
// TODO: for arrays, ... ? summary or actual array?
func main() {
	var err error
	host := flag.String("host", "127.0.0.1", "string")
	user := flag.String("user", "", "string")
	password := flag.String("password", "", "string")
	srcDir := flag.String("dir", "", "string")
	create := flag.String("create", "", "string")
	table := flag.String("table", "", "string")

	_, _, _ = *create, *table, *srcDir
	flag.Parse()
	con, err := chutils.NewConnect(*host, *user, *password, 40000000000)
	if err != nil {
		log.Fatalln(err)
	}
	defer con.Close()
	/*
		dir, err := ioutil.ReadDir(*srcDir)
		if err == nil {
			for _, f := range dir {
				fmt.Println(f)
			}
		}
	*/
	staticFile := "/mnt/drive3/data/freddie_data/historical_data_2000Q1.txt"
	monthlyFile := "/mnt/drive3/data/freddie_data/historical_data_time_2000Q1.txt"

	joined.Load(monthlyFile, staticFile, "ddd", "tmp", con)

}
