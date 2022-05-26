package main

import (
	"flag"
	"fmt"
	"github.com/invertedv/chutils"
	"github.com/invertedv/freddie/joined"
	"log"
	"os"
	"sort"
	"strings"
	"time"
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
	create := flag.String("create", "Y", "string")
	table := flag.String("table", "", "string")

	type filePair struct {
		Static  string
		Monthly string
	}

	flag.Parse()
	// add trailing slash, if needed
	if (*srcDir)[len(*srcDir)-1] != '/' {
		*srcDir += "/"
	}
	con, err := chutils.NewConnect(*host, *user, *password, 40000000000)
	if err != nil {
		log.Fatalln(err)
	}
	defer con.Close()

	fileList := make(map[string]*filePair)

	dir, err := os.ReadDir(*srcDir)
	if err != nil {
		log.Fatalln(fmt.Errorf("error reading directory: %s", *srcDir))
	}

	for _, f := range dir {
		if ind := strings.Index(f.Name(), ".txt"); ind > 0 {
			root := f.Name()[ind-6 : ind]
			if fileList[root] == nil {
				fileList[root] = new(filePair)
			}
			if strings.Index(f.Name(), "time") > 0 {
				fileList[root].Monthly = *srcDir + f.Name()
			} else {
				fileList[root].Static = *srcDir + f.Name()
			}
		}
	}

	for k, v := range fileList {
		if v.Monthly == "" || v.Static == "" {
			log.Fatalln(fmt.Errorf("quarter %s is missing static or monthly", k))
		}
	}
	keys := make([]string, 0, len(fileList))
	for k := range fileList {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	start := time.Now()
	createTable := *create == "Y" || *create == "y"
	for ind, k := range keys {
		/*
			staticFile := "/mnt/drive3/data/freddie_data/historical_data_2014Q4.txt"
			monthlyFile := "/mnt/drive3/data/freddie_data/historical_data_time_2014Q4.txt"
			if e := joined.Load(monthlyFile, staticFile, *table, "tmp", createTable, con); e != nil {
				log.Fatalln(e)
			}
			_ = k
			if ind == 0 {
				break
			}
		*/

		if e := joined.Load(fileList[k].Monthly, fileList[k].Static, *table, "tmp", createTable, con); e != nil {
			log.Fatalln(e)
		}

		createTable = false
		fmt.Printf("Done with quarter %s. %d out of %d \n", k, ind+1, len(keys))
	}
	fmt.Println("elapsed time", time.Since(start))
}
