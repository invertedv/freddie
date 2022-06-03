// package main.  This package imports the loan-level residential mortgage data provided by Freddie Mac into ClickHouse.
// The data is available here: https://www.freddiemac.com/research/datasets/sf-loanlevel-dataset.
//
// The final result is a single data with nested arrays for time-varying fields.
// Features:
//   - The data is subject to QA.  The results are presented as two string fields in a KeyVal format.
//   - A "DESCRIBE" of the output table provides info on each field
//   - New fields created are:
//      - loan age based on first pay date
//      - numeric dq field
//      - reo flag
//      - vintage (e.g. 2010Q2)
//      - property value at origination
//      - file names from which the loan was loaded
//      - QA results
//           - There are two fields -- one for monthly data and one for static data.
//           - The QA results are in keyval format: <field name>:<result>.  result: 0 if pass, 1 if fail.
//
// command-line parameters:
//   -host  ClickHouse IP address
//   -user  ClickHouse user
//   -password ClickHouse password for user
//   -table ClickHouse table in which to insert the data
//   -create if Y, then the table is created/reset.
//   -dir directory with Freddie Mac text files
//   -tmp ClickHouse database to use for temporary tables
//
// Since the standard and non-standard datasets have the same format, this utility can be used to create tables
// using either source.  A combined table can be built by running the app twice pointing to the same -table.
// On the first run, set -create Y and set -create N for the second run.
//
// Look at the example in the joined package for the DESCRIBE output of the table.
package main

import (
	"flag"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/invertedv/chutils"
	"github.com/invertedv/freddie/joined"
	"log"
	"os"
	"sort"
	"strings"
	"time"
)

func main() {
	var err error
	host := flag.String("host", "127.0.0.1", "string")
	user := flag.String("user", "", "string")
	password := flag.String("password", "", "string")
	srcDir := flag.String("dir", "", "string")
	create := flag.String("create", "Y", "string")
	table := flag.String("table", "", "string")
	tmp := flag.String("tmp", "", "string")

	// Each quarter of data consists of two files: one for static data, one for monthly data
	type filePair struct {
		Static  string
		Monthly string
	}

	flag.Parse()
	// add trailing slash, if needed
	if (*srcDir)[len(*srcDir)-1] != '/' {
		*srcDir += "/"
	}
	// connect to ClickHouse
	con, err := chutils.NewConnect(*host, *user, *password, clickhouse.Settings{
		"max_memory_usage":                   40000000000,
		"max_bytes_before_external_group_by": 20000000000,
	})
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if e := con.Close(); e != nil {
			log.Fatalln(e)
		}
	}()

	// holds the set of files to work through
	fileList := make(map[string]*filePair)

	dir, err := os.ReadDir(*srcDir)
	if err != nil {
		log.Fatalln(fmt.Errorf("error reading directory: %s", *srcDir))
	}

	// build the file list
	for _, f := range dir {
		if ind := strings.Index(f.Name(), ".txt"); ind > 0 {
			root := f.Name()[ind-6 : ind] // Year & Quarter CCYY"Q"Q
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

	// Check we got pairs
	for k, v := range fileList {
		if v.Monthly == "" || v.Static == "" {
			log.Fatalln(fmt.Errorf("quarter %s is missing static or monthly", k))
		}
	}

	// create a slice of keys.  We'll work through the data in chronological order
	keys := make([]string, 0, len(fileList))
	for k := range fileList {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	start := time.Now()
	createTable := *create == "Y" || *create == "y"
	for ind, k := range keys {
		if e := joined.Load(fileList[k].Monthly, fileList[k].Static, *table, *tmp, createTable, con); e != nil {
			log.Fatalln(e)
		}

		createTable = false
		fmt.Printf("Done with quarter %s. %d out of %d \n", k, ind+1, len(keys))
	}
	fmt.Println("elapsed time", time.Since(start))
}
