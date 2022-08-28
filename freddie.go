// package freddie.  This package imports the loan-level residential mortgage data provided by Freddie Mac into ClickHouse.
// The data is available here: https://www.freddiemac.com/research/datasets/sf-loanlevel-dataset.
//
// The final result is a single data with nested arrays for time-varying fields.
// Features:
//   - The data is subject to QA.  The results are presented as two string fields in a KeyVal format.
//   - A "DESCRIBE" of the output table provides info on each field
//   - New fields created are:
//   - vintage (e.g. 2010Q2)
//   - standard - Y/N flag, Y=standard process loan
//   - loan age based on first pay date
//   - numeric dq field
//   - reo flag
//   - property value at origination
//   - file names from which the loan was loaded
//   - QA results. There are three sets of fields:
//   - The nested table qa that has two arrays:
//   - field.  The name of a field that has validation issues.
//   - cntFail. The number of months for which this field failed qa.  For static fields, this value will
//     be 1.
//   - allFail.  An array of field names which failed for qa.  For monthly fields, this means the field failed for all months.
//
// command-line parameters:
//
//	-host  ClickHouse IP address. Default: 127.0.0.1.
//	-user  ClickHouse user. Default: default
//	-password ClickHouse password for user. Default: <empty>.
//	-table ClickHouse table in which to insert the data.
//	-create if Y, then the table is created/reset. Default: Y.
//	-dir directory with Freddie Mac text files.
//	-tmp ClickHouse database to use for temporary tables.
//	-concur # of concurrent processes to use in loading monthly files. Default: 1.
//	-memory max memory usage by ClickHouse.  Default: 40000000000.
//	-groupby max_bytes_before_external_groupby ClickHouse parameter. Default: 20000000000.
//
// Since the standard and non-standard datasets have the same format, this utility can be used to create tables
// using either source.  A combined table can be built by running the app twice pointing to the same -table.
// On the first run, set -create Y and set -create N for the second run.
//
// Look at the example in the joined package for the DESCRIBE output of the table.
//
// Note that the table produced by this package has slightly fewer loans than the check figures provided by Freddie.
// The difference seems to be that there are some loans in the static file that are not in the monthly file.
// With data through 2021Q3, this totals 1484 standard loans (HARP and non-HARP), and 207 non-standard loans.
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
	user := flag.String("user", "default", "string")
	password := flag.String("password", "", "string")
	srcDir := flag.String("dir", "", "string")
	create := flag.String("create", "Y", "string")
	table := flag.String("table", "", "string")
	tmp := flag.String("tmp", "", "string")
	nConcur := flag.Int("concur", 1, "int")
	max_memory := flag.Int64("memory", 40000000000, "int64")
	max_groupby := flag.Int64("groupby", 20000000000, "int64")

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
		"max_memory_usage":                   *max_memory,
		"max_bytes_before_external_group_by": *max_groupby,
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
		s := time.Now()
		if e := joined.Load(fileList[k].Monthly, fileList[k].Static, *table, *tmp, createTable, *nConcur, con); e != nil {
			log.Fatalln(e)
		}
		createTable = false

		fmt.Printf("Done with quarter %s. %d out of %d: time %0.2f minutes\n", k, ind+1, len(keys), time.Since(s).Minutes())
	}
	fmt.Printf("elapsed time: %0.2f hours\n", time.Since(start).Hours())
}
