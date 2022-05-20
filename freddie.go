package main

import (
	"flag"
	"fmt"
	"github.com/invertedv/chutils"
	s "github.com/invertedv/chutils/sql"
	_ "github.com/mailru/go-clickhouse/v2"
	"io/ioutil"
	"log"
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
	flag.Parse()
	con, err := chutils.NewConnect("http", *host, *user, *password)
	if err != nil {
		log.Fatalln(err)
	}
	defer con.Close()
	/*
		if _, e := con.Exec("SET max_memory_usage = 40000000000;"); e != nil {
			log.Fatalln(e)
		}
		if _, e := con.Exec("SET max_bytes_before_external_group_by=20000000000;"); e != nil {
			log.Fatalln(e)
		}

	*/
	//con.Exec("SET max_threads=" + str(threads))

	dir, err := ioutil.ReadDir(*srcDir)
	if err == nil {
		for _, f := range dir {
			fmt.Println(f)
		}
	}
	start := time.Now()
	/*
		fileName := "/mnt/drive3/data/freddie_data/historical_data_2000Q1.txt"
		if err := static.LoadRaw(fileName, "bbb", true, con); err != nil {
			log.Fatalln(err)
		}

		fileName = "/mnt/drive3/data/freddie_data/historical_data_time_2000Q1.txt"
		if err := monthly.LoadRaw(fileName, "aaa", true, 12, con); err != nil {
			log.Fatalln(err)
		}

	*/

	qry := `
WITH v AS (
SELECT
    lnID,
    arrayStringConcat(arrayMap(x,y -> concat(x, ':', toString(y)), field, validation), ';') as x
FROM ( 
SELECT
    lnID,
    groupArray(f) AS field,
    groupArray(v) AS validation
FROM(    
    SELECT
      lnID,
      field AS f,
      Max(valid) AS v
    FROM (
        SELECT
            lnID,
            arrayElement(splitByChar(':', v), 1) AS field,
            arrayElement(splitByChar(':', v), 2) = '0' ? 0 : 1 AS valid
        FROM (    
            SELECT
                lnID,
                arrayJoin(splitByChar(';', valMonthly)) AS v
            FROM
                aaa))
     GROUP BY lnID, field
     ORDER BY lnID, field)
GROUP BY lnID))
SELECT
    s.*,
    m.month,
    m.upb,
    m.dqStat,
    m.age,
    m.rTermLgl,
    m.mod,
    m.zb,
    m.curRate,
    m.defrl,
    m.intUPB,
    m.lpd,
    m.defectDt,
    m.zbDt,
    m.zbUPB,
    m.dqDis,
    m.ProNet,
    m.Loss,
    m.mMonth,
    m.mTLoss,
    m.mCLoss,
    m.mStep,
    v.x AS valMonthly
FROM
    bbb AS s
JOIN (
    SELECT
        lnID,
        groupArray(month) AS month,
        groupArray(upb) AS upb,
        groupArray(dqStat) AS dqStat,
        groupArray(age) AS age,
        groupArray(rTermLgl) AS rTermLgl,
        groupArray(mod) AS mod,
        groupArray(zb) AS zb,
        groupArray(curRate) AS curRate,
        groupArray(defrl) AS defrl,
        groupArray(intUPB) AS intUPB,
        max(lpd) AS lpd,
        max(defectDt) AS defectDt,
        max(zbDt) AS zbDt,
        max(zbUPB) as zbUPB,
        groupArray(dqDis = 'Y' ? aaa.month : Null) AS dqDis,
        groupArray(if(fclLoss > 1000000.0, Null, fclProNet)) AS ProNet,
        groupArray(if(fclLoss > 1000000.0, Null, fclLoss)) AS Loss,
        groupArray(if(modTLoss >= 0.0 or modCLoss >= 0.0 or stepMod = 'Y' , aaa.month, Null)) AS mMonth,
        groupArray(if(modTLoss >= 0.0 or modCLoss >= 0.0 or stepMod = 'Y' , modTLoss, Null)) AS mTLoss,
        groupArray(if(modTLoss >= 0.0 or modCLoss >= 0.0 or stepMod = 'Y' , modCLoss, Null)) AS mCLoss,
        groupArray(if(modTLoss >= 0.0 or modCLoss >= 0.0 or stepMod = 'Y' , stepMod, Null)) AS mStep
    FROM
        aaa
    GROUP BY lnID) AS m
ON s.lnID = m.lnID
JOIN v
ON s.lnID = v.lnID
`
	srdr := s.NewReader(qry, con)
	if e := srdr.Init("lnID", chutils.MergeTree); e != nil {
		log.Fatalln(e)
	}
	if e := srdr.TableSpec().Nest("monthly", "month", "intUPB"); e != nil {
		log.Fatalln(e)
	}
	srdr.TableSpec().Nest("mods", "mMonth", "mStep")
	//	srdr.TableSpec().Nest("fcl", "ProNet", "Loss")
	srdr.Name = "ccc"
	if e := srdr.TableSpec().Create(con, "ccc"); e != nil {
		log.Fatalln(e)
	}
	if e := srdr.Insert(); e != nil {
		log.Fatalln(e)
	}
	fmt.Println("elapsed time", time.Since(start), "total", (48000/117.)*time.Since(start).Hours())

}
