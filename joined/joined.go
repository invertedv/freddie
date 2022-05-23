package joined

import (
	"fmt"
	"github.com/invertedv/chutils"
	s "github.com/invertedv/chutils/sql"
	mon "github.com/invertedv/freddie/monthly"
	stat "github.com/invertedv/freddie/static"
	"log"
	"time"
)

func Load(monthly string, static string, table string, tmpDB string, con *chutils.Connect) error {
	start := time.Now()
	if err := stat.LoadRaw(static, "bbb", true, con); err != nil {
		log.Fatalln(err)
	}
	if err := mon.LoadRaw(monthly, "aaa", true, 12, con); err != nil {
		log.Fatalln(err)
	}

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
    m.bap,

    m.lpd,
    m.defectDt,
    m.zbDt,
    m.zbUPB,
    m.fileMonthly,
    m.dqDis,

    arrayElement(m.fclMonth, length(m.fclMonth)) AS fclMonth,
    arrayElement(m.fclProNet, length(m.fclMonth)) AS fclProNet,
    arrayElement(m.fclProMi, length(m.fclMonth)) AS fclProMi,
    arrayElement(m.fclProMw, length(m.fclMonth)) AS fclProMw,
    arrayElement(m.fclExp, length(m.fclMonth)) AS fclExp,
    arrayElement(m.fclLExp, length(m.fclMonth)) AS fclLExp,
    arrayElement(m.fclPExp, length(m.fclMonth)) AS fclPExp,
    arrayElement(m.fclTaxes, length(m.fclMonth)) AS fclTaxes,
    arrayElement(m.fclMExp, length(m.fclMonth)) AS fclMExp,
    arrayElement(m.fclLoss1, length(m.fclMonth)) AS fclLoss,

    arrayElement(m.modMonth, length(m.modMonth)) AS modMonth,
    arrayElement(m.modTLoss1, length(m.modMonth)) AS modTLoss,
    arrayElement(m.modCLoss1, length(m.modMonth)) AS modCLoss,
    arrayElement(m.stepMod1, length(m.modMonth)) AS stepMod,
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
        groupArray(bap) AS bap,

        max(lpd) AS lpd,
        max(defectDt) AS defectDt,
        max(zbDt) AS zbDt,
        max(zbUPB) AS zbUPB,
        max(fileMonthly) AS fileMonthly,

        groupArray(dqDis = 'Y' ? aaa.month : Null) AS dqDis,
        groupArray(if(fclLoss > 1000000.0, Null, aaa.month)) AS fclMonth,
        groupArray(if(fclLoss > 1000000.0, Null, fclProNet)) AS fclProNet,
        groupArray(if(fclLoss > 1000000.0, Null, fclProMi)) AS fclProMi,
        groupArray(if(fclLoss > 1000000.0, Null, fclProMw)) AS fclProMw,
        groupArray(if(fclLoss > 1000000.0, Null, fclExp)) AS fclExp,
        groupArray(if(fclLoss > 1000000.0, Null, fclLExp)) AS fclLExp,
        groupArray(if(fclLoss > 1000000.0, Null, fclPExp)) AS fclPExp,
        groupArray(if(fclLoss > 1000000.0, Null, fclTaxes)) AS fclTaxes,
        groupArray(if(fclLoss > 1000000.0, Null, fclMExp)) AS fclMExp,
        groupArray(if(fclLoss > 1000000.0, Null, fclLoss)) AS fclLoss1,

        groupArray(if(modTLoss >= 0.0 or modCLoss >= 0.0 or stepMod = 'Y' , aaa.month, Null)) AS modMonth,
        groupArray(if(modTLoss >= 0.0 or modCLoss >= 0.0 or stepMod = 'Y' , modTLoss, Null)) AS modTLoss1,
        groupArray(if(modTLoss >= 0.0 or modCLoss >= 0.0 or stepMod = 'Y' , modCLoss, Null)) AS modCLoss1,
        groupArray(if(modTLoss >= 0.0 or modCLoss >= 0.0 or stepMod = 'Y' , stepMod, Null)) AS stepMod1
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
	for _, fd := range srdr.TableSpec().FieldDefs {
		if _, fd1, e := stat.TableDef.Get(fd.Name); e == nil {
			fd.Description = fd1.Description
		}
		if _, fd1, e := mon.TableDef.Get(fd.Name); e == nil {
			fd.Description = fd1.Description
		}
		switch fd.Name {
		case "modMonth":
			fd.Description = "month of modification"
		case "fclMonth":
			fd.Description = "month of foreclosure resolution"
		case "valMonthly":
			fd.ChSpec.OuterFunc = "LowCardinality"
		case "valStatic":
			fd.ChSpec.OuterFunc = "LowCardinality"
		}
	}

	if e := srdr.TableSpec().Nest("monthly", "month", "bap"); e != nil {
		log.Fatalln(e)
	}

	srdr.Name = "ddd"
	if e := srdr.TableSpec().Create(con, srdr.Name); e != nil {
		log.Fatalln(e)
	}
	if e := srdr.Insert(); e != nil {
		log.Fatalln(e)
	}
	fmt.Println("elapsed time", time.Since(start), "total", (48000/117.)*time.Since(start).Hours())

	return nil
}
