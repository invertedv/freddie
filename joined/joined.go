package joined

import (
	"github.com/invertedv/chutils"
	s "github.com/invertedv/chutils/sql"
	mon "github.com/invertedv/freddie/monthly"
	stat "github.com/invertedv/freddie/static"
	"log"
	"strings"
)

func Load(monthly string, static string, table string, tmpDB string, create bool, con *chutils.Connect) error {
	tmpStatic := tmpDB + ".static"
	tmpMonthly := tmpDB + ".monthly"
	if err := stat.LoadRaw(static, tmpStatic, true, con); err != nil {
		log.Fatalln(err)
	}
	if err := mon.LoadRaw(monthly, tmpMonthly, true, 12, con); err != nil {
		log.Fatalln(err)
	}

	qryUse := strings.Replace(strings.Replace(qry, "tmpMonthly", tmpMonthly, -1), "tmpStatic", tmpStatic, -1)
	srdr := s.NewReader(qryUse, con)
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
		case "qaMonthly":
			fd.ChSpec.Funcs = chutils.OuterFuncs{chutils.OuterLowCardinality}
		case "valStatic":
			fd.ChSpec.Funcs = chutils.OuterFuncs{chutils.OuterLowCardinality}
		}
	}

	if e := srdr.TableSpec().Nest("monthly", "month", "bap"); e != nil {
		log.Fatalln(e)
	}

	if e := srdr.TableSpec().Nest("mod", "modMonth", "stepMod"); e != nil {
		log.Fatalln(e)
	}

	srdr.Name = table
	if create {
		if e := srdr.TableSpec().Create(con, srdr.Name); e != nil {
			log.Fatalln(e)
		}
	}
	if e := srdr.Insert(); e != nil {
		log.Fatalln(e)
	}

	return nil
}

const qry = `
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
                arrayJoin(splitByChar(';', qaMonthly)) AS v
            FROM
                tmpMonthly))
     GROUP BY lnID, field
     ORDER BY lnID, field)
GROUP BY lnID))
SELECT
    s.*,
    m.month,
    m.upb,
//    m.dqStat,
    m.dq,
    m.reo,
    m.age,
    m.rTermLgl,
    m.mod,
    m.zb,
    m.curRate,
    m.defrl,
    m.payPl,
    m.dqDis,
    m.intUPB,
    m.accrInt,
    m.bap,

    m.lpDt,
    m.defectDt,
    m.zbDt,
    m.zbUpb,
    m.fileMonthly,

    arrayElement(m.fclMonth, length(m.fclMonth)) AS fclMonth,
    arrayElement(m.fclProNet1, length(m.fclMonth)) AS fclProNet,
    arrayElement(m.fclProMi1, length(m.fclMonth)) AS fclProMi,
    arrayElement(m.fclProMw1, length(m.fclMonth)) AS fclProMw,
    arrayElement(m.fclExp1, length(m.fclMonth)) AS fclExp,
    arrayElement(m.fclLExp1, length(m.fclMonth)) AS fclLExp,
    arrayElement(m.fclPExp1, length(m.fclMonth)) AS fclPExp,
    arrayElement(m.fclTaxes1, length(m.fclMonth)) AS fclTaxes,
    arrayElement(m.fclMExp1, length(m.fclMonth)) AS fclMExp,
    arrayElement(m.fclLoss1, length(m.fclMonth)) AS fclLoss,

    arrayElement(m.modTLoss1, length(m.modMonth)) AS modTLoss,
    m.modMonth,
    m.modCLoss1 AS modCLoss,
    m.stepMod1 AS stepMod,
    v.x AS qaMonthly
FROM
    tmpStatic AS s
JOIN (
    SELECT
        lnID,
        groupArray(month) AS month,
        groupArray(upb) AS upb,
        groupArray(dqStat) AS dqStat,
        groupArray(dq) AS dq,
        groupArray(reo) AS reo,
        groupArray(age) AS age,
        groupArray(rTermLgl) AS rTermLgl,
        groupArray(mod) AS mod,
        groupArray(zb) AS zb,
        groupArray(curRate) AS curRate,
        groupArray(defrl) AS defrl,
        groupArray(intUPB) AS intUPB,
        groupArray(dqDis) AS dqDis,
        groupArray(payPl) AS payPl,
        groupArray(accrInt) AS accrInt,
        groupArray(bap) AS bap,

        max(lpDt) AS lpDt,
        max(defectDt) AS defectDt,
        max(zbDt) AS zbDt,
        max(zbUpb) AS zbUpb,
        max(fileMonthly) AS fileMonthly,

//        groupArray(dqDis = 'Y' ? aaa.month : Null) AS dqDis,
        groupArray(if(abs(fclLoss) + abs(fclExp) + abs(fclProNet+fclProMi+fclProMw) = 0.0 , Null, aaa.month)) AS fclMonth,
        groupArray(if(abs(fclLoss) + abs(fclExp) + abs(fclProNet+fclProMi+fclProMw) = 0.0, Null, fclProNet)) AS fclProNet1,
        groupArray(if(abs(fclLoss) + abs(fclExp) + abs(fclProNet+fclProMi+fclProMw) = 0.0, Null, fclProMi)) AS fclProMi1,
        groupArray(if(abs(fclLoss) + abs(fclExp) + abs(fclProNet+fclProMi+fclProMw) = 0.0, Null, fclProMw)) AS fclProMw1,
        groupArray(if(abs(fclLoss) + abs(fclExp) + abs(fclProNet+fclProMi+fclProMw) = 0.0, Null, fclExp)) AS fclExp1,
        groupArray(if(abs(fclLoss) + abs(fclExp) + abs(fclProNet+fclProMi+fclProMw) = 0.0, Null, fclLExp)) AS fclLExp1,
        groupArray(if(abs(fclLoss) + abs(fclExp) + abs(fclProNet+fclProMi+fclProMw) = 0.0, Null, fclPExp)) AS fclPExp1,
        groupArray(if(abs(fclLoss) + abs(fclExp) + abs(fclProNet+fclProMi+fclProMw) = 0.0, Null, fclTaxes)) AS fclTaxes1,
        groupArray(if(abs(fclLoss) + abs(fclExp) + abs(fclProNet+fclProMi+fclProMw) = 0.0, Null, fclMExp)) AS fclMExp1,
        groupArray(if(abs(fclLoss) + abs(fclExp) + abs(fclProNet+fclProMi+fclProMw) = 0.0, Null, fclLoss)) AS fclLoss1,

        groupArray(if(modTLoss != 0.0 or modCLoss != 0.0 or stepMod = 'Y' , aaa.month, Null)) AS modMonth,
        groupArray(if(modTLoss != 0.0 or modCLoss != 0.0 or stepMod = 'Y' , modTLoss, Null)) AS modTLoss1,
        groupArray(if(modTLoss != 0.0 or modCLoss != 0.0 or stepMod = 'Y' , modCLoss, Null)) AS modCLoss1,
        groupArray(if(modTLoss != 0.0 or modCLoss != 0.0 or stepMod = 'Y' , stepMod, Null)) AS stepMod1
    FROM
       (SELECT * FROM
        tmpMonthly ORDER BY lnID, month) AS aaa
    GROUP BY lnID) AS m
ON s.lnID = m.lnID
JOIN v
ON s.lnID = v.lnID
`

//TODO - change defers to funcs with _ = xyz.Close() to get it to stop complaining
