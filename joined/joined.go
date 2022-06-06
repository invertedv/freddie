// Package joined joins the static and monthly tables created by the static and monthly packages
package joined

import (
	"fmt"
	"github.com/invertedv/chutils"
	s "github.com/invertedv/chutils/sql"
	mon "github.com/invertedv/freddie/monthly"
	stat "github.com/invertedv/freddie/static"
	"strings"
)

// func Load loads the monthly and static files into tmpDB.monthly & tmpDB.static, then joins them and inserts
// the output into "table".  If create="Y", table is created/reset.  The monthly file is read/loaded using
// nConcur processes.
func Load(monthly string, static string, table string, tmpDB string, create bool, nConcur int, con *chutils.Connect) error {
	// load static data into temp table
	tmpStatic := tmpDB + ".static"
	if e := stat.LoadRaw(static, tmpStatic, true, con); e != nil {
		return e
	}
	// load monthly data into temp table
	tmpMonthly := tmpDB + ".monthly"
	if e := mon.LoadRaw(monthly, tmpMonthly, true, nConcur, con); e != nil {
		return e
	}

	// fill in placeholders in the JOIN query
	qryUse := strings.Replace(strings.Replace(qry, "tmpMonthly", tmpMonthly, -1), "tmpStatic", tmpStatic, -1)

	// build sql reader
	srdr := s.NewReader(qryUse, con)
	// initialize the TableDef
	if e := srdr.Init("lnId", chutils.MergeTree); e != nil {
		return e
	}
	// fill in the descriptions of the fields
	for _, fd := range srdr.TableSpec().FieldDefs {
		if _, fd1, e := stat.TableDef.Get(fd.Name); e == nil {
			fd.Description = fd1.Description
		}
		if _, fd1, e := mon.TableDef.Get(fd.Name); e == nil {
			fd.Description = fd1.Description
		}
		// new fields
		switch fd.Name {
		case "modMonth":
			fd.Description = "month of modification"
		case "fclMonth":
			fd.Description = "month of foreclosure resolution"
		case "ageFpDt":
			fd.Description = "age based on fdDt, missing=-1000"
		case "standard":
			fd.Description = "standard u/w process loan: Y, N"
		case "qaMonthly":
			fd.ChSpec.Funcs = chutils.OuterFuncs{chutils.OuterLowCardinality}
		case "valStatic":
			fd.ChSpec.Funcs = chutils.OuterFuncs{chutils.OuterLowCardinality}
		}
	}
	// Nested arrays for the monthly data
	if e := srdr.TableSpec().Nest("monthly", "month", "bap"); e != nil {
		return e
	}
	// Nested arrays for modifications data
	if e := srdr.TableSpec().Nest("mod", "modMonth", "stepMod"); e != nil {
		return e
	}

	srdr.Name = table
	if create {
		if e := srdr.TableSpec().Create(con, srdr.Name); e != nil {
			return e
		}
	}
	// Insert the data into the table
	if e := srdr.Insert(); e != nil {
		return e
	}

	// clean up
	if _, e := con.Exec(fmt.Sprintf("DROP TABLE %s", tmpStatic)); e != nil {
		return e
	}
	if _, e := con.Exec(fmt.Sprintf("DROP TABLE %s", tmpMonthly)); e != nil {
		return e
	}

	return nil
}

// qry is the query that does the join
const qry = `
WITH v AS (
SELECT
    lnId,
    arrayStringConcat(arrayMap(x,y -> concat(x, ':', toString(y)), field, validation), ';') as x
FROM ( 
SELECT
    lnId,
    groupArray(f) AS field,
    groupArray(v) AS validation
FROM(    
    SELECT
      lnId,
      field AS f,
      Max(valid) AS v
    FROM (
        SELECT
            lnId,
            arrayElement(splitByChar(':', v), 1) AS field,
            substr(arrayElement(splitByChar(':', v), 2), 1, 1) = '0' ? 0 : 1 AS valid
        FROM (    
            SELECT
                lnId,
                arrayJoin(splitByChar(';', qaMonthly)) AS v
            FROM
                tmpMonthly))
     GROUP BY lnId, field
     ORDER BY lnId, field)
GROUP BY lnId))
SELECT
    s.*,
    position(fileStatic, 'excl') = 0 ? 'Y' : 'N' AS standard,
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
    m.intUpb,
    m.accrInt,
    arrayMap(x->year(fpDt) > 1990 ? dateDiff('month', s.fpDt, x) + 1: -1000, m.month) AS ageFpDt,
    m.eLtv,
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
        lnId,
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
        groupArray(intUpb) AS intUpb,
        groupArray(dqDis) AS dqDis,
        groupArray(payPl) AS payPl,
        groupArray(accrInt) AS accrInt,
        groupArray(eLtv) AS eLtv,
        groupArray(bap) AS bap,

        max(lpDt) AS lpDt,
        max(defectDt) AS defectDt,
        max(zbDt) AS zbDt,
        max(zbUpb) AS zbUpb,
        max(fileMonthly) AS fileMonthly,

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
        tmpMonthly ORDER BY lnId, month) AS aaa
    GROUP BY lnId) AS m
ON s.lnId = m.lnId
JOIN v
ON s.lnId = v.lnId
`
