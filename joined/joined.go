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
		case "bucket":
			fd.Description = "loan bucket"
			//			fd.ChSpec.Base, fd.ChSpec.Length = chutils.ChInt, 32
		case "zip3":
			fd.Description = "3 digit zip"
			fd.ChSpec.Base, fd.ChSpec.Length = chutils.ChFixedString, 3
		case "modMonth":
			fd.Description = "month of modification"
		case "fclMonth":
			fd.Description = "month of foreclosure resolution"
		case "ageFpDt":
			fd.Description = "age based on fdDt, missing=-1000"
		case "standard":
			fd.Description = "standard u/w process loan: Y, N"
		case "field":
			fd.ChSpec.Funcs = append(fd.ChSpec.Funcs, chutils.OuterLowCardinality)
			fd.Description = "failed qa: field name array"
		case "cntFail":
			fd.Description = "# months that failed qa"
		case "allFail":
			fd.ChSpec.Funcs = append(fd.ChSpec.Funcs, chutils.OuterLowCardinality)
			fd.Description = "fields that failed qa all months"
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
	if e := srdr.TableSpec().Nest("qa", "field", "cntFail"); e != nil {
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
WITH qMonthly AS (
    SELECT 
        lnId, 
        groupArray(grp) AS qa,
        groupArray(n) AS nqa
    FROM (
        SELECT 
            lnId, 
            arrayJoin(splitByChar(':', qaMonthly)) AS grp,
            toInt32(count(*)) AS n
        FROM tmpMonthly 
        WHERE grp != ''
        GROUP BY lnId, grp)
    GROUP BY lnId),
qStatic AS (
    SELECT 
        lnId, 
        groupArray(grp) AS qa,
        groupArray(n) AS nqa
    FROM (
        SELECT 
            lnId, 
            arrayJoin(splitByChar(':', qaStatic)) AS grp,
            toInt32(count(*)) AS n
        FROM tmpStatic 
        WHERE grp != ''
        GROUP BY lnId, grp)
    GROUP BY lnId)
SELECT
    fico,
    fpDt,
    firstTime,
    matDt,
    msaD,
    mi,
    units,
    occ,
    cltv,
    dti,
    opb,
    ltv,
    rate,
    channel,
    pPen,
    amType,
    state,
    propType,
    substr(zip, 1, 3) AS zip3,
    s.lnId,
    toInt32(modulo(arraySum(bitPositionsToArray(reinterpretAsUInt64(substr(s.lnId, 5, 8)))), 20)) AS bucket,
    purpose,
    term,
    numBorr,
    seller,
    servicer,
    sConform,
    preHarpLnId,
    program,
    harp,
    valMthd,
    io,
    fileStatic,
    vintage,
    propVal,
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
    arrayConcat(qStatic.qa, qMonthly.qa) AS field,
    arrayConcat(qStatic.nqa, qMonthly.nqa) AS cntFail,
    arrayConcat(qStatic.qa,
         arrayFilter((x,y) -> y=length(month) ? 1 : 0, qMonthly.qa, qMonthly.nqa)) AS allFail
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
LEFT JOIN qMonthly 
ON s.lnId = qMonthly.lnId
LEFT JOIN qStatic
ON s.lnId = qStatic.lnId
`
