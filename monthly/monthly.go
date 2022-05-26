package monthly

import (
	"fmt"
	"github.com/invertedv/chutils"
	"github.com/invertedv/chutils/file"
	"github.com/invertedv/chutils/nested"
	s "github.com/invertedv/chutils/sql"
	"log"
	"os"
	"strconv"
	"time"
)

var TableDef *chutils.TableDef

// LoadRaw loads the raw monthly series
func LoadRaw(fileN string, table string, create bool, nConcur int, con *chutils.Connect) (err error) {
	fileName = fileN
	if err != nil {
		log.Fatalln(err)
	}

	f, err := os.Open(fileName)
	if err != nil {
		return err
	}
	rdr := file.NewReader(fileName, '|', '\n', '"', 0, 0, 0, f, 6000000)
	rdr.Skip = 0
	defer rdr.Close()
	rdr.SetTableSpec(Build())
	if e := rdr.TableSpec().Check(); e != nil {
		return e
	}

	// build slice of readers
	rdrs, err := file.Rdrs(rdr, nConcur)
	if err != nil {
		return
	}

	var wrtrs []chutils.Output
	if wrtrs, err = s.Wrtrs(table, nConcur, con); err != nil {
		return
	}

	newCalcs := make([]nested.NewCalcFn, 0)
	newCalcs = append(newCalcs, vField, fField, dqField, reoField)

	rdrsn := make([]chutils.Input, 0)
	for j, r := range rdrs {
		defer r.Close()
		defer wrtrs[j].Close()

		rn, e := nested.NewReader(r, xtraFields(), newCalcs)
		if e != nil {
			return e
		}
		rdrsn = append(rdrsn, rn)
		if j == 0 && create {
			if err = rn.TableSpec().Create(con, table); err != nil {
				return err
			}
		}
	}
	TableDef = rdrsn[0].TableSpec()

	err = chutils.Concur(12, rdrsn, wrtrs, 400000)
	return
}

func xtraFields() (fds []*chutils.FieldDef) {
	vfd := &chutils.FieldDef{
		Name:        "qaMonthly",
		ChSpec:      chutils.ChField{Base: chutils.ChString, Funcs: chutils.OuterFuncs{chutils.OuterLowCardinality}},
		Description: "validation results for each field: 0=pass, 1=fail",
		Legal:       chutils.NewLegalValues(),
		Missing:     "!",
		Width:       0,
	}
	ffd := &chutils.FieldDef{
		Name:        "fileMonthly",
		ChSpec:      chutils.ChField{Base: chutils.ChString, Funcs: chutils.OuterFuncs{chutils.OuterLowCardinality}},
		Description: "file monthly data loaded from",
		Legal:       chutils.NewLegalValues(),
		Missing:     "!",
		Width:       0,
	}
	dqfd := &chutils.FieldDef{
		Name:        "dq",
		ChSpec:      chutils.ChField{Base: chutils.ChInt, Length: 32},
		Description: "months delinquent",
		Legal:       &chutils.LegalValues{LowLimit: int32(0), HighLimit: int32(100)},
		Missing:     int32(-1),
		Width:       0,
	}
	reofd := &chutils.FieldDef{
		Name:        "reo",
		ChSpec:      chutils.ChField{Base: chutils.ChString, Funcs: chutils.OuterFuncs{chutils.OuterLowCardinality}},
		Description: "reo Y, N",
		Legal:       chutils.NewLegalValues(),
		Missing:     "!",
		Width:       0,
	}
	fds = []*chutils.FieldDef{vfd, ffd, dqfd, reofd}
	return
}

// gives the validation results for each field -- 0 = pass, 1 = value fail, 2 = type fail
func vField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	res := make([]byte, 0)
	for ind, v := range valid {
		name := td.FieldDefs[ind].Name
		// space is a valid answer, so let's put in a character
		switch name {
		case "mod":
			if data[ind].(string) == "" {
				data[ind] = "N"
			}
		case "stepMod":
			if data[ind].(string) == "" {
				data[ind] = "N"
			}

		}
		switch v {
		case chutils.VPass:
			res = append(res, []byte(name+":0;")...)
		default:
			res = append(res, []byte(name+":1;")...)
		}
	}
	// delete trailing ;
	res[len(res)-1] = ' '
	return string(res), nil
}

// fileName is global since used as a closure to fField
var fileName string

// fField adds the file name data comes from to output table
func fField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	return fileName, nil
}

// dqField adds the delinquency level as an integer
func dqField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	ind, _, err := td.Get("dqStat")
	if err != nil {
		return nil, err
	}
	dq, err := strconv.ParseInt(data[ind].(string), 10, 32)
	if err != nil {
		return -1, nil
	}
	return int32(dq), nil
}

// dqField adds the delinquency level as an integer
func reoField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	ind, _, err := td.Get("dqStat")
	if err != nil {
		return nil, err
	}
	res := "N"
	if data[ind].(string) == "RA" {
		res = "Y"
	}
	return res, nil
}

func Build() *chutils.TableDef {
	var (
		minDt   = time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC)
		maxDt   = time.Now()
		missDt  = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
		strMiss = "X"

		lnIDMiss = "error"

		monthMin, monthMax, monthMiss = minDt, maxDt, missDt

		upbMin, upbMax, upbMiss = float32(0.0), float32(2000000.0), float32(-1.0)

		dqStatMiss = "!"

		dqStatLvl = make([]string, 0)

		ageMin, ageMax, ageMiss                = int32(0), int32(480), int32(-1)
		rTermLglMin, rTermLglMax, rTermLglMiss = int32(0), int32(480), int32(-1)

		defectDtMin, defectDtMax, defectDtMiss = minDt, maxDt, missDt

		modMiss = strMiss
		modLvl  = []string{"Y", "P", "N"}

		zbMiss = strMiss
		zbLvl  = []string{"01", "02", "03", "96", "09", "15"}

		zbDtMin, zbDtMax, zbDtMiss = minDt, maxDt, missDt

		curRateMin, curRateMax, curRateMiss = float32(0.0), float32(15.0), float32(-1.0)
		defrlMin, defrlMax, defrlMiss       = float32(0.0), float32(1000000.0), float32(-1.0)

		lpdMin, lpdMax, lpdMiss = minDt, maxDt, missDt

		fclProMiMin, fclProMiMax, fclProMiMiss    = float32(-2000000.0), float32(2000000.0), float32(0.0)
		fclProNetMin, fclProNetMax, fclProNetMiss = float32(-2000000.0), float32(2000000.0), float32(0.0)
		fclProMwMin, fclProMwMax, fclProMwMiss    = float32(-2000000.0), float32(2000000.0), float32(0.0)

		fclExpMin, fclExpMax, fclExpMiss       = float32(-2000000.0), float32(2000000.0), float32(0.0)
		fclLExpMin, fclLExpMax, fclLExpMiss    = float32(-2000000.0), float32(2000000.0), float32(0.0)
		fclPExpMin, fclPExpMax, fclPExpMiss    = float32(-2000000.0), float32(2000000.0), float32(0.0)
		fclTaxesMin, fclTaxesMax, fclTaxesMiss = float32(-2000000.0), float32(2000000.0), float32(0.0)

		fclMExpMin, fclMExpMax, fclMExpMiss    = float32(-2000000.0), float32(2000000.0), float32(0.0)
		fclLossMin, fclLossMax, fclLossMiss    = float32(-2000000.0), float32(2000000.0), float32(0.0)
		modTLossMin, modTLossMax, modTLossMiss = float32(-2000000.0), float32(2000000.0), float32(0.0)

		stepModMiss = strMiss
		stepModLvl  = []string{"Y", "N", ""}

		payPlMiss = "N"
		payPlLvl  = []string{"Y", "P"}

		eLTVMin, eLTVMax, eLTVMiss          = int32(1), int32(900), int32(-1)
		zbUPBMin, zbUPBMax, zbUPBMiss       = float32(0.0), float32(2000000.0), float32(-1.0)
		accrIntMin, accrIntMax, accrIntMiss = float32(0.0), float32(500000.0), float32(-1.0)

		dqDisMiss = "N"
		dqDisLvl  = []string{"Y", "N"}

		bapMiss = strMiss
		bapLvl  = []string{"F", "R", "T"}

		modCLossMin, modCLossMax, modCLossMiss = float32(-150000.0), float32(150000.0), float32(0.0)
		intUPBMin, intUPBMax, intUPBMiss       = float32(0.0), float32(2000000.0), float32(-1.0)
	)

	for dq := 0; dq < 100; dq++ {
		dqStatLvl = append(dqStatLvl, fmt.Sprintf("%d", dq))
	}
	dqStatLvl = append(dqStatLvl, "RA") // REO Acquisition

	fds := make(map[int]*chutils.FieldDef)

	fd := &chutils.FieldDef{
		Name:        "lnID",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "Loan ID PYYQnXXXXXXX P=F or A YY=year, n=quarter, missing=" + lnIDMiss,
		Legal:       &chutils.LegalValues{},
		Missing:     lnIDMiss,
	}
	fds[0] = fd

	fd = &chutils.FieldDef{
		Name:        "month",
		ChSpec:      chutils.ChField{Base: chutils.ChDate, Length: 0, Format: "200601"},
		Description: "month of data, missing=" + monthMiss.Format("2006/1/2"),
		Legal:       &chutils.LegalValues{LowLimit: monthMin, HighLimit: monthMax},
		Missing:     monthMiss,
	}
	fds[1] = fd

	fd = &chutils.FieldDef{
		Name:        "upb",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "unpaid balance, missing=" + fmt.Sprintf("%v", upbMiss),
		Legal:       &chutils.LegalValues{LowLimit: upbMin, HighLimit: upbMax},
		Missing:     upbMiss,
	}
	fds[2] = fd

	fd = &chutils.FieldDef{
		Name:        "dqStat",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 3},
		Description: "DQ status code: 0-100 months, RA (REO), missing=" + dqStatMiss,
		Legal:       &chutils.LegalValues{Levels: dqStatLvl},
		Missing:     dqStatMiss,
	}
	fds[3] = fd

	fd = &chutils.FieldDef{
		Name:        "age",
		ChSpec:      chutils.ChField{Base: chutils.ChInt, Length: 32},
		Description: "loan age based on origination date" + fmt.Sprintf("%v", ageMiss),
		Legal:       &chutils.LegalValues{LowLimit: ageMin, HighLimit: ageMax},
		Missing:     ageMiss,
	}
	fds[4] = fd

	fd = &chutils.FieldDef{
		Name:        "rTermLgl",
		ChSpec:      chutils.ChField{Base: chutils.ChInt, Length: 32},
		Description: "remaining legal term, missing=" + fmt.Sprintf("%v", rTermLglMiss),
		Legal:       &chutils.LegalValues{LowLimit: rTermLglMin, HighLimit: rTermLglMax},
		Missing:     rTermLglMiss,
	}
	fds[5] = fd

	fd = &chutils.FieldDef{
		Name:        "defectDt",
		ChSpec:      chutils.ChField{Base: chutils.ChDate, Format: "200601"},
		Description: "underwriting defect date, missing=" + defectDtMiss.Format("2006/1/2"),
		Legal:       &chutils.LegalValues{LowLimit: defectDtMin, HighLimit: defectDtMax},
		Missing:     defectDtMiss,
	}
	fds[6] = fd

	fd = &chutils.FieldDef{
		Name:        "mod",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 1},
		Description: "modification flag: Y, N, P (prior), missing=" + modMiss,
		Legal:       &chutils.LegalValues{Levels: modLvl},
		Missing:     modMiss,
	}
	fds[7] = fd

	fd = &chutils.FieldDef{
		Name:        "zb",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 2},
		Description: "zero balance: 01 (pp),02 (3rd party), 03 (short), 96 (repurch), 09 (REO), 15(reperf), missing=" + zbMiss,
		Legal:       &chutils.LegalValues{Levels: zbLvl},
		Missing:     zbMiss}
	fds[8] = fd

	fd = &chutils.FieldDef{
		Name:        "zbDt",
		ChSpec:      chutils.ChField{Base: chutils.ChDate, Format: "200601"},
		Description: "zero balance date, missing=" + zbDtMiss.Format("2006/1/2"),
		Legal:       &chutils.LegalValues{LowLimit: zbDtMin, HighLimit: zbDtMax},
		Missing:     zbDtMiss,
	}
	fds[9] = fd

	fd = &chutils.FieldDef{
		Name:        "curRate",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "current note rate, missing=" + fmt.Sprintf("%v", curRateMiss),
		Legal:       &chutils.LegalValues{LowLimit: curRateMin, HighLimit: curRateMax},
		Missing:     curRateMiss,
	}
	fds[10] = fd

	fd = &chutils.FieldDef{
		Name:        "defrl",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "current deferral amount, missing=" + fmt.Sprintf("%v", defrlMiss),
		Legal:       &chutils.LegalValues{LowLimit: defrlMin, HighLimit: defrlMax},
		Missing:     defrlMiss,
	}
	fds[11] = fd

	fd = &chutils.FieldDef{
		Name:        "lpd",
		ChSpec:      chutils.ChField{Base: chutils.ChDate, Format: "200601"},
		Description: "last pay date, missing=" + lpdMiss.Format("2006/1/2"),
		Legal:       &chutils.LegalValues{LowLimit: lpdMin, HighLimit: lpdMax},
		Missing:     lpdMiss,
	}
	fds[12] = fd

	fd = &chutils.FieldDef{
		Name:        "fclProMi",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "foreclosure credit enhancement proceeds, missing=" + fmt.Sprintf("%v", fclProMiMiss),
		Legal:       &chutils.LegalValues{LowLimit: fclProMiMin, HighLimit: fclProMiMax},
		Missing:     fclProMiMiss,
	}
	fds[13] = fd

	fd = &chutils.FieldDef{
		Name:        "fclProNet",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "foreclosure net proceeds, missing=" + fmt.Sprintf("%v", fclProNetMiss),
		Legal:       &chutils.LegalValues{LowLimit: fclProNetMin, HighLimit: fclProNetMax},
		Missing:     fclProNetMiss,
	}
	fds[14] = fd

	fd = &chutils.FieldDef{
		Name:        "fclProMw",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "foreclosure make whole proceeds, missing=" + fmt.Sprintf("%v", fclProMwMiss),
		Legal:       &chutils.LegalValues{LowLimit: fclProMwMin, HighLimit: fclProMwMax},
		Missing:     fclProMwMiss,
	}
	fds[15] = fd

	fd = &chutils.FieldDef{
		Name:        "fclExp",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "total foreclosure expenses (values are negative), missing=" + fmt.Sprintf("%v", fclExpMiss),
		Legal:       &chutils.LegalValues{LowLimit: fclExpMin, HighLimit: fclExpMax},
		Missing:     fclExpMiss,
	}
	fds[16] = fd

	fd = &chutils.FieldDef{
		Name:        "fclLExp",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "foreclosure recovery legal expenses (values are negative), missing=" + fmt.Sprintf("%v", fclLExpMiss),
		Legal:       &chutils.LegalValues{LowLimit: fclLExpMin, HighLimit: fclLExpMax},
		Missing:     fclLExpMiss,
	}
	fds[17] = fd

	fd = &chutils.FieldDef{
		Name:        "fclPExp",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "foreclosure property preservation expenses (values are negative), missing=" + fmt.Sprintf("%v", fclPExpMiss),
		Legal:       &chutils.LegalValues{LowLimit: fclPExpMin, HighLimit: fclPExpMax},
		Missing:     fclPExpMiss,
	}
	fds[18] = fd

	fd = &chutils.FieldDef{
		Name:        "fclTaxes",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "foreclosure property taxes and insurance (values are negative), missing=" + fmt.Sprintf("%v", fclTaxesMiss),
		Legal:       &chutils.LegalValues{LowLimit: fclTaxesMin, HighLimit: fclTaxesMax},
		Missing:     fclTaxesMiss,
	}
	fds[19] = fd

	fd = &chutils.FieldDef{
		Name:        "fclMExp",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "foreclosure misc expenses (values are negative), missing=" + fmt.Sprintf("%v", fclMExpMiss),
		Legal:       &chutils.LegalValues{LowLimit: fclMExpMin, HighLimit: fclMExpMax},
		Missing:     fclMExpMiss,
	}
	fds[20] = fd

	fd = &chutils.FieldDef{
		Name:        "fclLoss",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "foreclosure loss amount (a loss is a negative value), missing=" + fmt.Sprintf("%v", fclLossMiss),
		Legal:       &chutils.LegalValues{LowLimit: fclLossMin, HighLimit: fclLossMax},
		Missing:     fclLossMiss,
	}
	fds[21] = fd

	fd = &chutils.FieldDef{
		Name:        "modTLoss",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "total modification loss, missing=" + fmt.Sprintf("%v", modTLossMiss),
		Legal:       &chutils.LegalValues{LowLimit: modTLossMin, HighLimit: modTLossMax},
		Missing:     modTLossMiss,
	}
	fds[22] = fd

	fd = &chutils.FieldDef{
		Name:        "stepMod",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 1},
		Description: "step mod flag: Y, N, missing=" + stepModMiss,
		Legal:       &chutils.LegalValues{Levels: stepModLvl},
		Missing:     stepModMiss,
	}
	fds[23] = fd

	fd = &chutils.FieldDef{
		Name:        "payPl",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 1},
		Description: "pay plan: Y, N, P (prior), missing=" + payPlMiss,
		Legal:       &chutils.LegalValues{Levels: payPlLvl},
		Missing:     payPlMiss,
	}
	fds[24] = fd

	fd = &chutils.FieldDef{
		Name:        "eLTV",
		ChSpec:      chutils.ChField{Base: chutils.ChInt, Length: 32},
		Description: "estimated LTV based on Freddie AVM, missing=" + fmt.Sprintf("%v", eLTVMiss),
		Legal:       &chutils.LegalValues{LowLimit: eLTVMin, HighLimit: eLTVMax},
		Missing:     eLTVMiss,
	}
	fds[25] = fd

	fd = &chutils.FieldDef{
		Name:        "zbUPB",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "UPB just prior to zero balance, missing=" + fmt.Sprintf("%v", zbUPBMiss),
		Legal:       &chutils.LegalValues{LowLimit: zbUPBMin, HighLimit: zbUPBMax},
		Missing:     zbUPBMiss,
	}
	fds[26] = fd

	fd = &chutils.FieldDef{
		Name:        "accrInt",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "delinquent accrued interest, missing=" + fmt.Sprintf("%v", accrIntMiss),
		Legal:       &chutils.LegalValues{LowLimit: accrIntMin, HighLimit: accrIntMax},
		Missing:     accrIntMiss,
	}
	fds[27] = fd

	fd = &chutils.FieldDef{
		Name:        "dqDis",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 1},
		Description: "dq due to disaster: Y, N, missing=" + dqDisMiss,
		Legal:       &chutils.LegalValues{Levels: dqDisLvl},
		Missing:     dqDisMiss,
	}
	fds[28] = fd

	fd = &chutils.FieldDef{
		Name:        "bap",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 1},
		Description: "borrower assistant plan: F (forebearance), R (repayment), T (trial), missing=" + bapMiss,
		Legal:       &chutils.LegalValues{Levels: bapLvl},
		Missing:     bapMiss,
	}
	fds[29] = fd

	fd = &chutils.FieldDef{
		Name:        "modCLoss",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "current period modification loss, missing=" + fmt.Sprintf("%v", modCLossMiss),
		Legal:       &chutils.LegalValues{LowLimit: modCLossMin, HighLimit: modCLossMax},
		Missing:     modCLossMiss,
	}
	fds[30] = fd

	fd = &chutils.FieldDef{
		Name:        "intUPB",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "interest bearing UPB, missing=" + fmt.Sprintf("%v", intUPBMiss),
		Legal:       &chutils.LegalValues{LowLimit: intUPBMin, HighLimit: intUPBMax},
		Missing:     intUPBMiss,
	}
	fds[31] = fd
	return chutils.NewTableDef("lnID, month", chutils.MergeTree, fds)
}
