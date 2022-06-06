package joined

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/invertedv/chutils"
	"log"
	"strings"
)

func ExampleLoad() {
	var con *chutils.Connect
	con, err := chutils.NewConnect("127.0.0.1", "tester", "testGoNow", clickhouse.Settings{})
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if con.Close() != nil {
			log.Fatalln(err)
		}
	}()
	rows, err := con.Query("DESCRIBE go.freddie")
	if err != nil {
		log.Fatalln(err)
	}
	var name, ftype, defaultType, defaultExpression, comment, n1, n2 string
	for rows.Next() {
		if e := rows.Scan(&name, &ftype, &defaultType, &defaultExpression, &comment, &n1, &n2); e != nil {
			log.Fatalln(e)
		}
		s := strings.TrimRight(fmt.Sprintf("%-20s %-25s %s", name, ftype, comment), " ")
		fmt.Println(s)

	}
	// Output:
	//fico                 Int32                     fico at origination, 301-850, missing=-1
	//fpDt                 Date                      first payment date, missing=1970/1/1
	//firstTime            FixedString(1)            first time homebuyer: Y, N, missing=X
	//matDt                Date                      loan maturity date (initial), missing=1970/1/1
	//msa                  FixedString(5)            msa/division code, missing/not in MSA=XXXXX
	//mi                   Int32                     mi percentage, 0-55, missing=-1
	//units                Int32                     # of units in the property, 1-4, missing=-1
	//occ                  FixedString(1)            property occupancy: P (primary), S (secondary), I (investor), missing=X
	//cltv                 Int32                     combined cltv at origination, 1-998, missing=-1
	//dti                  Int32                     dti at origination, 1-65, missing=-1
	//opb                  Float32                   balance at origination, missing=-1
	//ltv                  Int32                     ltv at origination, 1-998, missing=-1
	//rate                 Float32                   note rate at origination, 0-15, missing=-1
	//channel              FixedString(1)            acquisition channel: B, R, T, C, missing=X
	//pPen                 FixedString(1)            prepay penalty flag: Y, N, missing=X
	//amType               FixedString(3)            amortization type: FRM, ARM, missing=XXX
	//state                FixedString(2)            property state postal abbreviation, missing=XX
	//propType             FixedString(2)            property type: SF (single family), CO (condo), PU (PUD), CP (coop), MH (manufactured), missing=XX
	//zip                  FixedString(5)            3-digit zip (last 2 digits are 00), missing=00000
	//lnId                 String                    Loan ID PYYQnXXXXXXX P=F or A YY=year, n=quarter, missing=error
	//purpose              FixedString(1)            loan purpose: P (purch), C (cash out refi), N (rate/term refi) R (refi), missing=X
	//term                 Int32                     loan term at origination, missing=-1
	//numBorr              Int32                     number of borrowers, 1-10, missing=-1
	//seller               LowCardinality(String)    name of seller, missing=unknown
	//servicer             LowCardinality(String)    name of most recent servicer, missing=unknown
	//sConform             FixedString(1)            super conforming flag: Y, N, missing=X
	//preHARPlnId          String                    for HARP loans, lnId of prior loan, missing=error
	//program              FixedString(1)            freddie program: H (home possible) N (no program), missing=X
	//harp                 FixedString(1)            HARP loan: Y, N, missing=X
	//valMthd              FixedString(1)            property value method 1 (ACE), 2 (Full) 3 (Other), missing=X
	//io                   FixedString(1)            io Flag: Y, N, missing=X
	//qaStatic             LowCardinality(String)    validation results for each field: 0=pass, 1=fail
	//fileStatic           LowCardinality(String)    file static data loaded from
	//vintage              LowCardinality(String)    vintage (from fpDt)
	//propVal              Float32                   property value at origination
	//standard             LowCardinality(String)    standard u/w process loan: Y, N
	//monthly.month        Array(Date)               month of data, missing=1970/1/1
	//monthly.upb          Array(Float32)            unpaid balance, missing=-1
	//monthly.dq           Array(Int32)              months delinquent
	//monthly.reo          Array(String)             reo Y, N
	//monthly.age          Array(Int32)              loan age based on origination date, missing=-1
	//monthly.rTermLgl     Array(Int32)              remaining legal term, missing=-1
	//monthly.mod          Array(FixedString(1))     modification flag: Y, N, P (prior), missing=X
	//monthly.zb           Array(FixedString(2))     zero balance:00 (noop), 01 (pp),02 (3rd party), 03 (short), 96 (repurch), 09 (REO), 15(reperf), missing=X
	//monthly.curRate      Array(Float32)            current note rate, missing=-1
	//monthly.defrl        Array(Float32)            current deferral amount, missing=-1
	//monthly.payPl        Array(FixedString(1))     pay plan: Y, N, P (prior), missing=X
	//monthly.dqDis        Array(FixedString(1))     dq due to disaster: Y, N, missing=X
	//monthly.intUpb       Array(Float32)            interest bearing UPB, missing=-1
	//monthly.accrInt      Array(Float32)            delinquent accrued interest, missing=-1
	//monthly.ageFpDt      Array(Int64)              age based on fdDt, missing=-1000
	//monthly.eLtv         Array(Int32)              estimated LTV based on Freddie AVM, missing=-1
	//monthly.bap          Array(FixedString(1))     borrower assistant plan: F (forebearance), R (repayment), T (trial), N (none), missing=X
	//lpDt                 Date                      last pay date, missing=1970/1/1
	//defectDt             Date                      underwriting defect date, missing=1970/1/1
	//zbDt                 Date                      zero balance date, missing=1970/1/1
	//zbUpb                Float32                   UPB just prior to zero balance, missing=-1
	//fileMonthly          String                    file monthly data loaded from
	//fclMonth             Date                      month of foreclosure resolution
	//fclProNet            Float32                   foreclosure net proceeds, missing=-1
	//fclProMi             Float32                   foreclosure credit enhancement proceeds, missing=-1
	//fclProMw             Float32                   foreclosure make whole proceeds, missing=-1
	//fclExp               Float32                   total foreclosure expenses (values are negative), missing=-1
	//fclLExp              Float32                   foreclosure recovery legal expenses (values are negative), missing=-1
	//fclPExp              Float32                   foreclosure property preservation expenses (values are negative), missing=-1
	//fclTaxes             Float32                   foreclosure property taxes and insurance (values are negative), missing=-1
	//fclMExp              Float32                   foreclosure misc expenses (values are negative), missing=-1
	//fclLoss              Float32                   foreclosure loss amount (a loss is a negative value), missing=-1
	//modTLoss             Float32                   total modification loss, missing=-1
	//mod.modMonth         Array(Date)               month of modification
	//mod.modCLoss         Array(Float32)            current period modification loss, missing=-1
	//mod.stepMod          Array(FixedString(1))     step mod flag: Y, N, missing=X
	//qaMonthly            LowCardinality(String)    validation results for each field: 0=pass, 1=fail
}
