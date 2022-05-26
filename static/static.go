package static

import (
	"fmt"
	"github.com/invertedv/chutils"
	"github.com/invertedv/chutils/file"
	"github.com/invertedv/chutils/nested"
	s "github.com/invertedv/chutils/sql"
	"os"
	"time"
)

// TableDef is the *chutils.TableDef of the nested reader
var TableDef *chutils.TableDef

// LoadRaw loads the raw monthly series
func LoadRaw(filen string, table string, create bool, con *chutils.Connect) (err error) {
	fileName = filen

	// build initial reader
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

	newCalcs := make([]nested.NewCalcFn, 0)
	newCalcs = append(newCalcs, vField, fField, vintField, pvField)

	nrdr, err := nested.NewReader(rdr, xtraFields(), newCalcs)
	TableDef = nrdr.TableSpec()

	if create {
		if err = nrdr.TableSpec().Create(con, table); err != nil {
			return err
		}
	}

	wrtr := s.NewWriter(table, con)
	if err = chutils.Export(nrdr, wrtr, 400000); err != nil {
		return
	}
	return nil
}

// gives the validation results for each field -- 0 = pass, 1 = value fail, 2 = type fail
func vField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {

	res := make([]byte, 0)
	for ind, v := range valid {
		name := td.FieldDefs[ind].Name
		// There are a few fields where <space> is a valid answer.  Let's replace that with a character.
		switch v {
		case chutils.VPass, chutils.VDefault:
			res = append(res, []byte(name+":0;")...)
		default:
			res = append(res, []byte(name+":1;")...)
		}
	}
	// delete trailing ;
	res[len(res)-1] = ' '
	return string(res), nil
}

// fField adds the file name data comes from to output table
func vintField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	ind, _, err := td.Get("fpDt")
	if err != nil {
		return nil, err
	}
	fpd := data[ind].(time.Time)
	var qtr int = int((fpd.Month()-1)/3 + 1)
	vintage := fmt.Sprintf("%dQ%d", fpd.Year(), qtr)
	return vintage, nil
}

// fileName is global since used as a closure to fField
var fileName string

// fField adds the file name data comes from to output table
func fField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	return fileName, nil
}

// calculate property value from ltv
func pvField(td *chutils.TableDef, data chutils.Row, valid chutils.Valid, validate bool) (interface{}, error) {
	ltvInd, _, err := td.Get("ltv")
	if err != nil {
		return nil, err
	}
	if valid[ltvInd] != chutils.VPass {
		return -1.0, nil
	}
	ltv := float32(data[ltvInd].(int32))

	opbInd, _, err := td.Get("opb")
	if valid[opbInd] != chutils.VPass {
		return -1.0, nil
	}
	if err != nil {
		return nil, err
	}
	opb := data[opbInd].(float32)
	return opb / (ltv / 100.0), nil
}

// xtraFields defines extra fields for the nested reader
func xtraFields() (fds []*chutils.FieldDef) {
	vfd := &chutils.FieldDef{
		Name:        "qaStatic",
		ChSpec:      chutils.ChField{Base: chutils.ChString, Funcs: chutils.OuterFuncs{chutils.OuterLowCardinality}},
		Description: "validation results for each field: 0=pass, 1=fail",
		Legal:       chutils.NewLegalValues(),
		Missing:     "!",
		Width:       0,
	}
	ffd := &chutils.FieldDef{
		Name:        "fileStatic",
		ChSpec:      chutils.ChField{Base: chutils.ChString, Funcs: chutils.OuterFuncs{chutils.OuterLowCardinality}},
		Description: "file static data loaded from",
		Legal:       chutils.NewLegalValues(),
		Missing:     "!",
		Width:       0,
	}
	vintfd := &chutils.FieldDef{
		Name:        "vintage",
		ChSpec:      chutils.ChField{Base: chutils.ChString, Funcs: chutils.OuterFuncs{chutils.OuterLowCardinality}},
		Description: "vintage (from fpDt)",
		Legal:       chutils.NewLegalValues(),
		Missing:     "!",
		Width:       0,
	}
	pvfd := &chutils.FieldDef{
		Name:        "propVal",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "property value at origination",
		Legal:       &chutils.LegalValues{LowLimit: float32(1000.0), HighLimit: float32(5000000.0), Levels: nil},
		Missing:     float32(-1.0),
		Width:       0,
	}
	fds = []*chutils.FieldDef{vfd, ffd, vintfd, pvfd}
	return
}

// Build builds the TableDef for the static field files
func Build() *chutils.TableDef {
	var (
		// date ranges & missing value
		minDt  = time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC)
		nowDt  = time.Now()
		futDt  = time.Now().AddDate(40, 0, 0)
		missDt = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

		strMiss = "X" // generic missing value for FixedString(1)

		fpDtMin, fpDtMax, fpDtMiss = minDt, nowDt, missDt
		ficoMin, ficoMax, ficoMiss = int32(301), int32(850), int32(-1)

		firstTimeMiss = strMiss
		firstTimeLvl  = []string{"Y", "N"}

		matDtMin, matDtMax, matDtMiss = minDt, futDt, missDt

		msaMiss = "XXXXX"
		msaDef  = "00000"
		msaLvl  = []string{"10180", "10380", "10420", "10500", "10540", "10580", "10740", "10780", "10900", "11020", "11100", "11180",
			"11244", "11260", "11300", "11340", "11460", "11500", "11540", "11640", "11700", "12020", "12060", "12100", "12220",
			"12260", "12420", "12540", "12580", "12620", "12700", "12940", "12980", "13020", "13140", "13220", "13380", "13460",
			"13644", "13740", "13780", "13820", "13900", "13980", "14010", "14020", "14060", "14100", "14260", "14454", "14484",
			"14500", "14540", "14740", "14860", "15180", "15260", "15380", "15500", "15540", "15680", "15764", "15804", "15940",
			"15980", "16020", "16060", "16180", "16220", "16300", "16540", "16580", "16620", "16700", "16740", "16820", "16860",
			"16940", "16974", "16984", "17020", "17140", "17300", "17420", "17460", "17660", "17780", "17820", "17860", "17900",
			"17980", "18020", "18140", "18580", "18700", "18880", "19060", "19124", "19140", "19180", "19260", "19300", "19340",
			"19380", "19430", "19460", "19500", "19660", "19740", "19780", "19804", "20020", "20100", "20220", "20260", "20500",
			"20524", "20700", "20740", "20764", "20940", "20994", "21060", "21140", "21300", "21340", "21420", "21500", "21660",
			"21780", "21820", "21940", "22020", "22140", "22180", "22220", "22380", "22420", "22500", "22520", "22540", "22660",
			"22744", "22900", "23060", "23104", "23224", "23420", "23460", "23540", "23580", "23844", "23900", "24020", "24140",
			"24220", "24260", "24300", "24340", "24420", "24500", "24540", "24580", "24660", "24780", "24860", "25020", "25060",
			"25180", "25220", "25260", "25420", "25500", "25540", "25620", "25860", "25940", "25980", "26100", "26140", "26180",
			"26300", "26380", "26420", "26580", "26620", "26820", "26900", "26980", "27060", "27100", "27140", "27180", "27260",
			"27340", "27500", "27620", "27740", "27780", "27860", "27900", "27980", "28020", "28100", "28140", "28420", "28660",
			"28700", "28740", "28940", "29020", "29100", "29140", "29180", "29200", "29340", "29404", "29420", "29460", "29540",
			"29620", "29700", "29740", "29820", "29940", "30020", "30140", "30300", "30340", "30460", "30620", "30700", "30780",
			"30860", "30980", "31020", "31084", "31140", "31180", "31340", "31420", "31460", "31540", "31700", "31740", "31860",
			"31900", "32420", "32580", "32780", "32820", "32900", "33124", "33140", "33220", "33260", "33340", "33460", "33540",
			"33660", "33700", "33740", "33780", "33860", "33874", "34060", "34100", "34580", "34620", "34740", "34820", "34900",
			"34940", "34980", "35004", "35084", "35100", "35154", "35300", "35380", "35614", "35644", "35660", "35840", "35980",
			"36084", "36100", "36140", "36220", "36260", "36420", "36500", "36540", "36740", "36780", "36980", "37100", "37340",
			"37380", "37460", "37620", "37700", "37764", "37860", "37900", "37964", "38060", "38220", "38300", "38340", "38540",
			"38660", "38860", "38900", "38940", "39100", "39140", "39150", "39300", "39340", "39380", "39460", "39540", "39580",
			"39660", "39740", "39820", "39900", "40060", "40140", "40220", "40340", "40380", "40420", "40484", "40580", "40660",
			"40900", "40980", "41060", "41100", "41140", "41180", "41420", "41500", "41540", "41620", "41660", "41700", "41740",
			"41780", "41884", "41900", "41940", "41980", "42020", "42034", "42044", "42060", "42100", "42140", "42200", "42220",
			"42340", "42540", "42644", "42680", "42700", "43100", "43300", "43340", "43420", "43524", "43580", "43620", "43780",
			"43900", "44060", "44100", "44140", "44180", "44220", "44300", "44420", "44600", "44700", "44940", "45060", "45104",
			"45220", "45300", "45460", "45500", "45540", "45780", "45820", "45940", "46060", "46140", "46220", "46300", "46340",
			"46520", "46540", "46660", "46700", "47020", "47220", "47260", "47300", "47380", "47460", "47580", "47644", "47664",
			"47894", "47940", "48060", "48140", "48260", "48300", "48424", "48540", "48620", "48660", "48700", "48864", "48900",
			"49020", "49180", "49340", "49420", "49500", "49620", "49660", "49700", "49740", msaDef}

		miMin, miMax, miMiss       = int32(0), int32(55), int32(-1)
		unitMin, unitMax, unitMiss = int32(1), int32(4), int32(-1)

		occMiss = strMiss
		occLvl  = []string{"P", "S", "I"}

		cltvMin, cltvMax, cltvMiss = int32(1), int32(998), int32(-1)
		dtiMin, dtiMax, dtiMiss    = int32(1), int32(65), int32(-1)
		opbMin, opbMax, opbMiss    = float32(1000.0), float32(2000000.0), float32(-1.0)
		ltvMin, ltvMax, ltvMiss    = int32(1), int32(998), int32(-1)
		rateMin, rateMax, rateMiss = float32(0.0), float32(15.0), float32(-1.0)

		channelMiss = strMiss
		channelLvl  = []string{"B", "R", "T", "C"}

		ppenMiss = strMiss
		pPenLvl  = []string{"Y", "N"}

		amTypeMiss = "XXX"
		amTypeLvl  = []string{"FRM", "ARM"}

		stateMiss = "XX"
		stateLvl  = []string{"AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "GU", "HI", "IA", "ID",
			"IL", "IN", "KS", "KY", "LA", "MA", "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NC", "ND",
			"NE", "NH", "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "PR", "RI", "SC", "SD", "TN",
			"TX", "UT", "VA", "VI", "VT", "WA", "WI", "WV", "WY"}

		propTypeMiss = "XX"
		propTypeLvl  = []string{"SF", "CO", "CP", "MH", "PU"}

		zipMiss = "00000"
		zipLvl  = []string{"00500", "00600", "00700", "00800", "00900", "01000", "01100", "01200", "01300", "01400", "01500", "01600",
			"01700", "01800", "01900", "02000", "02100", "02200", "02300", "02400", "02500", "02600", "02700", "02800",
			"02900", "03000", "03100", "03200", "03300", "03400", "03500", "03600", "03700", "03800", "03900", "04000",
			"04100", "04200", "04300", "04400", "04500", "04600", "04700", "04800", "04900", "05000", "05100", "05200",
			"05300", "05400", "05500", "05600", "05700", "05800", "05900", "06000", "06100", "06200", "06300", "06400",
			"06500", "06600", "06700", "06800", "06900", "07000", "07100", "07200", "07300", "07400", "07500", "07600",
			"07700", "07800", "07900", "08000", "08100", "08200", "08300", "08400", "08500", "08600", "08700", "08800",
			"08900", "09200", "09500", "10000", "10100", "10200", "10300", "10400", "10500", "10600", "10700", "10800",
			"10900", "11000", "11100", "11200", "11300", "11400", "11500", "11600", "11700", "11800", "11900", "12000",
			"12100", "12200", "12300", "12400", "12500", "12600", "12700", "12800", "12900", "13000", "13100", "13200",
			"13300", "13400", "13500", "13600", "13700", "13800", "13900", "14000", "14100", "14200", "14300", "14400",
			"14500", "14600", "14700", "14800", "14900", "15000", "15100", "15200", "15300", "15400", "15500", "15600",
			"15700", "15800", "15900", "16000", "16100", "16200", "16300", "16400", "16500", "16600", "16700", "16800",
			"16900", "17000", "17100", "17200", "17300", "17400", "17500", "17600", "17700", "17800", "17900", "18000",
			"18100", "18200", "18300", "18400", "18500", "18600", "18700", "18800", "18900", "19000", "19100", "19200",
			"19300", "19400", "19500", "19600", "19700", "19800", "19900", "20000", "20100", "20200", "20300", "20400",
			"20500", "20600", "20700", "20800", "20900", "21000", "21100", "21200", "21300", "21400", "21500", "21600",
			"21700", "21800", "21900", "22000", "22100", "22200", "22300", "22400", "22500", "22600", "22700", "22800",
			"22900", "23000", "23100", "23200", "23300", "23400", "23500", "23600", "23700", "23800", "23900", "24000",
			"24100", "24200", "24300", "24400", "24500", "24600", "24700", "24800", "24900", "25000", "25100", "25200",
			"25300", "25400", "25500", "25600", "25700", "25800", "25900", "26000", "26100", "26200", "26300", "26400",
			"26500", "26600", "26700", "26800", "27000", "27100", "27200", "27300", "27400", "27500", "27600", "27700",
			"27800", "27900", "28000", "28100", "28200", "28300", "28400", "28500", "28600", "28700", "28800", "28900",
			"29000", "29100", "29200", "29300", "29400", "29500", "29600", "29700", "29800", "29900", "30000", "30100",
			"30200", "30300", "30400", "30500", "30600", "30700", "30800", "30900", "31000", "31100", "31200", "31300",
			"31400", "31500", "31600", "31700", "31800", "31900", "32000", "32100", "32200", "32300", "32400", "32500",
			"32600", "32700", "32800", "32900", "33000", "33100", "33200", "33300", "33400", "33500", "33600", "33700",
			"33800", "33900", "34100", "34200", "34300", "34400", "34500", "34600", "34700", "34800", "34900", "35000",
			"35100", "35200", "35400", "35500", "35600", "35700", "35800", "35900", "36000", "36100", "36200", "36300",
			"36400", "36500", "36600", "36700", "36800", "36900", "37000", "37100", "37200", "37300", "37400", "37500",
			"37600", "37700", "37800", "37900", "38000", "38100", "38200", "38300", "38400", "38500", "38600", "38700",
			"38800", "38900", "39000", "39100", "39200", "39300", "39400", "39500", "39600", "39700", "39800", "40000",
			"40100", "40200", "40300", "40400", "40500", "40600", "40700", "40800", "40900", "41000", "41100", "41200",
			"41300", "41400", "41500", "41600", "41700", "41800", "42000", "42100", "42200", "42300", "42400", "42500",
			"42600", "42700", "43000", "43100", "43200", "43300", "43400", "43500", "43600", "43700", "43800", "43900",
			"44000", "44100", "44200", "44300", "44400", "44500", "44600", "44700", "44800", "44900", "45000", "45100",
			"45200", "45300", "45400", "45500", "45600", "45700", "45800", "46000", "46100", "46200", "46300", "46400",
			"46500", "46600", "46700", "46800", "46900", "47000", "47100", "47200", "47300", "47400", "47500", "47600",
			"47700", "47800", "47900", "48000", "48100", "48200", "48300", "48400", "48500", "48600", "48700", "48800",
			"48900", "49000", "49100", "49200", "49300", "49400", "49500", "49600", "49700", "49800", "49900", "50000",
			"50100", "50200", "50300", "50400", "50500", "50600", "50700", "50800", "51000", "51100", "51200", "51300",
			"51400", "51500", "51600", "51800", "52000", "52100", "52200", "52300", "52400", "52500", "52600", "52700",
			"52800", "53000", "53100", "53200", "53300", "53400", "53500", "53600", "53700", "53800", "53900", "54000",
			"54100", "54200", "54300", "54400", "54500", "54600", "54700", "54800", "54900", "55000", "55100", "55200",
			"55300", "55400", "55500", "55600", "55700", "55800", "55900", "56000", "56100", "56200", "56300", "56400",
			"56500", "56600", "56700", "57000", "57100", "57200", "57300", "57400", "57500", "57600", "57700", "58000",
			"58100", "58200", "58300", "58400", "58500", "58600", "58700", "58800", "59000", "59100", "59200", "59300",
			"59400", "59500", "59600", "59700", "59800", "59900", "60000", "60100", "60200", "60300", "60400", "60500",
			"60600", "60700", "60800", "60900", "61000", "61100", "61200", "61300", "61400", "61500", "61600", "61700",
			"61800", "61900", "62000", "62200", "62300", "62400", "62500", "62600", "62700", "62800", "62900", "63000",
			"63100", "63200", "63300", "63400", "63500", "63600", "63700", "63800", "63900", "64000", "64100", "64200",
			"64400", "64500", "64600", "64700", "64800", "64900", "65000", "65100", "65200", "65300", "65400", "65500",
			"65600", "65700", "65800", "66000", "66100", "66200", "66400", "66500", "66600", "66700", "66800", "66900",
			"67000", "67100", "67200", "67300", "67400", "67500", "67600", "67700", "67800", "67900", "68000", "68100",
			"68300", "68400", "68500", "68600", "68700", "68800", "68900", "69000", "69100", "69200", "69300", "70000",
			"70100", "70300", "70400", "70500", "70600", "70700", "70800", "71000", "71100", "71200", "71300", "71400",
			"71600", "71700", "71800", "71900", "72000", "72100", "72200", "72300", "72400", "72500", "72600", "72700",
			"72800", "72900", "73000", "73100", "73300", "73400", "73500", "73600", "73700", "73800", "73900", "74000",
			"74100", "74300", "74400", "74500", "74600", "74700", "74800", "74900", "75000", "75100", "75200", "75300",
			"75400", "75500", "75600", "75700", "75800", "75900", "76000", "76100", "76200", "76300", "76400", "76500",
			"76600", "76700", "76800", "76900", "77000", "77200", "77300", "77400", "77500", "77600", "77700", "77800",
			"77900", "78000", "78100", "78200", "78300", "78400", "78500", "78600", "78700", "78800", "78900", "79000",
			"79100", "79200", "79300", "79400", "79500", "79600", "79700", "79800", "79900", "80000", "80100", "80200",
			"80300", "80400", "80500", "80600", "80700", "80800", "80900", "81000", "81100", "81200", "81300", "81400",
			"81500", "81600", "82000", "82100", "82200", "82300", "82400", "82500", "82600", "82700", "82800", "82900",
			"83000", "83100", "83200", "83300", "83400", "83500", "83600", "83700", "83800", "84000", "84100", "84200",
			"84300", "84400", "84500", "84600", "84700", "85000", "85100", "85200", "85300", "85400", "85500", "85600",
			"85700", "85800", "85900", "86000", "86200", "86300", "86400", "86500", "87000", "87100", "87200", "87300",
			"87400", "87500", "87600", "87700", "87800", "87900", "88000", "88100", "88200", "88300", "88400", "88900",
			"89000", "89100", "89200", "89300", "89400", "89500", "89700", "89800", "90000", "90100", "90200", "90300",
			"90400", "90500", "90600", "90700", "90800", "91000", "91100", "91200", "91300", "91400", "91500", "91600",
			"91700", "91800", "91900", "92000", "92100", "92200", "92300", "92400", "92500", "92600", "92700", "92800",
			"92900", "93000", "93100", "93200", "93300", "93400", "93500", "93600", "93700", "93900", "94000", "94100",
			"94200", "94300", "94400", "94500", "94600", "94700", "94800", "94900", "95000", "95100", "95200", "95300",
			"95400", "95500", "95600", "95700", "95800", "95900", "96000", "96100", "96200", "96300", "96400", "96500",
			"96600", "96700", "96800", "96900", "97000", "97100", "97200", "97300", "97400", "97500", "97600", "97700",
			"97800", "97900", "98000", "98100", "98200", "98300", "98400", "98500", "98600", "98700", "98800", "98900",
			"99000", "99100", "99200", "99300", "99400", "99500", "99600", "99700", "99800", "99900"}

		lnIDMiss = "error"

		purposeMiss = strMiss
		purposeLvl  = []string{"P", "C", "N", "R"}

		termMin, termMax, termMiss          = int32(1), int32(700), int32(-1)
		numBorrMin, numBorrMax, numBorrMiss = int32(1), int32(10), int32(-1)

		sellerMiss   = "unknown"
		servicerMiss = "unknown"

		sConformMiss = strMiss
		sConformDef  = "N"
		sConformLvl  = []string{"Y", "N"}

		programMiss = strMiss
		programDef  = "9"
		programLvl  = []string{"H", "9"}

		harpMiss = strMiss
		harpDef  = "N"
		harpLvl  = []string{"Y", "N"}

		valMthdMiss = strMiss
		valMthdLvl  = []string{"1", "2", "3", "9"}

		ioMiss = strMiss
		ioDef  = "N"
		ioLvl  = []string{"Y", "N"}
	)
	fds := make(map[int]*chutils.FieldDef)

	fd := &chutils.FieldDef{
		Name:        "fico",
		ChSpec:      chutils.ChField{Base: chutils.ChInt, Length: 32},
		Description: "fico at origination, 301-850, missing=-1 ",
		Legal:       &chutils.LegalValues{LowLimit: ficoMin, HighLimit: ficoMax},
		Missing:     ficoMiss,
	}
	fds[0] = fd

	fd = &chutils.FieldDef{
		Name:        "fpDt",
		ChSpec:      chutils.ChField{Base: chutils.ChDate, Format: "200601"},
		Description: "first payment date, missing=" + fpDtMiss.Format("2006/1/2"),
		Legal:       &chutils.LegalValues{LowLimit: fpDtMin, HighLimit: fpDtMax},
		Missing:     fpDtMiss,
	}
	fds[1] = fd

	fd = &chutils.FieldDef{
		Name:        "firstTime",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 1},
		Description: "first time homebuyer: Y, N, missing=" + firstTimeMiss,
		Legal:       &chutils.LegalValues{Levels: firstTimeLvl},
		Missing:     firstTimeMiss,
	}
	fds[2] = fd

	fd = &chutils.FieldDef{
		Name:        "matDt",
		ChSpec:      chutils.ChField{Base: chutils.ChDate, Format: "200601"},
		Description: "loan maturity date (initial), missing=" + matDtMiss.Format("2006/1/2"),
		Legal:       &chutils.LegalValues{LowLimit: matDtMin, HighLimit: matDtMax},
		Missing:     matDtMiss,
	}
	fds[3] = fd

	fd = &chutils.FieldDef{
		Name:        "msa",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 5},
		Description: "msa/division code, missing/not in MSA=" + msaMiss,
		Legal:       &chutils.LegalValues{Levels: msaLvl},
		Default:     "00000",
		Missing:     msaMiss,
	}
	fds[4] = fd

	fd = &chutils.FieldDef{
		Name:        "mi",
		ChSpec:      chutils.ChField{Base: chutils.ChInt, Length: 32},
		Description: "mi percentage, 0-55, missing=" + fmt.Sprintf("%v", miMiss),
		Legal:       &chutils.LegalValues{LowLimit: miMin, HighLimit: miMax},
		Missing:     miMiss,
	}
	fds[5] = fd

	fd = &chutils.FieldDef{
		Name:        "units",
		ChSpec:      chutils.ChField{Base: chutils.ChInt, Length: 32},
		Description: "# of units in the property, 1-4, missing=" + fmt.Sprintf("%v", unitMiss),
		Legal:       &chutils.LegalValues{LowLimit: unitMin, HighLimit: unitMax},
		Missing:     unitMiss,
	}
	fds[6] = fd

	fd = &chutils.FieldDef{
		Name:        "occ",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 1},
		Description: "property occupancy: P (primary), S (secondary), I (investor), missing=" + occMiss,
		Legal:       &chutils.LegalValues{Levels: occLvl},
		Missing:     occMiss,
	}
	fds[7] = fd

	fd = &chutils.FieldDef{
		Name:        "cltv",
		ChSpec:      chutils.ChField{Base: chutils.ChInt, Length: 32},
		Description: "combined cltv at origination, 1-998, missing=" + fmt.Sprintf("%v", cltvMiss),
		Legal:       &chutils.LegalValues{LowLimit: cltvMin, HighLimit: cltvMax},
		Missing:     cltvMiss,
	}
	fds[8] = fd

	fd = &chutils.FieldDef{
		Name:        "dti",
		ChSpec:      chutils.ChField{Base: chutils.ChInt, Length: 32},
		Description: "dti at origination, 1-65, missing=" + fmt.Sprintf("%v", dtiMiss),
		Legal:       &chutils.LegalValues{LowLimit: dtiMin, HighLimit: dtiMax},
		Missing:     dtiMiss,
	}
	fds[9] = fd

	fd = &chutils.FieldDef{
		Name:        "opb",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "balance at origination, missing=" + fmt.Sprintf("%v", opbMiss),
		Legal:       &chutils.LegalValues{LowLimit: opbMin, HighLimit: opbMax},
		Missing:     opbMiss,
	}
	fds[10] = fd

	fd = &chutils.FieldDef{
		Name:        "ltv",
		ChSpec:      chutils.ChField{Base: chutils.ChInt, Length: 32},
		Description: "ltv at origination, 1-998, missing=" + fmt.Sprintf("%v", ltvMiss),
		Legal:       &chutils.LegalValues{LowLimit: ltvMin, HighLimit: ltvMax},
		Missing:     ltvMiss,
	}
	fds[11] = fd

	fd = &chutils.FieldDef{
		Name:        "rate",
		ChSpec:      chutils.ChField{Base: chutils.ChFloat, Length: 32},
		Description: "note rate at origination, 0-15, missing=" + fmt.Sprintf("%v", rateMiss),
		Legal:       &chutils.LegalValues{LowLimit: rateMin, HighLimit: rateMax},
		Missing:     rateMiss,
	}
	fds[12] = fd

	fd = &chutils.FieldDef{
		Name:        "channel",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 1},
		Description: "acquisition channel: B, R, T, C, missing=" + channelMiss,
		Legal:       &chutils.LegalValues{Levels: channelLvl},
		Missing:     channelMiss,
	}
	fds[13] = fd

	fd = &chutils.FieldDef{
		Name:        "pPen",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 1},
		Description: "prepay penalty flag: Y, N, missing=" + ppenMiss,
		Legal:       &chutils.LegalValues{Levels: pPenLvl},
		Missing:     ppenMiss,
	}
	fds[14] = fd

	fd = &chutils.FieldDef{
		Name:        "amType",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 3},
		Description: "amortization type: FRM, ARM, missing=" + amTypeMiss,
		Legal:       &chutils.LegalValues{Levels: amTypeLvl},
		Missing:     amTypeMiss,
	}
	fds[15] = fd

	fd = &chutils.FieldDef{
		Name:        "state",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 2},
		Description: "property state postal abbreviation, missing=" + stateMiss,
		Legal:       &chutils.LegalValues{Levels: stateLvl},
		Missing:     stateMiss,
	}
	fds[16] = fd

	fd = &chutils.FieldDef{
		Name:        "propType",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 2},
		Description: "property type: SF (single family), CO (condo), PU (PUD), CP (coop), MH (manufactured), missing=" + propTypeMiss,
		Legal:       &chutils.LegalValues{Levels: propTypeLvl},
		Missing:     propTypeMiss,
	}
	fds[17] = fd

	fd = &chutils.FieldDef{
		Name:        "zip",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 5},
		Description: "3-digit zip (last 2 digits are 00), missing=" + zipMiss,
		Legal:       &chutils.LegalValues{Levels: zipLvl},
		Missing:     zipMiss,
	}
	fds[18] = fd

	fd = &chutils.FieldDef{
		Name:        "lnID",
		ChSpec:      chutils.ChField{Base: chutils.ChString},
		Description: "Loan ID PYYQnXXXXXXX P=F or A YY=year, n=quarter, missing=" + lnIDMiss,
		Legal:       &chutils.LegalValues{},
		Missing:     lnIDMiss,
	}
	fds[19] = fd

	fd = &chutils.FieldDef{
		Name:        "purpose",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 1},
		Description: "loan purpose: P (purch), C (cash out refi), N (rate/term refi) R (refi), missing=" + purposeMiss,
		Legal:       &chutils.LegalValues{Levels: purposeLvl},
		Missing:     purposeMiss,
	}
	fds[20] = fd

	fd = &chutils.FieldDef{
		Name:        "term",
		ChSpec:      chutils.ChField{Base: chutils.ChInt, Length: 32},
		Description: "loan term at origination, missing=" + fmt.Sprintf("%v", termMiss),
		Legal:       &chutils.LegalValues{LowLimit: termMin, HighLimit: termMax},
		Missing:     termMiss,
	}
	fds[21] = fd

	fd = &chutils.FieldDef{
		Name:        "numBorr",
		ChSpec:      chutils.ChField{Base: chutils.ChInt, Length: 32},
		Description: "number of borrowers, 1-10, missing=" + fmt.Sprintf("%v", numBorrMiss),
		Legal:       &chutils.LegalValues{LowLimit: numBorrMin, HighLimit: numBorrMax},
		Missing:     numBorrMiss,
	}
	fds[22] = fd

	fd = &chutils.FieldDef{
		Name:        "seller",
		ChSpec:      chutils.ChField{Base: chutils.ChString, Funcs: chutils.OuterFuncs{chutils.OuterLowCardinality}},
		Description: "name of seller, missing=" + sellerMiss,
		Legal:       &chutils.LegalValues{},
		Missing:     sellerMiss,
	}
	fds[23] = fd

	fd = &chutils.FieldDef{
		Name:        "servicer",
		ChSpec:      chutils.ChField{Base: chutils.ChString, Funcs: chutils.OuterFuncs{chutils.OuterLowCardinality}},
		Description: "name of most recent servicer, missing=" + servicerMiss,
		Legal:       &chutils.LegalValues{},
		Missing:     servicerMiss,
	}
	fds[24] = fd

	fd = &chutils.FieldDef{
		Name:        "sConform",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 1},
		Description: "super conforming flag: Y, N, missing=" + sConformMiss,
		Legal:       &chutils.LegalValues{Levels: sConformLvl},
		Missing:     sConformMiss,
		Default:     sConformDef,
	}
	fds[25] = fd

	fd = &chutils.FieldDef{
		Name:        "preHARPlnID",
		ChSpec:      chutils.ChField{Base: chutils.ChString, Length: 0},
		Description: "for HARP loans, lnID of prior loan, missing=" + lnIDMiss,
		Legal:       &chutils.LegalValues{},
		Missing:     lnIDMiss,
	}
	fds[26] = fd

	fd = &chutils.FieldDef{
		Name:        "program",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 1},
		Description: "freddie program: H (home possible) N (no program), missing=" + programMiss,
		Legal:       &chutils.LegalValues{Levels: programLvl},
		Missing:     programMiss,
		Default:     programDef,
	}
	fds[27] = fd

	fd = &chutils.FieldDef{
		Name:        "harp",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 1},
		Description: "HARP loan: Y, N, missing=" + harpMiss,
		Legal:       &chutils.LegalValues{Levels: harpLvl},
		Missing:     harpMiss,
		Default:     harpDef,
	}
	fds[28] = fd

	fd = &chutils.FieldDef{
		Name:        "valMthd",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 1},
		Description: "property value method 1 (ACE), 2 (Full) 3 (Other), missing=" + valMthdMiss,
		Legal:       &chutils.LegalValues{Levels: valMthdLvl},
		Missing:     valMthdMiss,
	}
	fds[29] = fd

	fd = &chutils.FieldDef{
		Name:        "io",
		ChSpec:      chutils.ChField{Base: chutils.ChFixedString, Length: 1},
		Description: "io Flag: Y, N, missing=" + ioMiss,
		Legal:       &chutils.LegalValues{Levels: ioLvl},
		Missing:     ioMiss,
		Default:     ioDef,
	}
	fds[30] = fd
	return chutils.NewTableDef("lnID", chutils.MergeTree, fds)
}
