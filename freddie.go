package main

import (
	"flag"
	"fmt"
	"github.com/invertedv/chutils"
	_ "github.com/mailru/go-clickhouse/v2"
	"io/ioutil"
	"log"
	"time"
)

const lnIDMiss = "!"

var monthMin, monthMax, MonthMiss time.Time = minDt, maxDt, missDt

const upbMin, upbMax, upbMiss = float32(0.0), float32(2000000.0), float32(-1.0)

const dqStatMiss = "!"

var dqStatLvl map[string]int

const ageMin, ageMax, ageMiss = int32(0), int32(480), int32(-1)
const rTermLglMin, rTermLglMax, rTermLglMiss = int32(0), int32(480), int32(-1)

var defectDtMin, defectDtMax, defectDtMiss = minDt, maxDt, missDt

const modMiss = "!"

var modLvl map[string]int

const zbMiss = "!"

var zbLvl map[string]int

var zbDtMin, zbDtMax, zbDtMiss = minDt, maxDt, missDt

const curRateMin, curRateMax, curRateMiss = float32(0.0), float32(15.0), float32(-1.0)
const defrlMin, defrlMax, defrlMiss = float32(0.0), float32(1000000.0), float32(-1.0)

var lpdMin, lpdMax, lpdMiss = minDt, maxDt, missDt

const fclPrdMiMin, fclPrdMiMax, fclPrdMiMiss = float32(0.0), float32(500000.0), float32(-1.0)
const fclPrdNetMin, fclPrdNetMax, fclPrdNetMiss = float32(0.0), float32(2000000.0), float32(-1.0)
const fclPrdMwMin, fclPrdMwMax, fclPrdMwMiss = float32(0.0), float32(2000000.0), float32(-1.0)
const fclExpMin, fclExpMax, fclExpMiss = float32(-200000.0), float32(0.0), float32(1.0) // fclExp is negative
const fclLExpMin, fclLExpMax, fclLExpMiss = float32(-120000.0), float32(0.0), float32(1.0)
const fclPExpMin, fclPExpMax, fclPExpMiss = float32(-120000.0), float32(0.0), float32(1.0)
const fclTaxesMin, fclTaxesMax, fclTaxesMiss = float32(-300000.0), float32(0.0), float32(1.0)

const (
	maxPV  = float32(2500000.0) // max for property values
	maxDef = float32(100000.0)  // max deferrals

	maxRate = float32(15.0) // max for interest rates

)

var maxDt = time.Now().Add(time.Hour * 24 * 365)

const (
	minFloat = float32(0.0)
	minInt   = int32(0)
)

var minDt = time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC)

const (
	missFloat     = float32(-1.0)
	missFloatBNeg = float32(-10000000.0)
	missInt       = int32(-1)
	missStr       = "!"
)

var missDt = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

// legal values

// initialize legal values
func init() {
	// dqStat
	dqStatLvl = make(map[string]int)
	for dq := 0; dq < 100; dq++ {
		dqStatLvl[fmt.Sprintf("%d", dq)] = 1
	}
	dqStatLvl["RA"] = 1 // REO Acquisition

	// mod
	modLvl = make(map[string]int)
	modLvl["Y"], modLvl["P"], modLvl["N"] = 1, 1, 1

	// zb
	zbLvl = make(map[string]int)
	zbLvl["01"], zbLvl["02"], zbLvl["03"], zbLvl["96"], zbLvl["09"], zbLvl["15"], zbLvl["00"] =
		1, 1, 1, 1, 1, 1, 1

}

func main() {
	host := flag.String("host", "127.0.0.1", "string")
	user := flag.String("user", "", "string")
	password := flag.String("password", "", "string")
	srcDir := flag.String("dir", "", "string")
	flag.Parse()
	con, err := chutils.NewConnect("http", *host, *user, *password)
	if err != nil {
		log.Fatalln(err)
	}
	con.DB.Ping()
	dir, err := ioutil.ReadDir(*srcDir)
	if err == nil {
		for _, f := range dir {
			fmt.Println(f)
		}
	}

}
