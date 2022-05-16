package main

import (
	"github.com/invertedv/chutils"
	"github.com/invertedv/chutils/file"
	"log"
	"os"
)

func monthlyTD(fileName string) (*file.Reader, error) {
	f, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	rdr := file.NewReader(fileName, '|', '\n', '"', 0, 0, 0, f, 6000000)
	rdr.Skip = 0
	fds := make(map[int]*chutils.FieldDef)

	fd := &chutils.FieldDef{
		Name:        "lnID",
		ChSpec:      chutils.ChField{chutils.ChString, 0, "", ""},
		Description: "Loan ID PYYQnXXXXXXX P=F or A YY=year, n=quarter",
		Legal:       &chutils.LegalValues{},
		Missing:     lnIDMiss,
	}
	fds[0] = fd

	fd = &chutils.FieldDef{
		Name: "month",
		ChSpec: chutils.ChField{
			Base:      chutils.ChDate,
			Length:    0,
			OuterFunc: "",
			Format:    "200601",
		},
		Description: "month of data",
		Legal:       &chutils.LegalValues{LowLimit: monthMin, HighLimit: monthMax},
		Missing:     MonthMiss,
	}
	fds[1] = fd

	fd = &chutils.FieldDef{
		Name: "upb",
		ChSpec: chutils.ChField{
			Base:      chutils.ChFloat,
			Length:    32,
			OuterFunc: "",
			Format:    "",
		},
		Description: "unpaid balance",
		Legal:       &chutils.LegalValues{LowLimit: upbMin, HighLimit: upbMax},
		Missing:     upbMiss,
	}
	fds[2] = fd

	fd = &chutils.FieldDef{
		Name: "dqStat",
		ChSpec: chutils.ChField{
			Base:      chutils.ChFixedString,
			Length:    3,
			OuterFunc: "",
			Format:    "",
		},
		Description: "DQ status code",
		Legal:       &chutils.LegalValues{Levels: &dqStatLvl},
		Missing:     dqStatMiss,
	}
	fds[3] = fd

	fd = &chutils.FieldDef{
		Name: "age",
		ChSpec: chutils.ChField{
			Base:      chutils.ChInt,
			Length:    32,
			OuterFunc: "",
			Format:    "",
		},
		Description: "loan age based on origination date",
		Legal:       &chutils.LegalValues{LowLimit: ageMin, HighLimit: ageMax},
		Missing:     ageMiss,
	}
	fds[4] = fd

	fd = &chutils.FieldDef{
		Name: "rTermLgl",
		ChSpec: chutils.ChField{
			Base:      chutils.ChInt,
			Length:    32,
			OuterFunc: "",
			Format:    "",
		},
		Description: "remaining legal term",
		Legal:       &chutils.LegalValues{LowLimit: rTermLglMin, HighLimit: rTermLglMax},
		Missing:     rTermLglMiss,
	}
	fds[5] = fd

	fd = &chutils.FieldDef{
		Name: "defectDt",
		ChSpec: chutils.ChField{
			Base:      chutils.ChDate,
			Length:    0,
			OuterFunc: "",
			Format:    "200601",
		},
		Description: "underwriting defect date",
		Legal:       &chutils.LegalValues{LowLimit: defectDtMin, HighLimit: defectDtMax},
		Missing:     defectDtMiss,
	}
	fds[6] = fd

	fd = &chutils.FieldDef{
		Name: "mod",
		ChSpec: chutils.ChField{
			Base:      chutils.ChFixedString,
			Length:    1,
			OuterFunc: "",
			Format:    "",
		},
		Description: "modification flag",
		Legal:       &chutils.LegalValues{Levels: &modLvl},
		Missing:     modMiss,
	}
	fds[7] = fd

	fd = &chutils.FieldDef{
		Name: "zb",
		ChSpec: chutils.ChField{
			Base:      chutils.ChFixedString,
			Length:    2,
			OuterFunc: "",
			Format:    "",
		},
		Description: "zero balance code",
		Legal:       &chutils.LegalValues{Levels: &zbLvl},
		Missing:     zbMiss}
	fds[8] = fd

	fd = &chutils.FieldDef{
		Name: "zbDt",
		ChSpec: chutils.ChField{
			Base:      chutils.ChDate,
			Length:    0,
			OuterFunc: "",
			Format:    "200601",
		},
		Description: "zero balance date",
		Legal:       &chutils.LegalValues{LowLimit: zbDtMin, HighLimit: zbDtMax},
		Missing:     zbDtMiss,
	}
	fds[9] = fd

	fd = &chutils.FieldDef{
		Name: "curRate",
		ChSpec: chutils.ChField{
			Base:      chutils.ChFloat,
			Length:    32,
			OuterFunc: "",
			Format:    "",
		},
		Description: "current note rate",
		Legal:       &chutils.LegalValues{LowLimit: curRateMin, HighLimit: curRateMax},
		Missing:     curRateMiss,
	}
	fds[10] = fd

	fd = &chutils.FieldDef{
		Name: "defrl",
		ChSpec: chutils.ChField{
			Base:      chutils.ChFloat,
			Length:    32,
			OuterFunc: "",
			Format:    "",
		},
		Description: "current deferral amount",
		Legal:       &chutils.LegalValues{LowLimit: defrlMin, HighLimit: defrlMax},
		Missing:     defrlMiss,
	}
	fds[11] = fd

	fd = &chutils.FieldDef{
		Name: "lpd",
		ChSpec: chutils.ChField{
			Base:      chutils.ChDate,
			Length:    0,
			OuterFunc: "",
			Format:    "200601",
		},
		Description: "last pay date",
		Legal:       &chutils.LegalValues{LowLimit: lpdMin, HighLimit: lpdMax},
		Missing:     lpdMiss,
	}
	fds[12] = fd

	fd = &chutils.FieldDef{
		Name: "fclPrdMi",
		ChSpec: chutils.ChField{
			Base:      chutils.ChFloat,
			Length:    32,
			OuterFunc: "",
			Format:    "",
		},
		Description: "foreclosure credit enhancement proceeds",
		Legal:       &chutils.LegalValues{LowLimit: fclPrdMiMin, HighLimit: fclPrdMiMax},
		Missing:     fclPrdMiMiss,
	}
	fds[13] = fd

	fd = &chutils.FieldDef{
		Name: "fclPrdNet",
		ChSpec: chutils.ChField{
			Base:      chutils.ChFloat,
			Length:    32,
			OuterFunc: "",
			Format:    "",
		},
		Description: "foreclosure net proceeds",
		Legal:       &chutils.LegalValues{LowLimit: fclPrdNetMin, HighLimit: fclPrdNetMax},
		Missing:     fclPrdNetMiss,
	}
	fds[14] = fd

	fd = &chutils.FieldDef{
		Name: "fclPrdMw",
		ChSpec: chutils.ChField{
			Base:      chutils.ChFloat,
			Length:    32,
			OuterFunc: "",
			Format:    "",
		},
		Description: "foreclosure make whole proceeds",
		Legal:       &chutils.LegalValues{LowLimit: fclPrdMwMin, HighLimit: fclPrdMwMax},
		Missing:     fclPrdMwMiss,
	}
	fds[15] = fd

	fd = &chutils.FieldDef{
		Name: "fclExp",
		ChSpec: chutils.ChField{
			Base:      chutils.ChFloat,
			Length:    32,
			OuterFunc: "",
			Format:    "",
		},
		Description: "total foreclosure expenses (values are negative)",
		Legal:       &chutils.LegalValues{LowLimit: fclExpMin, HighLimit: fclExpMax},
		Missing:     fclExpMiss,
	}
	fds[16] = fd

	fd = &chutils.FieldDef{
		Name: "fclLExp",
		ChSpec: chutils.ChField{
			Base:      chutils.ChFloat,
			Length:    32,
			OuterFunc: "",
			Format:    "",
		},
		Description: "foreclosure recovery legal expenses (values are negative)",
		Legal:       &chutils.LegalValues{LowLimit: fclLExpMin, HighLimit: fclLExpMax},
		Missing:     fclLExpMiss,
	}
	fds[17] = fd

	fd = &chutils.FieldDef{
		Name: "fclPExp",
		ChSpec: chutils.ChField{
			Base:      chutils.ChFloat,
			Length:    32,
			OuterFunc: "",
			Format:    "",
		},
		Description: "foreclosure property preservation expenses (values are negative)",
		Legal:       &chutils.LegalValues{LowLimit: fclPExpMin, HighLimit: fclPExpMax},
		Missing:     fclPExpMiss,
	}
	fds[18] = fd

	fd = &chutils.FieldDef{
		Name: "fclTaxes",
		ChSpec: chutils.ChField{
			Base:      chutils.ChFloat,
			Length:    32,
			OuterFunc: "",
			Format:    "",
		},
		Description: "foreclosure property taxes and insurance",
		Legal:       &chutils.LegalValues{LowLimit: fclTaxesMin, HighLimit: fclTaxesMax},
		Missing:     fclTaxesMiss,
	}
	fds[19] = fd

	fd = &chutils.FieldDef{
		Name: "fcl_misc_cost",
		ChSpec: chutils.ChField{
			Base:      chutils.ChFloat,
			Length:    32,
			OuterFunc: "",
			Format:    "",
		},
		Description: "",
		Legal:       &chutils.LegalValues{LowLimit: -800000.0, HighLimit: 4000000.0},
		Missing:     -1.0,
	}
	fds[20] = fd

	fd = &chutils.FieldDef{
		Name: "fcl_loss",
		ChSpec: chutils.ChField{
			Base:      chutils.ChFloat,
			Length:    32,
			OuterFunc: "",
			Format:    "",
		},
		Description: "",
		Legal:       &chutils.LegalValues{LowLimit: -5000000.0, HighLimit: 800000.0},
		Missing:     -1.0,
	}
	fds[21] = fd

	fd = &chutils.FieldDef{
		Name: "mod_t_loss",
		ChSpec: chutils.ChField{
			Base:      chutils.ChFloat,
			Length:    32,
			OuterFunc: "",
			Format:    "",
		},
		Description: "",
		Legal:       &chutils.LegalValues{LowLimit: -70000.0, HighLimit: 400000.0},
		Missing:     -1.0,
	}
	fds[22] = fd

	ln_stepmod_flg := make(map[string]int)
	ln_stepmod_flg["Y"], ln_stepmod_flg["N"], ln_stepmod_flg["X"] = 1, 1, 1
	fd = &chutils.FieldDef{
		Name: "ln_stepmod_flg",
		ChSpec: chutils.ChField{
			Base:      chutils.ChFixedString,
			Length:    1,
			OuterFunc: "",
			Format:    "",
		},
		Description: "",
		Legal:       &chutils.LegalValues{Levels: &ln_stepmod_flg},
		Missing:     "!",
	}
	fds[23] = fd

	ln_dfrd_pay_flg := make(map[string]int)
	ln_dfrd_pay_flg["Y"], ln_dfrd_pay_flg["P"], ln_dfrd_pay_flg["N"] = 1, 1, 1
	fd = &chutils.FieldDef{
		Name: "ln_dfrd_pay_flg",
		ChSpec: chutils.ChField{
			Base:      chutils.ChFixedString,
			Length:    1,
			OuterFunc: "",
			Format:    "",
		},
		Description: "",
		Legal:       &chutils.LegalValues{Levels: &ln_dfrd_pay_flg},
		Missing:     "!",
	}
	fds[24] = fd

	fd = &chutils.FieldDef{
		Name: "ln_curr_eltv",
		ChSpec: chutils.ChField{
			Base:      chutils.ChFloat,
			Length:    32,
			OuterFunc: "",
			Format:    "",
		},
		Description: "",
		Legal:       &chutils.LegalValues{LowLimit: 0.0, HighLimit: 250.0},
		Missing:     -1.0,
	}
	fds[25] = fd

	fd = &chutils.FieldDef{
		Name: "ln_zb_prin",
		ChSpec: chutils.ChField{
			Base:      chutils.ChFloat,
			Length:    32,
			OuterFunc: "",
			Format:    "",
		},
		Description: "",
		Legal:       &chutils.LegalValues{LowLimit: 0.0, HighLimit: 5000000.0},
		Missing:     -1.0,
	}
	fds[26] = fd

	fd = &chutils.FieldDef{
		Name: "ln_dq_accr_int",
		ChSpec: chutils.ChField{
			Base:      chutils.ChFloat,
			Length:    32,
			OuterFunc: "Nullable",
			Format:    "",
		},
		Description: "",
		Legal:       &chutils.LegalValues{LowLimit: -80000.0, HighLimit: 900000.0},
		Missing:     -1.0,
	}
	fds[27] = fd

	ln_dq_distr_flg := make(map[string]int)
	ln_dq_distr_flg["Y"], ln_dq_distr_flg["N"] = 1, 1
	fd = &chutils.FieldDef{
		Name: "ln_dq_distr_flg",
		ChSpec: chutils.ChField{
			Base:      chutils.ChFixedString,
			Length:    1,
			OuterFunc: "",
			Format:    "",
		},
		Description: "",
		Legal:       &chutils.LegalValues{Levels: &ln_dq_distr_flg},
		Missing:     "!",
	}
	fds[28] = fd

	borr_asst_plan := make(map[string]int)
	borr_asst_plan["F"], borr_asst_plan["R"], borr_asst_plan["T"], borr_asst_plan["N"] = 1, 1, 1, 1
	fd = &chutils.FieldDef{
		Name: "borr_asst_plan",
		ChSpec: chutils.ChField{
			Base:      chutils.ChFixedString,
			Length:    1,
			OuterFunc: "",
			Format:    "",
		},
		Description: "",
		Legal:       &chutils.LegalValues{Levels: &borr_asst_plan},
		Missing:     "!",
	}
	fds[29] = fd

	fd = &chutils.FieldDef{
		Name: "mod_c_loss",
		ChSpec: chutils.ChField{
			Base:      chutils.ChFloat,
			Length:    32,
			OuterFunc: "",
			Format:    "",
		},
		Description: "",
		Legal:       &chutils.LegalValues{LowLimit: -70000.0, HighLimit: 400000.0},
		Missing:     -1.0,
	}
	fds[30] = fd

	fd = &chutils.FieldDef{
		Name: "ln_ib_prin",
		ChSpec: chutils.ChField{
			Base:      chutils.ChFloat,
			Length:    32,
			OuterFunc: "",
			Format:    "",
		},
		Description: "",
		Legal:       &chutils.LegalValues{LowLimit: 0.0, HighLimit: 5000000.0},
		Missing:     -1.0,
	}
	fds[31] = fd

	rdr.SetTableSpec(&chutils.TableDef{
		//		Name:      "aaa",
		Key:       "ln_id",
		Engine:    chutils.MergeTree,
		FieldDefs: fds,
	})
	if e := rdr.TableSpec().Check(); e != nil {
		log.Fatalln(e)
	}
	return rdr, nil
}
