package main


// Dates for min, max, missing
d1999 := time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC)
dny := time.Now().Add(time.Hour * 24 * 365)
dmiss := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
fileName := "/mnt/drive3/data/freddie_data/historical_data_time_2000Q1.txt"
f, err := os.Open(fileName)
if err != nil {
return
}
rdr = file.NewReader(fileName, '|', '\n', '"', 0, 0, 0, f, 6000000)
//	defer rdr.Close()
rdr.Skip = 0
fds := make(map[int]*chutils.FieldDef)

fd := &chutils.FieldDef{
Name:        "ln_id",
ChSpec:      chutils.ChField{chutils.ChString, 0, "", ""},
Description: "Loan id",
Legal:       &chutils.LegalValues{},
Missing:     nil,
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
Description: "",
Legal:       &chutils.LegalValues{LowLimit: d1999, HighLimit: dny},
Missing:     dmiss,
}
fds[1] = fd

fd = &chutils.FieldDef{
Name: "ln_prin_c",
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
fds[2] = fd
dqs := make(map[string]int)
for dq := 0; dq < 100; dq++ {
dqs[string(dq)] = 1
}
dqs["RA"] = 1

fd = &chutils.FieldDef{
Name: "ln_dq_status_cd",
ChSpec: chutils.ChField{
Base:      chutils.ChFixedString,
Length:    3,
OuterFunc: "",
Format:    "",
},
Description: "DQ status code",
Legal:       &chutils.LegalValues{Levels: &dqs},
Missing:     "!",
}
fds[3] = fd

fd = &chutils.FieldDef{
Name: "ln_age",
ChSpec: chutils.ChField{
Base:      chutils.ChInt,
Length:    32,
OuterFunc: "",
Format:    "",
},
Description: "",
Legal:       &chutils.LegalValues{LowLimit: 0, HighLimit: 480},
Missing:     -1,
}
fds[4] = fd

fd = &chutils.FieldDef{
Name: "ln_rem_term_legal",
ChSpec: chutils.ChField{
Base:      chutils.ChInt,
Length:    32,
OuterFunc: "",
Format:    "",
},
Description: "",
Legal:       &chutils.LegalValues{LowLimit: 0, HighLimit: 480},
Missing:     -1,
}
fds[5] = fd

fd = &chutils.FieldDef{
Name: "uw_defect_dt",
ChSpec: chutils.ChField{
Base:      chutils.ChDate,
Length:    0,
OuterFunc: "",
Format:    "200601",
},
Description: "",
Legal:       &chutils.LegalValues{LowLimit: d1999, HighLimit: dny},
Missing:     dmiss,
}
fds[6] = fd

ln_mod_flg := make(map[string]int)
ln_mod_flg["Y"], ln_mod_flg["P"], ln_mod_flg["N"] = 1, 1, 1

fd = &chutils.FieldDef{
Name: "ln_mod_flg",
ChSpec: chutils.ChField{
Base:      chutils.ChFixedString,
Length:    1,
OuterFunc: "",
Format:    "",
},
Description: "",
Legal:       &chutils.LegalValues{Levels: &ln_mod_flg},
Missing:     "!",
}
fds[7] = fd

ln_zb_cd := make(map[string]int)
ln_zb_cd["01"], ln_zb_cd["02"], ln_zb_cd["03"], ln_zb_cd["96"], ln_zb_cd["09"], ln_zb_cd["15"], ln_zb_cd["00"] =
1, 1, 1, 1, 1, 1, 1
fd = &chutils.FieldDef{
Name: "ln_zb_cd",
ChSpec: chutils.ChField{
Base:      chutils.ChFixedString,
Length:    2,
OuterFunc: "",
Format:    "",
},
Description: "",
Legal:       &chutils.LegalValues{Levels: &ln_zb_cd},
Missing:     "!"}
//		Calculator: func(td *chutils.TableDef, fs chutils.Row) interface{} {
//			if fs[8].(string) == "" {
//				return "00"
//			}
//			return ""
//		},
fds[8] = fd

fd = &chutils.FieldDef{
Name: "ln_zb_dt",
ChSpec: chutils.ChField{
Base:      chutils.ChDate,
Length:    0,
OuterFunc: "",
Format:    "200601",
},
Description: "",
Legal:       &chutils.LegalValues{LowLimit: d1999, HighLimit: dny},
Missing:     dmiss,
}
fds[9] = fd

fd = &chutils.FieldDef{
Name: "ln_c_ir",
ChSpec: chutils.ChField{
Base:      chutils.ChFloat,
Length:    32,
OuterFunc: "",
Format:    "",
},
Description: "",
Legal:       &chutils.LegalValues{LowLimit: 0.0, HighLimit: 15.0},
Missing:     -1.0,
}
fds[10] = fd

fd = &chutils.FieldDef{
Name: "ln_defrl_amt",
ChSpec: chutils.ChField{
Base:      chutils.ChFloat,
Length:    32,
OuterFunc: "",
Format:    "",
},
Description: "",
Legal:       &chutils.LegalValues{LowLimit: 0.0, HighLimit: 100000.0},
Missing:     -1.0,
}
fds[11] = fd

fd = &chutils.FieldDef{
Name: "ln_last_pay_dt",
ChSpec: chutils.ChField{
Base:      chutils.ChDate,
Length:    0,
OuterFunc: "",
Format:    "200601",
},
Description: "",
Legal:       &chutils.LegalValues{LowLimit: d1999, HighLimit: dny},
Missing:     dmiss,
}
fds[12] = fd

fd = &chutils.FieldDef{
Name: "fcl_ce_prcds",
ChSpec: chutils.ChField{
Base:      chutils.ChFloat,
Length:    32,
OuterFunc: "",
Format:    "",
},
Description: "",
Legal:       &chutils.LegalValues{LowLimit: -100000.0, HighLimit: 500000.0},
Missing:     -1.0,
}
fds[13] = fd

fd = &chutils.FieldDef{
Name: "fcl_net_prcds",
ChSpec: chutils.ChField{
Base:      chutils.ChFloat,
Length:    32,
OuterFunc: "",
Format:    "",
},
Description: "",
Legal:       &chutils.LegalValues{LowLimit: -600000.0, HighLimit: 2000000.0},
Missing:     -1.0,
}
fds[14] = fd

fd = &chutils.FieldDef{
Name: "fcl_reprch_mw_prcds",
ChSpec: chutils.ChField{
Base:      chutils.ChFloat,
Length:    32,
OuterFunc: "",
Format:    "",
},
Description: "",
Legal:       &chutils.LegalValues{LowLimit: -2000000.0, HighLimit: 2000000.0},
Missing:     -1.0,
}
fds[15] = fd

fd = &chutils.FieldDef{
Name: "fcl_cost",
ChSpec: chutils.ChField{
Base:      chutils.ChFloat,
Length:    32,
OuterFunc: "",
Format:    "",
},
Description: "",
Legal:       &chutils.LegalValues{LowLimit: -5000000.0, HighLimit: 1000000.0},
Missing:     -1.0,
}
fds[16] = fd

fd = &chutils.FieldDef{
Name: "fcl_recov_cost",
ChSpec: chutils.ChField{
Base:      chutils.ChFloat,
Length:    32,
OuterFunc: "",
Format:    "",
},
Description: "",
Legal:       &chutils.LegalValues{LowLimit: -1200000.0, HighLimit: 200000.0},
Missing:     -1.0,
}
fds[17] = fd

fd = &chutils.FieldDef{
Name: "fcl_pres_cost",
ChSpec: chutils.ChField{
Base:      chutils.ChFloat,
Length:    32,
OuterFunc: "",
Format:    "",
},
Description: "",
Legal:       &chutils.LegalValues{LowLimit: -1200000.0, HighLimit: 100000.0},
Missing:     -1.0,
}
fds[18] = fd

fd = &chutils.FieldDef{
Name: "fcl_taxes",
ChSpec: chutils.ChField{
Base:      chutils.ChFloat,
Length:    32,
OuterFunc: "",
Format:    "",
},
Description: "",
Legal:       &chutils.LegalValues{LowLimit: -4000000.0, HighLimit: 1000000.0},
Missing:     -1.0,
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
return rdr
}
