package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	influx "github.com/influxdata/influxdb1-client/v2"
)

var (
	influxHost       = getEnv("INFLUX_HOST", "tsdbe.nidec-asi-online.com")
	influxPort       = getEnv("INFLUX_PORT", "443")
	influxUser       = getEnv("INFLUX_USER", "Elto")
	influxPw         = getEnv("INFLUX_PW", "NidecItadmElto")
	influxDB         = getEnv("INFLUX_DB", "Elto")
	influxMeas       = getEnv("INFLUX_MEAS", "elto1sec_borne")
	influxTagProject = getEnv("INFLUX_TAG_PROJECT", "project")

	mysqlDSN = getEnv("MYSQL_DSN", "AdminNidec:u6Ehe987XBSXxa4@tcp(141.94.31.144:3306)/indicator?parseTime=true")

	projects     = []string{"8822_001", "8822_002", "8822_003", "8822_004", "8822_005", "8822_006", "8822_008"}
	mappingSites = map[string]string{
		"8822_001": "Saint-Jean-de-Maurienne",
		"8822_002": "La Rochelle",
		"8822_003": "Pouilly-en-Auxois",
		"8822_004": "Carvin",
		"8822_005": "Blois",
		"8822_006": "Pau - Novotel",
		"8822_008": "Courtenay",
	}
)

var (
	IC1_SEQ01_MAP = map[int]string{
		0:  "IC00 - 080Q1 - [NX_OBI_400VAC_Q_Flt] - 400VAC - CB Open",
		1:  "IC01 - 080Q1 - [NX_OBI_400VAC_ElecFLT] - 400VAC - Electrical Fault Flt",
		2:  "IC02 - 081Q1-081F1 - [NX_OBI_LIGHTARR_FLT] - NX PARAFOUDRE Flt",
		3:  "IC03 - [CU_OBI_400VAC_Q_Flt] - 400VAC Main circuit breakers Cooling unit Open",
		4:  "IC04 - [AUX_OBI_400VAC_Q_Flt] - 400VAC Main circuit breakers Open",
		5:  "IC05 - [UPS_OBI_IN230VAC_Q_Flt] - 230VAC Main circuit breakers UPS INPUT Open",
		6:  "IC06 - [IMD_IBH_230VAC_Q] - 230VAC circuit breakers IMD Open",
		7:  "IC07 - 110Q1 - [UPS_OBI_OUT230VAC_Q_Flt] - 230VAC Main circuit breakers UPS OUT Open",
		8:  "IC08 - [REL_OBI_230VAC_Q_Flt] - 230VAC UPS Relay Circuit breaker Open",
		9:  "IC09 - [IMD_OBI_230VAC_Q_Flt] - 230VAC circuit breakers IMD Open",
		10: "IC10 - [LOV_AU_OBI_230VAC_Q_Flt] - 230VAC circuit breakers Measurement + AU Open",
		14: "IC14 - 105Q1 - [EXTGAZ_OBI_400VAC_Q_Flt] - 400VAC Circuit breaker GAZ Extractor Open",
		15: "IC15 - 105Q2 - [EXTGAZ_OBI_K] - Gaz Extractor RUN Open",
		16: "IC16 - 159Q1 - [AFE1_OBI_ACPL_F_FLT] - AFE1 AC Preload fuses - Fuse FLT Open",
		17: "IC17 - 193KM1 - [AFE1_OBI_AC_Q_Flt] - AFE1 AC Circuit breaker",
		18: "IC18 - 193K1 - [AFE1_OBI_ACPL_K] - AFE1 DC Preload contactor",
		19: "IC19 - [AMS_OBI_AFE1_ComFlt] AFE1 Loss communication",
		20: "IC20 - [IMD_OBI_DefCom] Loss communication",
		21: "IC21 - [AMS_OBI_RIO_ComFlt] RIO Loss communication",
		22: "IC22 - [AMS_OBI_Lovato_ComFlt] Lovato Loss communication",
		23: "IC23 - [AMS_OBI_CU_ComFlt] Loss communication",
		24: "IC24 - AFE VDC Measurement < 490VDC",
	}

	PC1_SEQ01_MAP = map[int]string{
		0:  "PC00 - 080Q1 - [NX_OBI_400VAC_Q_Flt] - 400VAC - CB Open",
		1:  "PC01 - 080Q1 - [NX_OBI_400VAC_ElecFLT] - 400VAC - Electrical Fault Flt",
		2:  "PC02 - 081Q1-081F1 - [NX_OBI_LIGHTARR_FLT] - NX PARAFOUDRE Flt",
		3:  "PC03 - [CU_OBI_400VAC_Q_Flt] - 400VAC Main circuit breakers Cooling unit Open",
		4:  "PC04 - [AUX_OBI_400VAC_Q_Flt] - 400VAC Main circuit breakers Open",
		5:  "PC05 - 110Q1 - [UPS_OBI_OUT230VAC_Q_Flt] - 230VAC Main circuit breakers UPS OUT Open",
		6:  "PC06 - [REL_OBI_230VAC_Q_Flt] - 230VAC UPS Relay Circuit breaker Open",
		7:  "PC07 - 112Q2 - [IMD_OBI_230VAC_Q_Flt] - 230VAC circuit breakers IMD Open",
		8:  "PC08 - [NX_OBI_400VAC_DecouplingFLT] - AV Relay Decoupling Flt",
		9:  "PC09 - 351S1 - [BP_OBI_AU_EXT_FLT] - ES Push button External Open",
		10: "PC10 - 105Q1 - [EXTGAZ_OBI_400VAC_Q_Flt] - 400VAC Circuit breaker GAZ Extractor Open",
		11: "PC11 - 105Q2 - [EXTGAZ_OBI_K] - Gaz Extractor RUN Open",
		12: "PC12 - 24VDC - IO_IBH_24VDC_ON / SAFE_IBH_24VDC_ON / GAPA_IBH_24VDC_ON - 24VDC UPS Voltage ON",
		13: "PC13 - [AMS_OBI_AFE1_ComFlt] AFE1 Loss communication",
		14: "PC14 - 155F1 - [IMD_OBI_Flt] - Controleur permanent d'isolement Flt",
		15: "PC15 - [AMS_OBI_RIO_ComFlt] RIO Loss communication",
		16: "PC16 - [AMS_OBI_Lovato_ComFlt] Lovato Loss communication",
		17: "PC17 - [AMS_OBI_CU_ComFlt] Loss communication",
		18: "PC18 - 193K1 - [AFE1_OBI_ACPL_K_DiscKM] - AFE1 DC Preload contactor",
		19: "PC19 - 193K1 - [AFE1_OBI_ACPL_K_DiscKM] - AFE1 DC Preload contactor (dup.)",
		20: "PC20 - 193KM1 - [AFE1_OBI_AC_K_DiscKM] - Discordance AFE DC Line contactor",
		21: "PC21 - AFE1 FAULT",
		22: "PC22 - AFE1 voltage input flt",
		23: "PC23 - AFE1 tension < 150VDC",
		24: "PC24 - 155F2 - [SAFE_OBI_ON_FLT] - ES LIVE Flt",
		25: "PC25 - 155F1 - [IMD_OBI_Flt] - Controleur permanent d'isolement Communication fault",
		26: "PC26 - 1505Q1 - [FIRE_OBI_Q_Flt / FIRE_OBI_GazRelease_FLT] - alarme incendie",
		27: "PC27 - Conteneur température fault",
		28: "PC28 - Fire System Fault",
		29: "PC29 - Manual Force STOP",
		30: "PC30 - SEQ TimeOut STEP",
		31: "PC31 - Doors open",
	}

	IC1_SEQ02_MAP = map[int]string{
		0: "IC00 - BBMS1 No Relay closed",
		1: "IC01 - 111Q2 - [HB1_IBH_230VAC_Q] - 230VAC UPS HB1 circuit breaker Hacheur BATT1 Open",
		2: "IC02 - Manual disable HB",
		3: "IC03 - SEQ01 [SEQ01.OLI.Branch_01] AFE - Is Step 100",
	}

	PC1_SEQ02_MAP = map[int]string{
		0:  "PC00 - 360F1 - SAFE_OBI_ON_FLT - ES LIVE Flt",
		1:  "PC01 - [AMS_OBI_RIO_ComFlt] - RIO Loss communication",
		2:  "PC02 - SEQ01 - AFE - Is Step 100",
		3:  "PC03 - [AMS_OBI_HB1_ComFlt] Hacheur1 Loss communication",
		4:  "PC04 - BBMS1 - Loss Communication",
		5:  "PC05 - BBMS1 Fault",
		6:  "PC06 - HB1 - FAULT",
		7:  "PC07 - 206Q1 - [HB1_OBI_OUTDC_K_ICPCFlt] - HB1 OUT DC Contactor running condition missing",
		8:  "PC08 - 111Q2 - [HB1_IBH_230VAC_Q] - 230VAC UPS HB1 circuit breaker Hacheur BATT1",
		9:  "PC09 - 111Q5 - [HB_IBH_Measure230VAC_Q] - 230VAC Circuit breaker - Measurement circuit",
		10: "PC10 - 24VDC UPS BBMS Voltage ON Open",
		11: "PC11 - 24VDC UPS Rack1_2 Voltage ON Open",
		12: "PC12 - HB1 BATT1 Reactor temperature Fault",
		13: "PC13 - 210K1 - [GAPA_OBI_24VDC_ON_ICPCFlt] - GAPA 24V VDC Contactor running condition missing",
		14: "PC14 - BBMS1 No Relay closed",
		16: "PC16 - SEQ02 TimeOut - TimeOut 10",
		17: "PC17 - SEQ02 TimeOut - TimeOut 20",
		18: "PC18 - SEQ02 TimeOut - TimeOut 30",
		19: "PC19 - SEQ02 TimeOut - TimeOut 40",
		20: "PC20 - SEQ02 TimeOut - TimeOut 60",
	}

	IC1_SEQ03_MAP = map[int]string{
		0: "IC00 - BBMS2 No Relay closed",
		1: "IC01 - 111Q3 - [HB2_IBH_230VAC_Q] - 230VAC UPS HB2 circuit breaker Hacheur BATT2 Open",
		2: "IC02 - Manual disable HB",
		3: "IC03 - SEQ01 [SEQ01.OLI.Branch_01] AFE - Is Step 100",
	}

	PC1_SEQ03_MAP = map[int]string{
		0:  "PC00 - 360F1 - SAFE_OBI_ON_FLT - ES LIVE Flt",
		1:  "PC01 - [AMS_OBI_RIO_ComFlt] - RIO Loss communication",
		2:  "PC02 - SEQ01 - AFE - Is Step 100",
		3:  "PC03 - [AMS_OBI_HB2_ComFlt] Hacheur2 Loss communication",
		4:  "PC04 - BBMS2 - Loss Communication",
		5:  "PC05 - BBMS2 Fault",
		6:  "PC06 - HB2 - FAULT",
		7:  "PC07 - 246Q1 - [HB2_OBI_OUTDC_K_ICPCFlt] - HB2 OUT DC Contactor running condition missing",
		8:  "PC08 - 111Q3 - [HB2_IBH_230VAC_Q] - 230VAC UPS HB2 circuit breaker Hacheur BATT2",
		9:  "PC09 - 111Q5 - [HB_IBH_Measure230VAC_Q] - 230VAC Circuit breaker - Measurement circuit",
		10: "PC10 - 24VDC UPS BBMS Voltage ON Open",
		11: "PC11 - 24VDC UPS Rack1_2 Voltage ON Open",
		12: "PC12 - HB2 BATT2 Reactor temperature Fault",
		13: "PC13 - 210K1 - [GAPA_OBI_24VDC_ON_ICPCFlt] - GAPA 24V VDC Contactor running condition missing",
		14: "PC14 - BBMS2 No Relay closed",
		16: "PC16 - SEQ03 TimeOut - TimeOut 10",
		17: "PC17 - SEQ03 TimeOut - TimeOut 20",
		18: "PC18 - SEQ03 TimeOut - TimeOut 30",
		19: "PC19 - SEQ03 TimeOut - TimeOut 40",
		20: "PC20 - SEQ03 TimeOut - TimeOut 60",
	}

	PDC_SEQ_IC_MAP = map[int]string{
		0:  "IC00 - Main sequence running",
		1:  "IC01 - Ev contactor not closed",
		2:  "IC02 - No over temp Self",
		6:  "IC06 - EndPoint OCPP Connected",
		7:  "IC07 - HMI communication Fault",
		8:  "IC08 - Not charging",
		9:  "IC09 - DCBM Fault",
		10: "IC10 - Unavailable from CPO",
		11: "IC11 - Payter Com Fault",
		12: "IC12 - ZMQ Com Fault",
	}

	PDC_SEQ_PC_MAP = map[int]string{
		0:  "PC00 - RIO COM",
		2:  "PC02 - Inverter M1 Ready",
		3:  "PC03 - UpstreamSequence no fault",
		4:  "PC04 - Ev contactor no discordance",
		6:  "PC06 - No over temp Self",
		7:  "PC07 - No TO",
		8:  "PC08 - Plug no Over Temp CCS",
		9:  "PC09 - Inverter OverVoltage",
		12: "PC12 - Communication EVI",
		13: "PC13 - ES EVI",
		14: "PC14 - Manual Indispo",
		15: "PC15 - HMI communication Fault",
	}
)

type EquipConfig struct {
	ICField string
	PCField string
	ICMap   map[int]string
	PCMap   map[int]string
	Title   string
	EqpName string
}

var equipConfigs = map[string]EquipConfig{
	"AC": {
		ICField: "SEQ01.OLI.A.IC1",
		PCField: "SEQ01.OLI.A.PC1",
		ICMap:   IC1_SEQ01_MAP,
		PCMap:   PC1_SEQ01_MAP,
		Title:   "AC (SEQ01)",
		EqpName: "Variateur AFE",
	},
	"DC1": {
		ICField: "SEQ02.OLI.A.IC1",
		PCField: "SEQ02.OLI.A.PC1",
		ICMap:   IC1_SEQ02_MAP,
		PCMap:   PC1_SEQ02_MAP,
		Title:   "Batterie DC1 (SEQ02)",
		EqpName: "Variateur HB1",
	},
	"DC2": {
		ICField: "SEQ03.OLI.A.IC1",
		PCField: "SEQ03.OLI.A.PC1",
		ICMap:   IC1_SEQ03_MAP,
		PCMap:   PC1_SEQ03_MAP,
		Title:   "Batterie DC2 (SEQ03)",
		EqpName: "Variateur HB2",
	},
	"PDC1": {
		ICField: "SEQ12.OLI.A.IC1",
		PCField: "SEQ12.OLI.A.PC1",
		ICMap:   PDC_SEQ_IC_MAP,
		PCMap:   PDC_SEQ_PC_MAP,
		Title:   "Point de charge 1 (SEQ12)",
		EqpName: "HC1 (PDC1)",
	},
	"PDC2": {
		ICField: "SEQ22.OLI.A.IC1",
		PCField: "SEQ22.OLI.A.PC1",
		ICMap:   PDC_SEQ_IC_MAP,
		PCMap:   PDC_SEQ_PC_MAP,
		Title:   "Point de charge 2 (SEQ22)",
		EqpName: "HC1 (PDC2)",
	},
	"PDC3": {
		ICField: "SEQ13.OLI.A.IC1",
		PCField: "SEQ13.OLI.A.PC1",
		ICMap:   PDC_SEQ_IC_MAP,
		PCMap:   PDC_SEQ_PC_MAP,
		Title:   "Point de charge 3 (SEQ13)",
		EqpName: "HC2 (PDC3)",
	},
	"PDC4": {
		ICField: "SEQ23.OLI.A.IC1",
		PCField: "SEQ23.OLI.A.PC1",
		ICMap:   PDC_SEQ_IC_MAP,
		PCMap:   PDC_SEQ_PC_MAP,
		Title:   "Point de charge 4 (SEQ23)",
		EqpName: "HC2 (PDC4)",
	},
	"PDC5": {
		ICField: "SEQ14.OLI.A.IC1",
		PCField: "SEQ14.OLI.A.PC1",
		ICMap:   PDC_SEQ_IC_MAP,
		PCMap:   PDC_SEQ_PC_MAP,
		Title:   "Point de charge 5 (SEQ14)",
		EqpName: "HC3 (PDC5)",
	},
	"PDC6": {
		ICField: "SEQ24.OLI.A.IC1",
		PCField: "SEQ24.OLI.A.PC1",
		ICMap:   PDC_SEQ_IC_MAP,
		PCMap:   PDC_SEQ_PC_MAP,
		Title:   "Point de charge 6 (SEQ24)",
		EqpName: "HC3 (PDC6)",
	},
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getSiteName(siteCode string) string {
	if name, ok := mappingSites[siteCode]; ok {
		return name
	}
	return siteCode
}

func decodeBinary(value int) []int {
	var positions []int
	for i := 0; i < 32; i++ {
		if value&(1<<i) != 0 {
			positions = append(positions, i)
		}
	}
	return positions
}

func getLatestValue(client influx.Client, siteCode, fieldName string) (float64, error) {
	queryStr := fmt.Sprintf(
		`SELECT "%s" FROM "%s" WHERE "%s" = '%s' ORDER BY time DESC LIMIT 1`,
		fieldName,
		influxMeas,
		influxTagProject,
		siteCode,
	)

	q := influx.NewQuery(queryStr, influxDB, "")
	response, err := client.Query(q)
	if err != nil {
		return 0, err
	}
	if response.Error() != nil {
		return 0, response.Error()
	}

	if len(response.Results) == 0 || len(response.Results[0].Series) == 0 {
		return 0, fmt.Errorf("aucune donnée")
	}

	series := response.Results[0].Series[0]
	if len(series.Values) == 0 {
		return 0, fmt.Errorf("aucune valeur")
	}

	row := series.Values[0]
	if row[1] == nil {
		return 0, fmt.Errorf("valeur NULL")
	}

	var value float64
	switch v := row[1].(type) {
	case float64:
		value = v
	case int64:
		value = float64(v)
	case json.Number:
		if f, err := v.Float64(); err == nil {
			value = f
		} else {
			return 0, fmt.Errorf("impossible de parser")
		}
	}

	return value, nil
}

func closeDefaut(db *sql.DB, id int, dateFin time.Time) error {
	query := `UPDATE kpi_defauts_log SET date_fin = ? WHERE id = ?`
	_, err := db.Exec(query, dateFin, id)
	if err != nil {
		return fmt.Errorf("erreur fermeture: %w", err)
	}
	log.Printf("FERME - ID: %d, Date_Fin: %s", id, dateFin.Format("2006-01-02 15:04:05"))
	return nil
}

func createDefaut(db *sql.DB, site, fieldName, eqp, defaut string, bitPos int, startTime time.Time) error {
	query := `INSERT INTO kpi_defauts_log (site, date_debut, date_fin, defaut, eqp, bit_position, field_name) 
	          VALUES (?, ?, NULL, ?, ?, ?, ?)`
	_, err := db.Exec(query, site, startTime, defaut, eqp, bitPos, fieldName)
	if err != nil {
		return fmt.Errorf("erreur création: %w", err)
	}
	log.Printf(" CREE - Bit: %d, Defaut: %s, Date_debut: %s", bitPos, defaut, startTime.Format("2006-01-02 15:04:05"))
	return nil
}

func findDefautStartDate(client influx.Client, siteCode, fieldName string, bitPos int) (time.Time, error) {
	lookbackDays := 120
	startTime := time.Now().AddDate(0, 0, -lookbackDays)

	queryStr := fmt.Sprintf(
		`SELECT "%s" FROM "%s" WHERE "%s" = '%s' AND time >= '%s' ORDER BY time ASC`,
		fieldName,
		influxMeas,
		influxTagProject,
		siteCode,
		startTime.Format(time.RFC3339),
	)

	q := influx.NewQuery(queryStr, influxDB, "")
	response, err := client.Query(q)
	if err != nil {
		return time.Time{}, err
	}
	if response.Error() != nil {
		return time.Time{}, response.Error()
	}

	bitMask := 1 << bitPos
	previousValue := 0
	var transitionTime time.Time

	for _, result := range response.Results {
		for _, series := range result.Series {
			for _, row := range series.Values {
				t, err := time.Parse(time.RFC3339, row[0].(string))
				if err != nil {
					continue
				}

				if row[1] == nil {
					continue
				}

				var value int
				switch v := row[1].(type) {
				case float64:
					value = int(v)
				case int64:
					value = int(v)
				case json.Number:
					if f, err := v.Float64(); err == nil {
						value = int(f)
					} else {
						continue
					}
				default:
					continue
				}

				bitActive := (value & bitMask) != 0
				wasBitActive := (previousValue & bitMask) != 0

				if !wasBitActive && bitActive {
					transitionTime = t
				}

				previousValue = value
			}
		}
	}

	if transitionTime.IsZero() {
		return time.Time{}, fmt.Errorf("pas trouvé")
	}

	return transitionTime, nil
}

func processFieldMonitor(client influx.Client, db *sql.DB, siteCode, site string, fieldName string, fieldMap map[int]string, eqpName string) error {
	currentValue, err := getLatestValue(client, siteCode, fieldName)
	if err != nil {
		return fmt.Errorf("erreur récupération valeur: %w", err)
	}
	intValue := int(currentValue)

	log.Printf(" Fermeture des défauts ouverts...")
	queryOpen := `SELECT id, bit_position FROM kpi_defauts_log WHERE site = ? AND field_name = ? AND eqp = ? AND date_fin IS NULL`
	rows, err := db.Query(queryOpen, site, fieldName, eqpName)
	if err != nil {
		return fmt.Errorf("erreur requête: %w", err)
	}
	defer rows.Close()

	closedCount := 0
	for rows.Next() {
		var id, bitPos int
		if err := rows.Scan(&id, &bitPos); err != nil {
			log.Printf("      ✗ Erreur scan: %v", err)
			continue
		}

		bitMask := 1 << bitPos
		bitActive := (intValue & bitMask) != 0

		if !bitActive {
			if err := closeDefaut(db, id, time.Now()); err != nil {
				log.Printf("      ✗ Erreur fermeture: %v", err)
			} else {
				closedCount++
			}
		}
	}

	if closedCount > 0 {
		log.Printf("      → %d défaut(s) fermé(s)", closedCount)
	} else {
		log.Printf("      ℹ Aucun défaut à fermer")
	}

	log.Printf("    2️⃣ Recherche des nouveaux défauts...")
	activeBits := decodeBinary(intValue)

	newCount := 0
	for _, bitPos := range activeBits {
		if defautDesc, ok := fieldMap[bitPos]; ok && defautDesc != "" {
			queryCheck := `SELECT COUNT(*) FROM kpi_defauts_log WHERE site = ? AND field_name = ? AND eqp = ? AND bit_position = ?`
			var count int
			err := db.QueryRow(queryCheck, site, fieldName, eqpName, bitPos).Scan(&count)
			if err != nil {
				log.Printf("      ✗ Erreur vérification: %v", err)
				continue
			}

			if count == 0 {
				startTime, err := findDefautStartDate(client, siteCode, fieldName, bitPos)
				if err != nil {
					log.Printf("      ⚠ Date_debut non trouvée pour bit %d, utilisation NOW (%v)", bitPos, err)
					startTime = time.Now()
				}

				if err := createDefaut(db, site, fieldName, eqpName, defautDesc, bitPos, startTime); err != nil {
					log.Printf("      ✗ Erreur création: %v", err)
				} else {
					newCount++
				}
			}
		}
	}

	if newCount > 0 {
		log.Printf("      → %d nouveau(x) défaut(s)", newCount)
	} else {
		log.Printf("      ℹ Aucun nouveau défaut")
	}

	return nil
}

func processEquipment(client influx.Client, db *sql.DB, siteCode, siteName string, config EquipConfig) error {
	log.Printf("  %s", config.EqpName)

	if err := processFieldMonitor(client, db, siteCode, siteName, config.ICField, config.ICMap, config.EqpName); err != nil {
		log.Printf("IC: %v", err)
	}

	if err := processFieldMonitor(client, db, siteCode, siteName, config.PCField, config.PCMap, config.EqpName); err != nil {
		log.Printf("PC: %v", err)
	}

	return nil
}

func processSite(client influx.Client, db *sql.DB, siteCode string) error {
	siteName := getSiteName(siteCode)
	log.Printf("\n %s", siteName)

	for _, config := range equipConfigs {
		if err := processEquipment(client, db, siteCode, siteName, config); err != nil {
			log.Printf("  ✗ Erreur: %v", err)
		}
	}

	return nil
}

func main() {
	loc, err := time.LoadLocation("Europe/Paris")
	if err != nil {
		log.Fatalf("Erreur chargement timezone: %v", err)
	}
	time.Local = loc

	db, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		log.Fatalf("✗ MySQL: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("✗ Connexion MySQL: %v", err)
	}
	log.Println("✓ MySQL OK")

	influxURL := fmt.Sprintf("https://%s:%s", influxHost, influxPort)
	client, err := influx.NewHTTPClient(influx.HTTPConfig{
		Addr:               influxURL,
		Username:           influxUser,
		Password:           influxPw,
		InsecureSkipVerify: true,
	})
	if err != nil {
		log.Fatalf("✗ InfluxDB: %v", err)
	}
	defer client.Close()

	log.Printf("✓ InfluxDB OK\n")

	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 3)

	for _, siteCode := range projects {
		wg.Add(1)
		go func(sc string) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			if err := processSite(client, db, sc); err != nil {
				log.Printf("✗ Site %s: %v", sc, err)
			}
		}(siteCode)
	}

	wg.Wait()
}
