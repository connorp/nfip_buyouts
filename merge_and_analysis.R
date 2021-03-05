## Connor P. Jackson second year paper
## cpjackson@berkeley.edu
## Merge and analyze the data

library(data.table)
library(bit64)
library(lubridate)
library(tidyr)
library(DescTools)
library(tidycensus)

## ---- import-data ----

nchomes <- fread("../data_buyouts/ZAsmt_NC.csv", key="ImportParcelID", index="latest")
nctrans <- fread("../data_buyouts/ZTrans_NC.csv", key="ImportParcelID", na.strings = "")
source("NFIP_claims_processing.R")

nchomes <- nchomes[!is.na(PropertyAddressCensusTractAndBlock)]
# TODO: recreate missing census tracts from zipcodes or latlongs

# Remove unreasonable years built, cross-apply missing years built, drop properties with unknown years
nchomes[EffectiveYearBuilt < YearBuilt, EffectiveYearBuilt := NA]
nchomes[EffectiveYearBuilt > year(Sys.time()), EffectiveYearBuilt := NA]
nchomes[is.na(EffectiveYearBuilt), EffectiveYearBuilt := YearBuilt]
nchomes[is.na(YearBuilt), YearBuilt := EffectiveYearBuilt]
nchomes <- nchomes[!is.na(YearBuilt)]

## Handle multiple assessment records in a year. Keep the most recent one
nchomes[, record_date := my(ExtractDate)]
nchomes[, record_year := year(record_date)]
setorder(nchomes, ImportParcelID, record_date)
nchomes <- unique(nchomes, by=c("ImportParcelID", "record_year"), fromLast=TRUE)

sfha_homes <- nchomes[SFHA_TF == TRUE]

modal_val <- function(confusingVar) {
  if (length(confusingVar) == uniqueN(confusingVar)) return(tail(confusingVar, n = 1))
  return(tail(Mode(confusingVar), n = 1))
}

## TODO: handle changes in census tracts better
## If a house is listed in different tracts in different years, take the modal tract. For ties, take the later
sfha_homes[, censusTract := as.integer64(modal_val(as.numeric(as.character(censusTract)))), by = .(ImportParcelID)]
# Do the same thing for year built
sfha_homes[, YearBuilt := modal_val(YearBuilt), by = .(ImportParcelID)]
sfha_homes[, EffectiveYearBuilt := modal_val(EffectiveYearBuilt), by = .(ImportParcelID)]

## ---- CH-create-property-long-data ----

# Define the transaction year
nctrans[, date := DocumentDate]
nctrans[is.na(date), date := RecordingDate]
nctrans[, year := year(date)]
nctrans[, IntraFamilyTransferFlag := IntraFamilyTransferFlag == "Y"]
nctrans[, TransferTaxExemptFlag := TransferTaxExemptFlag == "Y"]
nctrans[DocumentTypeStndCode == "", DocumentTypeStndCode := NA]
nctrans[LoanTypeStndCode == "", LoanTypeStndCode := NA]
nctrans[SalesPriceAmountStndCode == "", SalesPriceAmountStndCode := NA]
nctrans[, mortgage_nfip_req := LoanTypeProgramStndCode != ""]

dataclasses <- c(NA, "", "D", "H", "M", "P")
doctypes <- c(NA, "", "ADDE","AFDV","AGSL","BFDE","BSDE","CDDE","CHDE","CMDE","CPDE","CTSL",
              "DEED","EXCH","GRDE","IDDE","LWDE","MTGE","NOWR","QCDE","RCDE","SPWD","WRDE")
pricecodes <- c(NA, "", "AF","AV","CF","CM","CN","CR","CS","CU","EP","HB","LN","MP","RA","RD","ST")
loantypes <- c(NA, "", "AC","AS","BL","CE","CM","CL","DP","FO","FM","FA","PM","SL")

# identify the arms length transactions, not intra-family transfers, refinances, foreclosures, etc,
# only for the homes in Special Flood Hazard Areas
nctrans_armslen <- nctrans[(ImportParcelID %in% sfha_homes[, unique(ImportParcelID)] &
                            DataClassStndCode %in% dataclasses & DocumentTypeStndCode %in% doctypes &
                            SalesPriceAmountStndCode %in% pricecodes & LoanTypeStndCode %in% loantypes &
                            mortgage_nfip_req == TRUE & IntraFamilyTransferFlag == FALSE & year >= 1995)]

# Define only a single transaction per property per year. If there are multiple transaction
# records in a year, choose the one with the highest (non-missing) recorded sale price
setkey(nctrans_armslen, ImportParcelID, year)
setorder(nctrans_armslen, ImportParcelID, year, -SalesPriceAmount, na.last = TRUE)
nctrans_armslen <- unique(nctrans_armslen, by = c("ImportParcelID", "year"))
nctrans_armslen[, transaction_obs := TRUE]

# construct the panel dataset by filling in every parcel x year combination
panel_frame <-CJ(ImportParcelID = sfha_homes[, unique(ImportParcelID)], year = nctrans_armslen[, unique(year)])
keepvars <- c("ImportParcelID", "date", "year", "SalesPriceAmount", "mortgage_nfip_req", "transaction_obs")
nctrans_panel <- merge(nctrans_armslen[, ..keepvars], panel_frame, all = TRUE)
rm(nctrans_armslen, panel_frame)

## We apply the assessor data onto the transaction panel, assuming the old assessment records
## hold until a new one occurs
nctrans_panel[, roll_year := year]
setkey(nctrans_panel, ImportParcelID, roll_year)
sfha_homes[, roll_year := record_year]
setkey(sfha_homes, ImportParcelID, roll_year)

nctrans_panel <- sfha_homes[nctrans_panel, roll = TRUE, rollends = TRUE]
nctrans_panel[is.na(transaction_obs), transaction_obs := FALSE]
nctrans_panel[, roll_year := NULL]

setnames(nctrans_panel, "FLD_ZONE", "floodZone")

## ---- flood-events ----
flood_frame <- CJ(tract_zone = unique(c(sfha_homes[, unique(paste(censusTract, FLD_ZONE, sep="_"))],
                                         claims[sfha == TRUE, unique(paste(censusTract, floodZone, sep="_"))])),
                  year = claims[, unique(year(yearofLoss))])

flood_count <- claims[sfha == TRUE, .(flood_count = .N), by = .(tract_zone = paste(censusTract, floodZone, sep="_"),
                                                                year = year(yearofLoss))]
setkey(flood_count, tract_zone, year)

flood_panel <- merge(flood_count, flood_frame, all = TRUE)

rm(flood_count, flood_frame)
flood_panel[, c("censusTract", "floodZone") := tstrsplit(tract_zone, "_", fixed = TRUE, type.convert = TRUE)]
flood_panel[, censusTract := as.integer64(censusTract)]
flood_panel[is.na(flood_count), flood_count := 0]
flood_panel[, flood_event := flood_count > 0]
flood_panel[, tract_zone := NULL]

## TODO: change years to start in August or at the start of flood season?

## ---- nfip-policy-holders ----
policies_frame <- CJ(tract_zone_year = unique(c(sfha_homes[, unique(paste(censusTract, FLD_ZONE, YearBuilt, sep="_"))],
                                                policies[sfha == TRUE, unique(paste(censusTract, floodZone, originalConstructionDate, sep="_"))])),
                     year = policies[, unique(policyyear)])

policies_status <- policies[sfha == TRUE, .(policies_count = .N, postFIRM = (mean(postFIRMConstructionIndicator) > 0.5)),
                            by = .(tract_zone_year = paste(censusTract, floodZone, originalConstructionDate, sep="_"),
                                   year = policyyear)]
setkey(policies_status, tract_zone_year, year)

policies_panel <- merge(policies_status, policies_frame, all = TRUE)
policies_panel[, c("censusTract", "floodZone", "originalConstructionDate") := tstrsplit(tract_zone_year,
                                                                                        "_", fixed = TRUE, type.convert = TRUE)]
policies_panel[, tract_zone_year := NULL]
policies_panel[, censusTract := as.integer64(censusTract)]
policies_panel[is.na(policies_count), policies_count := 0]
rm(policies_frame, policies_status)

# TODO: Find other data on year of initial FIRM (and maybe CRS class as well?)
FIRMdates <- policies_panel[postFIRM == TRUE, .(FIRMyear = min(originalConstructionDate, na.rm = TRUE)),
                            keyby = .(censusTract, floodZone)]
FIRMdates[FIRMyear < 1974, FIRMyear := 1974]
policies_panel[, postFIRM := NULL]
setnames(policies_panel, "originalConstructionDate", "YearBuilt")
setkey(policies_panel, censusTract, floodZone, YearBuilt, year)

## ---- combine-panels ----

tzy_panel <- nctrans_panel[, .(properties_count = .N, TotalAssessedValue = mean(TotalAssessedValue),
                               TotalMarketValue = mean(TotalMarketValue),
                               transaction_count = sum(transaction_obs), SalesPriceAmount = mean(SalesPriceAmount)),
                           keyby = .(censusTract, floodZone, YearBuilt, year)]
tzy_panel[, transaction_prob := transaction_count / properties_count]

tzy_panel <- merge(tzy_panel, policies_panel, all.x = TRUE)
tzy_panel <- merge(tzy_panel, flood_panel, by = c("censusTract", "floodZone", "year"), all.x = TRUE)
tzy_panel <- merge(tzy_panel, FIRMdates, by = c("censusTract", "floodZone"), all.x = TRUE)
setkey(tzy_panel, censusTract, floodZone, YearBuilt, year)

tzy_panel[, policy_prob := policies_count / properties_count]
tzy_panel[, adapted := YearBuilt > FIRMyear]
tzy_panel[YearBuilt < 1974, adapted := FALSE]
tzy_panel[YearBuilt == FIRMyear, adapted := NA]  # ambiguous within-year FIRM timing
tzy_panel[, reg_reform := year >= 2013]

# Create the lag flood policy and flood events data
setorder(tzy_panel, censusTract, floodZone, YearBuilt, year)
tzy_panel[, policies_L1 := shift(policies_count, 1, type = "lag"), by = .(censusTract, floodZone, YearBuilt)]
tzy_panel[, policies_L2 := shift(policies_count, 2, type = "lag"), by = .(censusTract, floodZone, YearBuilt)]
tzy_panel[, policies_L3 := shift(policies_count, 3, type = "lag"), by = .(censusTract, floodZone, YearBuilt)]
tzy_panel[, policies_F1 := shift(policies_count, 1, type = "lead"), by = .(censusTract, floodZone, YearBuilt)]
tzy_panel[, flood_L1 := shift(flood_event, 1, type = "lag"), by = .(censusTract, floodZone, YearBuilt)]
tzy_panel[, flood_L2 := shift(flood_event, 2, type = "lag"), by = .(censusTract, floodZone, YearBuilt)]
tzy_panel[, flood_L3 := shift(flood_event, 3, type = "lag"), by = .(censusTract, floodZone, YearBuilt)]
tzy_panel[, flood_F1 := shift(flood_event, 1, type = "lead"), by = .(censusTract, floodZone, YearBuilt)]
tzy_panel[, policy_prob_L1 := policies_L1 / properties_count]
tzy_panel[, policy_prob_L2 := policies_L2 / properties_count]
tzy_panel[, policy_prob_L3 := policies_L3 / properties_count]
tzy_panel[, policy_prob_F1 := policies_F1 / properties_count]
tzy_panel[, reg_reform_L1 := shift(reg_reform, 1, type = "lag"), by = .(censusTract, floodZone, YearBuilt)]
tzy_panel[, reg_reform_L2 := shift(reg_reform, 2, type = "lag"), by = .(censusTract, floodZone, YearBuilt)]
tzy_panel[, reg_reform_L3 := shift(reg_reform, 3, type = "lag"), by = .(censusTract, floodZone, YearBuilt)]
tzy_panel[, reg_reform_F1 := shift(reg_reform, 1, type = "lead"), by = .(censusTract, floodZone, YearBuilt)]

# tractable instruments: legislative changes (2013 x adapted)
# construct the instruments manually: flood_event:policy_prob | flood_L1:policy_prob_L1 | flood_L2:policy_prob_L2
tzy_panel[, flooded_insured := flood_event*policy_prob]
tzy_panel[, flooded_insured_L1 := flood_L1*policy_prob_L1]
tzy_panel[, flooded_insured_L2 := flood_L2*policy_prob_L2]
tzy_panel[, flooded_insured_L3 := flood_L3*policy_prob_L3]
tzy_panel[, flooded_insured_F1 := flood_F1*policy_prob_F1]


# year range: 2009-2016 (2017 is only thru September) (2012-2016 with lags)
tzy_panel[, in_sample := ((year %in% 2009:2016) & !is.na(adapted) & YearBuilt < 2009)]
tzy_panel[, sample_2L := in_sample & year > 2010]
tzy_panel[, sample_3L := in_sample & year > 2011]
tzy_panel[, panel_id := as.factor(paste(censusTract, floodZone, sep="_"))]
tzy_panel[, censusTract := as.factor(censusTract)]
tzy_panel[, YearBuilt := as.factor(YearBuilt)]
tzy_panel[, year := as.factor(year)]

# TODO: figure out issue with policies exceeding properties
bad_tracts <- tzy_panel[in_sample == TRUE & policies_count > properties_count, unique(panel_id)]
tzy_panel[panel_id %in% bad_tracts, in_sample := FALSE]

# TODO: Fix the handful of inconsistencies when aggregating nctrans_panel up to tzy_panel
# tzy_panel[in_sample == TRUE, .N, keyby = .(is.na(policies_L3), year)]

## ---- census-data ----

# v2009 <- load_variables(2009, "acs1", cache = TRUE)
# v2009_5 <- load_variables(2009, "acs5", cache = TRUE)
#
# acs_vars <- c("B01001_002", "B01002_001", "B01003_001", "B02001_002", "B02001_003", "B03001_003",
#               "B17001_002", "B06011_001", "C18108_010", "C18130_016",
#               "C18108_002", "C18130_002")
# acs_2009 <- get_acs(geography = "tract", variables = acs_vars, state = "NC", moe = 95,
#                     year = 2009, survey = "acs5")
