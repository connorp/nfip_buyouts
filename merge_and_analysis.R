## Connor P. Jackson second year paper
## cpjackson@berkeley.edu
## Merge and analyze the data

library(data.table)
library(bit64)
library(lubridate)
library(tidyr)
library(sf)
library(DescTools)

## ---- import-data ----

nchomes <- fread("../data_buyouts/ZAsmt_NC.csv", key="ImportParcelID", index="latest")
nctrans <- fread("../data_buyouts/ZTrans_NC.csv", key="ImportParcelID", na.strings = "")
source("NFIP_claims_processing.R")

## ----apply-NFHL ----

### TODO: Geocode missing latlongs
# Can't assign a flood zone without Lat/Long
nchomes <- nchomes[!is.na(PropertyAddressLatitude)]

# Census tract format: 2 digit state + 3 digit county +
# 6 digit tract (maybe containing a decimal) + 4 digit block
# Remove obviously incorrect census blocks and fix formatting
nchomes[!grepl("^37", PropertyAddressCensusTractAndBlock), PropertyAddressCensusTractAndBlock := NA]
nchomes[nchar(PropertyAddressCensusTractAndBlock) < 10, PropertyAddressCensusTractAndBlock := NA]
nchomes[, censusTract := as.integer64(gsub(".", "", substring(PropertyAddressCensusTractAndBlock, 1, 12), fixed = TRUE))]
nchomes <- nchomes[!is.na(censusTract)]
# TODO: recreate missing census tracts from zipcodes or latlongs

# Remove unreasonable years built, cross-apply missing years built, drop properties with unknown years
nchomes[EffectiveYearBuilt < YearBuilt, EffectiveYearBuilt := NA]
nchomes[EffectiveYearBuilt > year(Sys.time()), EffectiveYearBuilt := NA]
nchomes[is.na(EffectiveYearBuilt), EffectiveYearBuilt := YearBuilt]
nchomes[is.na(YearBuilt), YearBuilt := EffectiveYearBuilt]
nchomes <- nchomes[!is.na(YearBuilt)]

# Create a geometry file of the parcel locations so we can match to flood zones
nchomes_shp <- st_as_sf(nchomes[latest==TRUE, .(ImportParcelID, PropertyAddressLongitude, PropertyAddressLatitude)],
                        coords = c("PropertyAddressLongitude", "PropertyAddressLatitude"))
st_crs(nchomes_shp) <- st_crs(nfhl)

# Match to flood zones
nchomes_fldzn <- st_join(nchomes_shp, nfhl)
setDT(nchomes_fldzn, key="ImportParcelID")

nchomes_fldzn[STATIC_BFE == -9999, STATIC_BFE := NA]
nchomes_fldzn[DEPTH == -9999, DEPTH := NA]
nchomes_fldzn[FLD_ZONE == "AREA NOT INCLUDED", FLD_ZONE := NA]

setorder(nchomes_fldzn, ImportParcelID, FLD_ZONE)

# As long as the flood zones are the same in the overlapping polygons, we don't care
nchomes_fldzn <- unique(nchomes_fldzn, by=c("ImportParcelID", "FLD_ZONE"))

### TODO: Handle overlapping flood zones better
# if there are duplicate flood zones, take the higher one
nchomes_fldzn <- unique(nchomes_fldzn, by="ImportParcelID", fromLast=FALSE)

# merge flood zone data back onto assessor data
nchomes <- merge(nchomes, nchomes_fldzn, by = "ImportParcelID", all.x = TRUE)
rm(nchomes_fldzn)
nchomes[, SFHA_TF := SFHA_TF == "T"]

## Handle multiple assessment records in a year. Keep the most recent one
nchomes[, record_date := my(ExtractDate)]
nchomes[, record_year := year(record_date)]
setorder(nchomes, ImportParcelID, record_date)
nchomes <- unique(nchomes, by=c("ImportParcelID", "record_year"), fromLast=TRUE)

sfha_homes <- nchomes[SFHA_TF == TRUE]

tract_mode <- function(tracts) {
  if (length(tracts) == uniqueN(tracts)) return(tail(tracts, n = 1))
  return(tail(Mode(tracts), n = 1))
}

## TODO: handle changes in census tracts better
## If a house is listed in different tracts in different years, take the modal tract. For ties, take the later
sfha_homes[, censusTract := as.integer64(tract_mode(as.numeric(as.character(censusTract)))), by = .(ImportParcelID)]

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

flood_count <- claims[sfha == TRUE, .(flood_count = .N), by = .(censusTract, floodZone, year = year(yearofLoss))]
flood_count[, tract_zone := paste(censusTract, floodZone, sep="_")]
setkey(flood_count, tract_zone, year)

flood_panel <- merge(flood_count, flood_frame, all = TRUE)

rm(flood_count, flood_frame)
flood_panel[is.na(flood_count), c("censusTract", "floodZone") := tstrsplit(tract_zone, "_", fixed = TRUE)]
flood_panel[is.na(flood_count), flood_count := 0]
flood_panel[, flood_event := flood_count > 0]
flood_panel[, tract_zone := NULL]

## ---- nfip-policy-holders ----
policies_frame <- CJ(tract_zone_year = unique(c(sfha_homes[, unique(paste(censusTract, FLD_ZONE, YearBuilt, sep="_"))],
                                                policies[sfha == TRUE, unique(paste(censusTract, floodZone, originalConstructionDate, sep="_"))])),
                     year = policies[, unique(policyyear)])

policies_status <- policies[sfha == TRUE, .(policies_count = .N, postFIRM = (mean(postFIRMConstructionIndicator) > 0.5)),
                            by = .(censusTract, floodZone, originalConstructionDate, year = policyyear)]
policies_status[, tract_zone_year := paste(censusTract, floodZone, originalConstructionDate, sep="_")]
setkey(policies_status, tract_zone_year, year)

policies_panel <- merge(policies_status, policies_frame, all = TRUE)
policies_panel[is.na(policies_count), c("censusTract", "floodZone", "
                                        originalConstructionDate") := tstrsplit(tract_zone_year, "_", fixed = TRUE)]
policies_panel[, tract_zone_year := NULL]
policies_panel[is.na(policies_count), policies_count := 0]
rm(policies_frame, policies_status)

# TODO: Find other data on year of initial FIRM
FIRMdates <- policies_panel[postFIRM == TRUE, .(FIRMyear = min(originalConstructionDate, na.rm = TRUE)),
                            keyby = .(censusTract, floodZone)]
FIRMdates[FIRMyear < 1974, FIRMyear := 1974]
policies_panel[, postFIRM := NULL]
setnames(policies_panel, "originalConstructionDate", "YearBuilt")
setkey(policies_panel, censusTract, floodZone, YearBuilt, year)

## ---- combine-panels ----

tzy_panel <- nctrans_panel[, .(properties_count = .N, TotalAssessedValue = mean(TotalAssessedValue),
                               TotalMarketValue = mean(TotalMarketValue),
                               transaction_prob = mean(transaction_obs), SalesPriceAmount = mean(SalesPriceAmount)),
                           keyby = .(censusTract, floodZone, YearBuilt, year)]

tzy_panel <- merge(tzy_panel, policies_panel, all.x = TRUE)
tzy_panel <- merge(tzy_panel, flood_panel, by = c("censusTract", "floodZone", "year"), all.x = TRUE)
tzy_panel <- merge(tzy_panel, FIRMdates, by = c("censusTract", "floodZone"), all.x = TRUE)
setkey(tzy_panel, censusTract, floodZone, YearBuilt, year)

tzy_panel[, policy_prob := policies_count / properties_count]

# Create the lag flood policy and flood events data
tzy_panel[, policies_L1 := shift(policies_count, 1, type = "lag"), by = .(censusTract, floodZone, YearBuilt)]
tzy_panel[, policies_L2 := shift(policies_count, 2, type = "lag"), by = .(censusTract, floodZone, YearBuilt)]
tzy_panel[, policies_L3 := shift(policies_count, 3, type = "lag"), by = .(censusTract, floodZone, YearBuilt)]
tzy_panel[, flood_L1 := shift(flood_event, 1, type = "lag"), by = .(censusTract, floodZone, YearBuilt)]
tzy_panel[, flood_L2 := shift(flood_event, 2, type = "lag"), by = .(censusTract, floodZone, YearBuilt)]
tzy_panel[, flood_L3 := shift(flood_event, 3, type = "lag"), by = .(censusTract, floodZone, YearBuilt)]
tzy_panel[, policy_prob_L1 := policies_L1 / properties_count]
tzy_panel[, policy_prob_L2 := policies_L2 / properties_count]
tzy_panel[, policy_prob_L3 := policies_L3 / properties_count]

# year range: 2009-2016 (2017 is only thru September)
