## Connor P. Jackson second year paper
## cpjackson@berkeley.edu
## Merge and analyze the data

library(data.table)
library(bit64)
library(lubridate)
library(tidyr)
library(sf)

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

# construct the panel dataset by filling in every parcel x year combination
panel_frame <-CJ(ImportParcelID = sfha_homes[, unique(ImportParcelID)], year = nctrans_armslen[, unique(year)])
keepvars <- c("ImportParcelID", "date", "year", "SalesPriceAmount", "mortgage_nfip_req")
nctrans_panel <- merge(nctrans_armslen[, ..keepvars], panel_frame, all = TRUE)
rm(nctrans_armslen, panel_frame)

## We apply the assessor data onto the transaction panel, assuming the old assessment records
## hold until a new one occurs
nctrans_panel[, roll_year := year]
setkey(nctrans_panel, ImportParcelID, roll_year)
sfha_homes[, roll_year := record_year]
setkey(sfha_homes, ImportParcelID, roll_year)

nctrans_panel <- sfha_homes[nctrans_panel, roll = TRUE, rollends = TRUE]

## ---- flood-events ----
flood_frame <- CJ(censusTract = unique(c(sfha_homes[, unique(censusTract)], claims[sfha == TRUE, unique(censusTract)])),
                  floodZone = claims[sfha == TRUE, unique(floodZone)], year = claims[, unique(year(yearofLoss))])
flood_count <- claims[sfha == TRUE, .N, keyby = .(censusTract, floodZone, year = year(yearofLoss))]
flood_panel <- merge(flood_count, flood_frame, all = TRUE)
flood_panel[is.na(N), N := 0]
flood_panel[, flood_event := N > 0]
