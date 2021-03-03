## Connor P. Jackson second year paper
## cpjackson@berkeley.edu
## ZTRAX data import and processing

library(data.table)
library(lubridate)
library(demogztrax)
library(ggmap)

## ---- import-ztrax-assessment ----
ZAsmt_struct <- demogztrax::layout_spec$Zillow_Assessor.ZAsmt

cols_main <- c(1:3,5:8,27,28,30,82:84)
colnames_main <- ZAsmt_struct[TableName == "utMain" & column_id %in% cols_main, FieldName]

# TODO: add number of stories
cols_building <- c(1,6,15,16,46)
colnames_bldg <- ZAsmt_struct[TableName == "utBuilding" & column_id %in% cols_building, FieldName]

cols_value <- c(1:9,14)
colnames_value <- ZAsmt_struct[TableName == "utValue" & column_id %in% cols_value, FieldName]
colnames_all <- unique(c(colnames_main, colnames_bldg, colnames_value))

file_names <- c("utMain", "utBuilding", "utValue")

nchomes <- ztrax_fread(branch="Zasmt",state="NC", keeptab=file_names,
                       keepvars = colnames_all)

for (table in names(nchomes)) setkey(nchomes[[table]], RowID, FIPS)

nchomes <- Reduce(function(...) merge(..., all = T), nchomes)

home_types <- c("RR000","RR101","RR102","RR103","RR104","RR105","RR106","RR107",
                "RR108","RR109","RR110","RR111","RR112","RR113","RR114","RR115",
                "RR116","RR117","RR118","RR119","RR120")

nchomes <- nchomes[PropertyLandUseStndCode %in% home_types]

# Identify the most recent assessment records
nchomes[, latest := (.I[which.max(Edition)] == .I), by = .(ImportParcelID)]
nchomes[, record_date := parse_date_time(ExtractDate, "mY")]

# Handle empty strings
nchomes[PropertyFullStreetAddress == "", PropertyFullStreetAddress := NA]
nchomes[PropertyCity == "", PropertyCity := NA]
nchomes[PropertyAddressCensusTractAndBlock == "", PropertyAddressCensusTractAndBlock := NA]

# # Handle the missing LatLongs for some entries
# # Make sure the Google API key is set
# register_google(Sys.getenv("GOOGLE_API"))
#
# # Geocode the cluster locations we only have addresses for (convert address to lat/long)
# nchomes[(!is.na(PropertyFullStreetAddress) & !is.na(PropertyCity) & !is.na(PropertyZip) & is.na(PropertyAddressLatitude)),
#         c("longitude", "latitude") := geocode(paste(PropertyFullStreetAddress, PropertyCity, "NC", PropertyZip))]
#

## TODO: Geocode the ~30% of listings which are not geocoded, or match from other
##       records for that parcel

## ----apply-NFHL ----

nfhl <- sf::st_read("../data_buyouts/NFHL/NFHL_37_20201206.gdb", "S_Fld_Haz_Ar")

# keep only the needed columns
nfhl_cols <- c("DFIRM_ID", "FLD_AR_ID", "FLD_ZONE", "SFHA_TF", "STATIC_BFE", "DEPTH", "LEN_UNIT", "SHAPE")
nfhl <- nfhl[nfhl_cols]

### TODO: Geocode missing latlongs
# Can't assign a flood zone without Lat/Long
nchomes <- nchomes[!is.na(PropertyAddressLatitude)]

# Census tract format: 2 digit state + 3 digit county +
# 6 digit tract (maybe containing a decimal) + 4 digit block
# Remove obviously incorrect census blocks and fix formatting
nchomes[!grepl("^37", PropertyAddressCensusTractAndBlock), PropertyAddressCensusTractAndBlock := NA]
nchomes[nchar(PropertyAddressCensusTractAndBlock) < 10, PropertyAddressCensusTractAndBlock := NA]
nchomes[, censusTract := as.integer64(gsub(".", "", substring(PropertyAddressCensusTractAndBlock, 1, 12), fixed = TRUE))]

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
nchomes[, geometry := NULL]

fwrite(nchomes, "../data_buyouts/ZAsmt_NC.csv")

## ---- import-ztrax-transactions ----
Ztrans_struct <- demogztrax::layout_spec$Zillow_Transaction.ZTrans

cols_trans <- c(1,2,5:7,17,18,25:32,60:63,66,68,102)
colnames_trans <- Ztrans_struct[TableName == "utMain" & column_id %in% cols_trans, FieldName]
colnames_trans <- c(colnames_trans, "ImportParcelID")
trans_files <- c("utMain", "utPropertyInfo")

nctrans <- ztrax_fread("ZTranTran", state="NC", keeptabs = trans_files,
                       keepvars = colnames_trans)

for (table in names(nctrans)) setkey(nctrans[[table]], TransId, FIPS)

nctrans <- merge(nctrans$Main.txt, nctrans$PropertyInfo.txt, all.x = TRUE)

### TODO: handle transactions missing ImportParcelID
nctrans <- nctrans[ImportParcelID %in% nchomes[, ImportParcelID]]
# Implicitly drops all transactions missing ImportParcelID

fwrite(nctrans, "../data_buyouts/ZTrans_NC.csv")
