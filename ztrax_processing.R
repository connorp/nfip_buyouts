## Connor P. Jackson second year paper
## cpjackson@berkeley.edu
## ZTRAX data import and processing

library(data.table)
library(lubridate)
library(demogztrax)

## ---- import-ztrax-assessment ----
ZAsmt_struct <- demogztrax::layout_spec$Zillow_Assessor.ZAsmt

cols_main <- c(1:3,5:8,27,28,30,82:84)
colnames_main <- ZAsmt_struct[TableName == "utMain" & column_id %in% cols_main, FieldName]

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

fwrite(nchomes, "../data_buyouts/ZAsmt_NC.csv")

# Manually import some data?
# nc <- fread("~/ZTRAX/clean/2019/Zillow_Assessor/37/ZAsmt/Main.txt",
#             sep="|", col.names = colnames_nc_main, select = cols_main, header = FALSE)

## ---- import-ztrax-transactions ----
Ztrans_struct <- demogztrax::layout_spec$Zillow_Transaction.ZTrans

cols_trans <- c(1,2,5,7,17:20,25,27:32,60:63,66,68,102)
colnames_trans <- Ztrans_struct[TableName == "utMain" & column_id %in% cols_trans, FieldName]
colnames_trans <- c(colnames_trans, "ImportParcelID")
trans_files <- c("utMain", "utPropertyInfo")

nctrans <- ztrax_fread("ZTranTran", state="NC", keeptabs = trans_files,
                       keepvars = colnames_trans)

for (table in names(nctrans)) setkey(nctrans[[table]], TransId, FIPS)

nctrans <- merge(nctrans$Main.txt, nctrans$PropertyInfo.txt, all.x = TRUE)

nctrans <- nctrans[ImportParcelID %in% nchomes[, ImportParcelID]]
# Will need to identify transactions that lack ImportParcelID

fwrite(nctrans, "../data_buyouts/ZTrans_NC.csv")
