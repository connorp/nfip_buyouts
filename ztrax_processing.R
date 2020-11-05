## Connor P. Jackson second year paper
## cpjackson@berkeley.edu
## ZTRAX data import and processing

library(data.table)
library(demogztrax)
library(tigris)

## ---- import-ztrax-assessment ----
ZAsmt_struct <- demogztrax::layout_spec$Zillow_Assessor.ZAsmt

cols_main <- c(1,4,5,27,28,30,82,83)
colnames_main <- ZAsmt_struct[TableName == "utMain" & column_id %in% cols_main, FieldName]

cols_building <- c(1,15)
colnames_bldg <- ZAsmt_struct[TableName == "utBuilding" & column_id %in% cols_building, FieldName]

colnames_value <- ZAsmt_struct[TableName == "utValue", FieldName]
colnames_all <- unique(c(colnames_main, colnames_bldg, colnames_value))

file_names <- c("utMain", "utBuilding", "utValue")

read_assessment <- function(state, files=NULL, colnames=NULL){
  nchomes <- ztrax_fread(branch="Zasmt",state=state, keeptab=files,
                         keepvars = colnames)

  lapply(nchomes, function(table) setkey(table, RowID, FIPS, BatchID))
  return(Reduce(function(...) merge(..., all = T), nchomes))
}

nchomes <- read_assessment("NC", file_names, colnames_all)
nctracts <- tracts(37)

fwrite(nchomes, "../data_buyouts/ZAsmt_NC.csv")

# Manually import some data?
# nc <- fread("~/ZTRAX/clean/2019/Zillow_Assessor/37/ZAsmt/Main.txt",
#             sep="|", col.names = colnames_nc_main, select = cols_main, header = FALSE)
