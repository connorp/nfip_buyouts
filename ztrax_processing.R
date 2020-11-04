## Connor P. Jackson second year paper
## cpjackson@berkeley.edu
## ZTRAX data import and processing

library(data.table)
library(demogztrax)

## ---- import-ztrax-assessment ----
ZAsmt_struct <- demogztrax::layout_spec$Zillow_Assessor.ZAsmt

cols_main <- c(1,4,5,27,28,30,37,82,83)
colnames_main <- ZAsmt_struct[TableName == "utMain" & column_id %in% cols_main, FieldName]

cols_building <- c(1,15)
colnames_bldg <- ZAsmt_struct[TableName == "utBuilding" & column_id %in% cols_building, FieldName]

colnames_value <- ZAsmt_struct[TableName == "utValue", FieldName]
colnames_all <- unique(c(colnames_main, colnames_bldg, colnames_value))

nchomes <- ztrax_fread(branch="Zasmt",state="NC", keeptabs = c("utMain", "utBuilding", "utValue"),
                       keepvars = colnames_all)

# Manually import some data?
# nc <- fread("~/ZTRAX/clean/2019/Zillow_Assessor/37/ZAsmt/Main.txt",
#             sep="|", col.names = colnames_nc_main, select = cols_main, header = FALSE)
