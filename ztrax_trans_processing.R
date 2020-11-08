## Connor P. Jackson second year paper
## cpjackson@berkeley.edu
## ZTRAX Transaction data import and processing

library(data.table)
library(demogztrax)

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
