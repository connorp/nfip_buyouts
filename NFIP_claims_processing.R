## Connor P. Jackson second year paper
## cpjackson@berkeley.edu
## Exploratory code: FEMA National Flood Insurance Program

library(data.table)
library(sf)

## ---- import-nfip-claims ----

assemble_claims <-function(path="../data_buyouts/FimaNfipClaims.csv", state=NULL) {
  claims <- fread(path, stringsAsFactors = TRUE)

  validclaims <-
    claims[occupancyType == 1 & totalBuildingInsuranceCoverage > 0 &
             totalBuildingInsuranceCoverage <= 250000 & amountPaidOnBuildingClaim > 0 &
             amountPaidOnBuildingClaim <= 250000 &
             amountPaidOnBuildingClaim <= totalBuildingInsuranceCoverage]
  if (is.null(state)) {
    return(validclaims)
  } else {
    return(validclaims[state == ..state])
  }
}

claims <- assemble_claims(state="NC")

## ---- import-NFHL-zones ----

# nfhl_layers <- st_layers("../nfip_data/NFHL/NFHL_48_20201014.gdb")
nfhl <- sf::st_read("../nfip_data/NFHL/NFHL_48_20201014.gdb", "S_Fld_Haz_Ar")
