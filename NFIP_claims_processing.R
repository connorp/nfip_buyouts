## Connor P. Jackson second year paper
## cpjackson@berkeley.edu
## Exploratory code: FEMA National Flood Insurance Program

library(data.table)
library(sf)

## ---- import-nfip-claims ----

assemble_claims <-function(path="../data_buyouts/FimaNfipClaims.csv", state=NULL) {
  claims <- fread(path, stringsAsFactors = TRUE)
  state_filter <- state

  validclaims <-
    claims[occupancyType == 1 & totalBuildingInsuranceCoverage > 0 &
             totalBuildingInsuranceCoverage <= 250000 & amountPaidOnBuildingClaim > 0 &
             amountPaidOnBuildingClaim <= 250000 &
             amountPaidOnBuildingClaim <= totalBuildingInsuranceCoverage]
  if (is.null(state_filter)) {
    return(validclaims)
  } else {
    return(validclaims[state == state_filter])
  }
}

# Need to handle negative claim amounts and corresponding duplicate claim payments

claims <- assemble_claims(state="NC")
claims[abs(elevationDifference) == 999, elevationDifference := NA]
claims[, floodZone := as.character(floodZone)]
claims[, sfha := grepl("A|V", floodZone)]
claims[censusTract < 37000000000, censusTract := NA]
claims <- claims[!is.na(censusTract)]
# todo: attempt to recreate missing census tracts from zipcodes/latLongs

## ---- import-nfip-policies ----

policies <- fread("../data_buyouts/policies.csv")
policies[censusTract < 37000000000 | censusTract >= 38000000000, censusTract := NA]
policies <- policies[!is.na(censusTract) & occupancyType == 1 &
                     totalBuildingInsuranceCoverage > 0 & totalBuildingInsuranceCoverage <= 250000]
policies[, originalConstructionDate := year(originalConstructionDate)]
policies[originalConstructionDate > year(Sys.time()), originalConstructionDate := NA]
policies <- policies[!is.na(originalConstructionDate)]
policies[, policyyear := year(policyEffectiveDate)]
policies <- policies[policyyear < year(Sys.time())]  # drop the handful of 2021 policies
policies[elevationDifference < -100 | elevationDifference > 200, elevationDifference := NA]
policies[, sfha := grepl("A|V", floodZone)]

# dates: policyEffectiveDate, originalNBDate, policyTerminationDate, (cancellationDateOfFloodPolicy)

## ---- import-NFHL-zones ----

nfhl <- sf::st_read("../data_buyouts/NFHL/NFHL_37_20201206.gdb", "S_Fld_Haz_Ar")

# keep only the needed columns
nfhl_cols <- c("DFIRM_ID", "FLD_AR_ID", "FLD_ZONE", "SFHA_TF", "STATIC_BFE", "DEPTH", "LEN_UNIT", "SHAPE")
nfhl <- nfhl[nfhl_cols]
