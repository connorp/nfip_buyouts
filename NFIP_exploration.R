## Connor P. Jackson second year paper
## cpjackson@berkeley.edu
## Exploratory code: FEMA National Flood Insurance Program

library(data.table)
library(jsonlite)

claims <- fread("~/Google Drive (cpjackson@berkeley.edu)/Second year paper/FimaNfipClaims.csv",
                stringsAsFactors = TRUE)

validclaims <-
claims[occupancyType == 1 & totalBuildingInsuranceCoverage > 0 &
         totalBuildingInsuranceCoverage <= 250000 & amountPaidOnBuildingClaim > 0 &
         amountPaidOnBuildingClaim <= 250000 &
         amountPaidOnBuildingClaim <= totalBuildingInsuranceCoverage]

