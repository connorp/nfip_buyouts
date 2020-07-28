## Connor P. Jackson second year paper
## cpjackson@berkeley.edu
## Exploratory code: FEMA National Flood Insurance Program

library(data.table)
library(jsonlite)
library(crul)

policies_path <- "https://www.fema.gov/api/open/v1/FimaNfipPolicies"
claims_path <- "https://www.fema.gov/api/open/v1/FimaNfipClaims"

policies_columns <- "censusTract,countyCode,elevationDifference,latitude,longitude,policyCost,policyEffectiveDate,reportedCity"

query_FEMA_API <- function(API_url, columns = NULL, rowfilter = NULL, perQuery = 1000, queryCap = NULL) {
  # First find out how long the data are
  initial_resp <- GET(url = API_url,
                      query = list(`$inlinecount` = 'allpages',
                                   `$top` = 1,
                                   `$filter` = rowfilter))
  initial_res <- fromJSON(content(policies_response, as = "text"))
  N <- initial_res$metadata$count

  # If we don't want to pull the full table
  if (!is.null(queryCap)) {
    N <- min(N, queryCap)
  }

  # Build the vector of URLs to query
  assemble_url <- function(skipN) {
    # Assemble a single URL with the appropriate number of rows skipped
    final_url <- modify_url(url = API_url,
                            query = list(`$skip` = skipN,
                                         `$filter` = rowfilter,
                                         `$select` = columns,
                                         `$top` = perQuery))
    return(final_url)
  }

  # Assemble a vector of URLs with the same criteria but per page
  query_starts <- seq(from = 0, to = N, by = perQuery)
  query_urls <- sapply(query_starts, assemble_url)

  # Query the URLs asynchronously
  Async_client <- Async$new(urls = query_urls)
  query_results <- Async_client$get()

  parse_query <- function(query_result) {
    query_outputs <- fromJSON(query_result$parse("UTF-8"))
    # Need to get the name of the two attributes, and use the second one
    tablename <- names(query_outputs)[2]
    return(query_outputs[[tablename]])
  }

  outtable <- rbindlist(lapply(query_results, parse_query), fill = TRUE)
  return(outtable)
}
