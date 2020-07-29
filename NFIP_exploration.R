## Connor P. Jackson second year paper
## cpjackson@berkeley.edu
## Exploratory code: FEMA National Flood Insurance Program

library(data.table)
library(jsonlite)
library(httr)
library(crul)
library(parallel)

policies_path <- "https://www.fema.gov/api/open/v1/FimaNfipPolicies"
claims_path <- "https://www.fema.gov/api/open/v1/FimaNfipClaims"

query_FEMA_API <- function(API_url, columns = NULL, rowfilter = NULL, perQuery = 1000,
                           queryCap = NULL, times = 5) {
  # First find out how long the data are
  initial_resp <- httr::GET(url = API_url,
                            query = list(`$inlinecount` = 'allpages',
                                         `$top` = 1,
                                         `$filter` = rowfilter))
  parsed_result <- jsonlite::fromJSON(httr::content(initial_resp, as = "text"))
  N <- parsed_result$metadata$count

  # If we don't want to pull the full table
  if (!is.null(queryCap)) {
    N <- min(N, queryCap)
  }

  # Build the vector of URLs to query
  assemble_url <- function(skipN) {
    # Assemble a single URL with the appropriate number of rows skipped
    final_url <- httr::modify_url(url = API_url,
                                  query = list(`$skip` = skipN,
                                               `$filter` = rowfilter,
                                               `$select` = columns,
                                               `$top` = perQuery))
    return(final_url)
  }

  # Assemble a vector of URLs with the same criteria but per page
  page_indices <- seq(from = 0, to = N, by = perQuery)
  query_urls <- sapply(page_indices, assemble_url)

  # Query the URLs asynchronously
  Async_client <- Async$new(urls = query_urls)

  query_results <- list()

  for (i in 1:times) {
    # a retry loop for getting failed pages of the API

    # Send the GET request
    query_responses <- Async_client$get()

    # Identify the queries that succeeded
    query_success <- sapply(query_responses, function(z) z$success())

    # save the successful queries
    query_results <- append(query_results, query_responses[query_success])

    # Identify the failed queries, prepare to reissue them
    query_urls <- query_urls[!query_success]
    Async_client$urls <- query_urls

    # We are done if no queries failed
    if (length(query_urls) == 0) break

    # otherwise, wait an exponential decay time (with jitter) and retry the failed ones
    Sys.sleep(stats::runif(1, min = 1, max = min(2^i, 60)))
  }

  parse_query <- function(query_result) {
    query_outputs <- jsonlite::fromJSON(query_result$parse("UTF-8"))
    # Need to get the name of the two attributes, and use the second one
    tablename <- names(query_outputs)[2]
    return(query_outputs[[tablename]])
  }

  outtable <- rbindlist(mclapply(query_results, parse_query, mc.cores = 3), fill = TRUE)
  return(outtable)
}
