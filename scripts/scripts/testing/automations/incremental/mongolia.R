url <- "https://www1.e-mongolia.mn/covid-19"

data <- rjson::fromJSON(file = "https://e-mongolia.mn/shared-api/api/covid-stat/daily")

date <- data$data$lastDataDate
count <- as.integer(data$data$testedPcrTotal)

add_snapshot(
    count = count,
    date = date,
    sheet_name = "Mongolia",
    country = "Mongolia",
    units = "samples tested",
    source_url = url,
    source_label = "Ministry of Health"
)
