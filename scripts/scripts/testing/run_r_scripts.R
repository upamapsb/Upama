suppressPackageStartupMessages({
    library(data.table)
    library(googledrive)
    library(googlesheets4)
    library(httr)
    library(imputeTS)
    library(lubridate)
    library(pdftools)
    library(retry)
    library(rjson)
    library(rvest)
    library(stringr)
    library(tidyr)
})
rm(list = ls())

SKIP <- c("south_sudan")

args <- commandArgs(trailingOnly=TRUE)
execution_mode <- args[1]

if (length(SKIP) > 0) warning("Skipping the following countries: ", paste0(SKIP, collapse = ", "))

CONFIG <- rjson::fromJSON(file = "testing_dataset_config.json")
`_` <- Sys.setlocale("LC_TIME", "en_US")

add_snapshot <- function(count, sheet_name, country, units, date = today() - 1,
                         source_url, source_label, testing_type = NA_character_,
                         notes = NA_character_, daily_change = NA_integer_) {

    prev <- fread(file = sprintf("automated_sheets/%s.csv", sheet_name))
    prev[, Date := as.character(Date)]

    stopifnot(!is.na(date))
    stopifnot(is.integer(count))
    stopifnot(!is.na(count))
    stopifnot(units %in% c("people tested", "samples tested", "tests performed", "units unclear", "tests performed (CDC)"))
    stopifnot(length(count) == 1)
    stopifnot(count >= max(prev$`Cumulative total`, na.rm = TRUE))

    if (count == max(prev$`Cumulative total`, na.rm = TRUE)) {
        return(FALSE)
    }

    new <- data.table(
        Country = country,
        Units = units,
        Date = as.character(date),
        `Cumulative total` = count,
        `Source URL` = source_url,
        `Source label` = source_label,
        Notes = notes
    )

    if (!is.na(daily_change)) {
        new[, `Daily change in cumulative total` := daily_change]
    }

    df <- rbindlist(list(prev, new), use.names = TRUE)
    setorder(df, Date)
    df <- df[, .SD[1], Date]

    fwrite(df, sprintf("automated_sheets/%s.csv", sheet_name))
}

make_monotonic <- function(df) {
    # Forces time series to become monotonic.
    # The algorithm assumes that the most recent values are the correct ones,
    # and therefore removes previous higher values.
    setDT(df)
    setorder(df, Date)
    while (any(df$`Cumulative total` - shift(df$`Cumulative total`) < 0, na.rm = TRUE)) {
        diff <- shift(df$`Cumulative total`, -1) - df$`Cumulative total`
        df <- df[diff >= 0 | is.na(diff)]
    }
    return(df)
}

scripts_path <- ifelse(execution_mode == "quick", "automations/incremental", "automations")
scripts <- list.files(scripts_path, pattern = "\\.R$", full.names = TRUE, include.dirs = FALSE, recursive = TRUE)
if (length(SKIP) > 0) scripts <- scripts[!str_detect(scripts, paste(SKIP, collapse = "|"))]

for (s in scripts) {
    rm(list = setdiff(ls(), c("scripts", "add_snapshot", "s", "CONFIG", "make_monotonic")))
    message(sprintf("%s - %s", Sys.time(), s))
    try(source(s))
}
