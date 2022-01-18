options(scipen = 999)

# Tests
url <- "http://www.chp.gov.hk/files/misc/statistics_on_covid_19_testing_cumulative.csv"

df <- fread(url, showProgress = FALSE)

setnames(df, c("from", "Date", "t1", "t2", "t3", "t4", "t5"))
df[, change := rowSums(df[, c("t1", "t2", "t3", "t4", "t5")], na.rm = TRUE)]

df[, from := NULL]
df[, Date := dmy(Date)]

df <- df[, .(change = sum(change, na.rm = TRUE)), Date]

setorder(df, Date)
df[, `Cumulative total` := cumsum(change)]
df <- df[, c("Date", "Cumulative total")]

# Cases
cases <- fread(
    "http://www.chp.gov.hk/files/misc/latest_situation_of_reported_cases_covid_19_eng.csv",
    select = c("As of date", "Number of confirmed cases"), showProgress = FALSE
)
setnames(cases, "As of date", "Date")
cases[, Date := dmy(Date)]

# Positive rate
df <- merge(df, cases, "Date", all.x = TRUE)
setorder(df, Date)
df[, cases_over_period := `Number of confirmed cases` - shift(`Number of confirmed cases`)]
df[, tests_over_period := `Cumulative total` - shift(`Cumulative total`)]
df[, `Positive rate` := round(cases_over_period / tests_over_period, 5)]
df[, c("cases_over_period", "tests_over_period", "Number of confirmed cases") := NULL]

df[, Country := "Hong Kong"]
df[, Units := "tests performed"]
df[, `Source URL` := url]
df[, `Source label` := "Department of Health"]
df[, Notes := NA_character_]

fwrite(df, "automated_sheets/Hong Kong.csv")
