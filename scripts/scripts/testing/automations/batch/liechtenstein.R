context <- rjson::fromJSON(file = "https://www.covid19.admin.ch/api/data/context")
url <- context$sources$individual$csv$daily$testPcrAntigen

df <- fread(
    url,
    showProgress = FALSE,
    select = c("datum", "entries", "entries_pos", "nachweismethode", "geoRegion")
)
df <- df[geoRegion == "FL"]

df <- df[, .(
    entries = sum(entries, na.rm = TRUE),
    entries_pos = sum(entries_pos, na.rm = TRUE)
), datum]

setorder(df, datum)
df[, pr := round(frollsum(entries_pos, 7) / frollsum(entries, 7), 3)]
df <- df[, c("datum", "entries", "pr")]
setnames(df, c("datum", "entries", "pr"), c("Date", "Daily change in cumulative total", "Positive rate"))

df[, Country := "Liechtenstein"]
df[, Units := "tests performed"]
df[, `Source URL` := "https://opendata.swiss/en/dataset/covid-19-schweiz"]
df[, `Source label` := "Federal Office of Public Health"]
df[, Notes := NA_character_]

df <- df[`Daily change in cumulative total` > 0]

fwrite(df, "automated_sheets/Liechtenstein.csv")
