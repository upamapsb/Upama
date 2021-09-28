df <-
  fread(
    "https://raw.githubusercontent.com/COVID19-Malta/COVID19-Data/master/COVID-19%20Malta%20-%20COVID%20Tests.csv",
    select = c("Publication date", "Total PCR and rapid antigen tests"),
    showProgress = FALSE
  )

setnames(
  df,
  c("Publication date", "Total PCR and rapid antigen tests"),
  c("Date", "Cumulative total")
)

df[, Country := "Malta"]
df[, Date := dmy(Date)]
df[, `Source URL` := "https://github.com/COVID19-Malta/COVID19-Data/blob/master/COVID-19%20Malta%20-%20COVID%20Tests.csv"]
df[, `Source label` := "COVID-19 Public Health Response Team (Ministry for Health)"]
df[, Notes := NA_character_]
df[, Units := "tests performed"]

fwrite(df, "automated_sheets/Malta.csv")
