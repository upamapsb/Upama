process_page <- function(page) {
  date <- page %>%
    html_node(".dateDetalils") %>%
    html_text %>%
    str_remove("Postuar më: ") %>%
    dmy() %>%
    as.character()

  daily <- page %>%
    html_node("a") %>%
    html_text %>%
    str_extract("[\\d,]+ testime") %>%
    str_remove(" testime") %>%
    as.integer()

  source <- page %>%
    html_node("a") %>%
    html_attr("href")

  if (!is.na(daily)) {
    data.table(Date = date, "Daily change in cumulative total" = daily, "Source URL" = source)
  } else {
    return(NULL)
  }
}

url <- "https://shendetesia.gov.al/category/lajme/"

pages <- read_html(url) %>% html_nodes(".newsDetails")

df <- rbindlist(lapply(pages, FUN = process_page))

df[, Country := "Albania"]
df[, Units := "tests performed"]
df[, `Source label` := "Ministry of Health and Social Protection"]
df[, Units := "tests performed"]
df[, Notes := NA_character_]

existing <- fread("automated_sheets/Albania.csv")
existing <- existing[Date < min(df$Date)]
existing[, Date := as.character(Date)]

df <- rbindlist(list(existing, df), use.names = TRUE)
setorder(df, Date)

fwrite(df, "automated_sheets/Albania.csv")
