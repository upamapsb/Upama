url <- "https://www.moh.gov.sg/covid-19/statistics"

page <- read_html(url)

date <- page %>% html_nodes("h3") %>%
    html_text() %>%
    str_subset("Tested") %>%
    str_extract("as .. [^)]+") %>%
    str_replace("as .. ", "") %>%
    na.omit() %>%
    dmy()

tables <- page %>% html_nodes("table")
idx <- which(str_detect(html_text(tables), "Total Swabs Tested"))
count <- tables[idx] %>%
    html_nodes("td") %>%
    tail(1) %>%
    html_text() %>%
    str_replace_all("[^\\d]", "") %>%
    as.integer()

add_snapshot(
    count = count,
    date = date,
    sheet_name = "Singapore",
    country = "Singapore",
    units = "samples tested",
    testing_type = "PCR only",
    source_url = url,
    source_label = "Ministry of Health"
)
