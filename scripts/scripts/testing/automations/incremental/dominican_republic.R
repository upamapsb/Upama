url <- read_html("https://www.msp.gob.do/web/?page_id=6948") %>%
    html_node(".downloadlink") %>%
    html_attr("href") %>%
    paste0("https://www.msp.gob.do", .)

download.file(
    url = url,
    destfile = "tmp/tmp.pdf",
    quiet = TRUE,
    method = "curl",
    extra = "-k"
)

date <- pdf_text("tmp/tmp.pdf") %>%
    str_extract_all("\\d+/\\d+/\\d{4}") %>%
    unlist() %>%
    dmy() %>%
    max()

count <- pdf_text("tmp/tmp.pdf")[2] %>%
    str_extract("Total\\s+[\\d,]+") %>%
    str_extract("[\\d,]+$") %>%
    str_replace_all("[^\\d]", "") %>%
    as.integer()

add_snapshot(
    count = count,
    date = date,
    sheet_name = "Dominican Republic",
    country = "Dominican Republic",
    units = "samples tested",
    testing_type = "PCR only",
    source_url = url,
    source_label = "Ministry of Public Health and Social Assistance"
)

file.remove("tmp/tmp.pdf")
