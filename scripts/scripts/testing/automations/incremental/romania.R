links <- read_html("https://gov.ro/ro/media/comunicate") %>%
  html_nodes(".DescriptionList h2 a") %>%
  html_attr("href")

url <- links[str_detect(links, "buletin")][1]

url <- read_html(url) %>%
  html_node(".filesBoxInner a") %>%
  html_attr("href")

date <- str_extract(url, "\\d{2}-\\d{2}") %>%
  as.Date(format = "%d-%m")
if (is.na(date)) date <- today()

url <- str_replace(url, "http\\://https", "https")

download.file(url = url, destfile = "tmp/tmp.pdf", quiet = TRUE, mode = 'wb')

pcr <- pdf_text("tmp/tmp.pdf") %>%
  str_extract("au fost prelucrate?.*") %>%

  na.omit() %>%
  str_replace_all("[^\\d]", "") %>%
  as.integer()

ag <- pdf_text("tmp/tmp.pdf") %>%
  str_extract("teste RT-PCR și .* .*teste") %>%

  na.omit() %>%
  str_replace_all("[^\\d]", "") %>%
  as.integer()

count <- pcr + ag

add_snapshot(
  count = count,
  date = date,
  sheet_name = "Romania",
  country = "Romania",
  units = "tests performed",
  source_url = url,
  source_label = "Ministry of Internal Affairs"
)
