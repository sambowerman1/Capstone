states<- read.csv("~/Desktop/all_states_summarized.csv")
length(unique(states$state))

summarized<- states %>% filter(correct_person == "true")
library(ggplot2)
library(dplyr)
library(lubridate)
summarized$dob = ymd(summarized$dob)
summarized$dod = ymd(summarized$dod)
summarized$age_at_death = ((summarized$dod) - (summarized$dob))/365
summarized$year_at_death = year(summarized$dod)
summarized$year_at_birth = year(summarized$dob)


summarized %>% ggplot(aes(x = dob)) + geom_histogram()

summarized %>% ggplot(aes(x = year_at_birth)) + geom_histogram()


summarized %>% ggplot(aes(x = age_at_death)) + geom_histogram()

summarized %>% ggplot(aes(x = year_at_death, y = age_at_death)) + geom_point()

ai <- read.csv("~/Downloads/LLM Leaderboard - Sheet1.csv")


providers <- c("OpenAI","Anthropic","Xiaomi","xAI")
ai2 <- ai %>%  filter(Provider %in% providers)



ai2 %>% filter(Provider %in% providers) %>% 
  mutate(
    intelligence_index = suppressWarnings(
      as.numeric(gsub(",", "", trimws(as.character(Artificial.Analysis.Intelligence.Index))))
    )
  ) %>%
  filter(!is.na(intelligence_index), intelligence_index > 30, Price.Per.1M.Blended.Tokens != "$0.00") %>%
  mutate(
    Price_num = suppressWarnings(as.numeric(gsub("[$,]", "", Price.Per.1M.Blended.Tokens))),
    is_xiaomi = grepl("Xiaomi", Model, ignore.case = TRUE)
  ) %>%
  ggplot(aes(x = intelligence_index, y = Price_num, label = Model)) +
  geom_point(aes(color = is_xiaomi, size = is_xiaomi)) +
  geom_label(aes(fill = is_xiaomi), color = "white", size = 3, show.legend = TRUE) +
  scale_color_manual(
    values = c(`FALSE` = "gray55", `TRUE` = "#E30613"),
    labels = c(`FALSE` = "Other", `TRUE` = "Xiaomi"),
    name = NULL
  ) +
  scale_fill_manual(
    values = c(`FALSE` = "gray55", `TRUE` = "#E30613"),
    labels = c(`FALSE` = "Other", `TRUE` = "Xiaomi"),
    name = NULL
  ) +
  scale_size_manual(values = c(`FALSE` = 2, `TRUE` = 4.5), guide = "none") +
  guides(color = guide_legend(), fill = "none") +
  labs(
    x = "Artificial Analysis Intelligence Index",
    y = "Price per 1M Blended Tokens"
  )


ai2$Price.Per.1M.Blended.Tokens <- as.numeric(gsub("\\$", "", ai2$Price.Per.1M.Blended.Tokens))

ai2 %>% filter(Provider %in% providers) %>% 
  mutate(
    intelligence_index = suppressWarnings(
      as.numeric(gsub(",", "", trimws(as.character(Artificial.Analysis.Intelligence.Index))))
    )
  ) %>%
  filter(!is.na(intelligence_index), intelligence_index > 20, Price.Per.1M.Blended.Tokens > 0.0 ) %>% 
  ggplot(aes(x = intelligence_index, y = Price.Per.1M.Blended.Tokens, color = Provider, label = Model)) + geom_point() + scale_y_log10(labels = scales::dollar_format()) + geom_label()

