states<- read.csv("~/Desktop/all_states_summarized.csv")
length(unique(states$state))

summarized<- states %>% filter(correct_person == "true")
library(ggplot2)
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


