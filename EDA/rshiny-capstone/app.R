# =============================================================================
# Memorial Highway Analysis — R Shiny App
# =============================================================================
# Install packages (run once):
#   install.packages(c("shiny","shinydashboard","tidyverse","scales",
#                      "ggrepel","patchwork","usmap","stringr",
#                      "forcats","DT","bslib"))
#
# Usage:
#   1. Place this file and all 6 CSVs in the same folder.
#   2. Open in RStudio and click "Run App", OR run:
#        shiny::runApp("path/to/folder")
# =============================================================================

library(shiny)
library(shinydashboard)
library(tidyverse)
library(scales)
library(ggrepel)
library(usmap)
library(stringr)
library(forcats)
library(lubridate)
library(DT)
library(lubridate)

# =============================================================================
# COLOURS  (Forest theme)
# =============================================================================
CLR_BG      <- "#f2f0eb"
CLR_SURFACE <- "#fafaf7"
CLR_TEXT    <- "#1c2818"
CLR_MUTED   <- "#5a6850"
CLR_FAINT   <- "#8a9880"
CLR_GOP     <- "#a84030"
CLR_DEM     <- "#2d5a3d"
CLR_AMBER   <- "#a87020"
CLR_TEAL    <- "#2d5a3d"
CLR_PURPLE  <- "#534AB7"
CLR_NEUTRAL <- "#b8b0a4"

# =============================================================================
# GGPLOT THEME
# =============================================================================
theme_hw <- function(base_size = 11) {
  theme_minimal(base_size = base_size) +
    theme(
      plot.background  = element_rect(fill = CLR_BG,      colour = NA),
      panel.background = element_rect(fill = CLR_SURFACE, colour = NA),
      panel.grid.major = element_line(colour = "#d8d4cc",  linewidth = 0.35),
      panel.grid.minor = element_blank(),
      axis.text        = element_text(colour = CLR_MUTED,  size = 9),
      axis.title       = element_text(colour = CLR_MUTED,  size = 9),
      plot.title       = element_text(colour = CLR_TEXT,   size = 12,
                                      face = "bold", margin = margin(b = 4)),
      plot.subtitle    = element_text(colour = CLR_MUTED,  size = 9,
                                      margin = margin(b = 10)),
      plot.caption     = element_text(colour = CLR_FAINT,  size = 8, hjust = 0),
      legend.text      = element_text(colour = CLR_MUTED,  size = 8),
      legend.title     = element_text(colour = CLR_TEXT,   size = 9),
      legend.background = element_rect(fill = CLR_BG,      colour = NA),
      strip.text       = element_text(colour = CLR_TEXT,   size = 9, face = "bold"),
      plot.margin      = margin(12, 16, 10, 12)
    )
}

# =============================================================================
# DATA LOADING & PREPARATION
# =============================================================================
# Helper — load with a fallback message if file not found
load_csv <- function(name) {
  path <- name
  if (!file.exists(path)) stop(paste("File not found:", path,
    "\nPlace the CSV files in the same directory as app.R"))
  read_csv(path, show_col_types = FALSE)
}

hw_raw      <- load_csv("all_states_summarized.csv")
states_demo <- load_csv("us_states_demographics.csv")
co_demo     <- load_csv("us_counties_demographics.csv")
elec24      <- load_csv("2024_US_County_Level_Presidential_Results.csv")
elec20      <- load_csv("2020_US_County_Level_Presidential_Results.csv")
elec16      <- load_csv("2016_US_County_Level_Presidential_Results.csv")

# ── Highway counts ────────────────────────────────────────────────────────
hw_counts <- hw_raw %>% count(state, name = "highways")

# ── Florida county demographic override ──────────────────────────────────
fl_valid_counties <- hw_raw %>%
  filter(state == "Florida", !is.na(county),
         !county %in% c("Multiple Counties", "Miami\u2010Dade", "Flager")) %>%
  pull(county) %>% unique()

fl_avg <- co_demo %>%
  filter(str_detect(County, "Florida")) %>%
  mutate(county_clean = str_remove(County, " County, Florida")) %>%
  filter(county_clean %in% fl_valid_counties) %>%
  summarise(across(c(Median_Age, Median_Household_Income, Unemployment_Rate,
                     Pct_Below_Poverty_Level, Pct_White_Alone, Pct_Black_Alone,
                     Pct_Hispanic, Pct_Asian_Alone, Pct_AIAN_Alone, Pct_TwoOrMore),
                   function(x) mean(x, na.rm = TRUE))) %>%
  mutate(State = "Florida")

demo_cols <- c("State","Median_Age","Median_Household_Income","Unemployment_Rate",
               "Pct_Below_Poverty_Level","Pct_White_Alone","Pct_Black_Alone",
               "Pct_Hispanic","Pct_Asian_Alone","Pct_AIAN_Alone","Pct_TwoOrMore",
               "HS_Grad_or_Higher","Bachelors_or_Higher")

state_demo <- states_demo %>%
  select(all_of(demo_cols)) %>%
  filter(State != "Florida") %>%
  bind_rows(fl_avg %>% select(any_of(demo_cols)))

# ── State abbreviation lookup ─────────────────────────────────────────────
abbr2state <- c(
  AL="Alabama", AK="Alaska", AZ="Arizona", AR="Arkansas", CA="California",
  CO="Colorado", CT="Connecticut", DE="Delaware", FL="Florida", GA="Georgia",
  HI="Hawaii", ID="Idaho", IL="Illinois", IN="Indiana", IA="Iowa",
  KS="Kansas", KY="Kentucky", LA="Louisiana", ME="Maine", MD="Maryland",
  MA="Massachusetts", MI="Michigan", MN="Minnesota", MS="Mississippi", MO="Missouri",
  MT="Montana", NE="Nebraska", NV="Nevada", NH="New Hampshire", NJ="New Jersey",
  NM="New Mexico", NY="New York", NC="North Carolina", ND="North Dakota", OH="Ohio",
  OK="Oklahoma", OR="Oregon", PA="Pennsylvania", RI="Rhode Island", SC="South Carolina",
  SD="South Dakota", TN="Tennessee", TX="Texas", UT="Utah", VT="Vermont",
  VA="Virginia", WA="Washington", WV="West Virginia", WI="Wisconsin", WY="Wyoming",
  DC="District of Columbia"
)

# ── Election aggregation ──────────────────────────────────────────────────
agg_elec <- function(df, col) {
  df %>%
    group_by(state_name = .data[[col]]) %>%
    summarise(vd = sum(votes_dem, na.rm=TRUE),
              vg = sum(votes_gop, na.rm=TRUE),
              vt = sum(total_votes, na.rm=TRUE), .groups="drop") %>%
    mutate(per_dem=vd/vt, per_gop=vg/vt,
           margin_gop=(per_gop-per_dem)*100)
}
s24 <- agg_elec(elec24, "state_name")
s20 <- agg_elec(elec20, "state_name")
s16 <- elec16 %>%
  group_by(state_abbr) %>%
  summarise(vd=sum(votes_dem,na.rm=TRUE), vg=sum(votes_gop,na.rm=TRUE),
            vt=sum(total_votes,na.rm=TRUE), .groups="drop") %>%
  mutate(state_name=abbr2state[state_abbr],
         per_dem=vd/vt, per_gop=vg/vt,
         margin_gop=(per_gop-per_dem)*100) %>%
  filter(!is.na(state_name))

# ── Master dataset ────────────────────────────────────────────────────────
master <- hw_counts %>%
  left_join(s24 %>% select(state_name, m24=margin_gop, d24=per_dem, g24=per_gop),
            by=c("state"="state_name")) %>%
  left_join(s20 %>% select(state_name, m20=margin_gop), by=c("state"="state_name")) %>%
  left_join(s16 %>% select(state_name, m16=margin_gop), by=c("state"="state_name")) %>%
  left_join(state_demo, by=c("state"="State")) %>%
  mutate(swing_20_24 = m24 - m20,
         winner24    = factor(if_else(m24>0,"GOP","DEM"), levels=c("GOP","DEM")))

# ── Honoree profiles per state ────────────────────────────────────────────
state_profiles <- hw_raw %>%
  group_by(state) %>%
  summarise(highways = n(),
            has_wiki  = sum(!is.na(wikipedia_url)),
            male      = sum(gender=="male",   na.rm=TRUE),
            female    = sum(gender=="female", na.rm=TRUE),
            military  = sum(involved_in_military=="yes", na.rm=TRUE),
            politics  = sum(involved_in_politics=="yes",  na.rm=TRUE),
            sports    = sum(involved_in_sports=="yes",    na.rm=TRUE),
            music     = sum(involved_in_music=="yes",     na.rm=TRUE),
            .groups="drop") %>%
  mutate(wiki_pct = has_wiki/highways)

# ── Honoree birth/death years ─────────────────────────────────────────────
eras <- hw_raw %>%
  mutate(dob_year  = as.integer(str_extract(dob, "\\d{4}")),
         dod_year  = as.integer(str_extract(dod, "\\d{4}")),
         lifespan  = dod_year - dob_year,
         birth_dec = (dob_year %/% 10) * 10,
         death_dec = (dod_year %/% 10) * 10)

# ── US state list for geography extraction ────────────────────────────────
us_state_names <- c(
  "Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut",
  "Delaware","Florida","Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa",
  "Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts","Michigan",
  "Minnesota","Mississippi","Missouri","Montana","Nebraska","Nevada",
  "New Hampshire","New Jersey","New Mexico","New York","North Carolina",
  "North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania","Rhode Island",
  "South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont",
  "Virginia","Washington","West Virginia","Wisconsin","Wyoming"
)
extract_state <- function(x) {
  if (is.na(x) || x == "not found") return(NA_character_)
  str_extract(x, paste(us_state_names, collapse="|"))
}
geo <- hw_raw %>%
  mutate(birth_state = map_chr(place_of_birth, extract_state),
         death_state = map_chr(place_of_death,  extract_state))

# ── Word frequency ────────────────────────────────────────────────────────
stopwords <- c("highway","memorial","route","road","bridge","avenue","boulevard",
               "dr","drive","street","freeway","parkway","expressway","corridor",
               "the","of","and","to","at","for","in","a","an","sr","us","state",
               "interstate","hwy","blvd","ave","iii","ii","jr")

word_freq <- hw_raw %>%
  filter(!is.na(highway_name)) %>%
  mutate(words = str_extract_all(str_to_lower(highway_name), "[a-z']+")) %>%
  unnest(words) %>%
  filter(!words %in% stopwords, str_length(words) > 2) %>%
  count(words, sort=TRUE)

# =============================================================================
# ODMP DATA PREP
# =============================================================================
parse_tour_years <- function(x) {
  x <- as.character(x)
  ifelse(
    is.na(x) | x == "Not available", NA_real_, {
      yrs <- suppressWarnings(
        as.numeric(sub(".*?(\\d+)\\s*year.*", "\\1", x, perl=TRUE)))
      mos <- suppressWarnings(
        as.numeric(sub(".*?(\\d+)\\s*month.*", "\\1", x, perl=TRUE)))
      # sub returns the original string unchanged if pattern doesn't match
      yrs_val <- ifelse(is.na(yrs) | yrs == as.numeric(x), 0, yrs)
      mos_val <- ifelse(is.na(mos) | mos == as.numeric(x), 0, mos / 12)
      # safer: use str_match which returns NA on no match
      yrs_m <- suppressWarnings(
        as.numeric(str_match(x, "(\\d+)\\s*year")[,2]))
      mos_m <- suppressWarnings(
        as.numeric(str_match(x, "(\\d+)\\s*month")[,2]))
      yrs_val2 <- ifelse(is.na(yrs_m), 0, yrs_m)
      mos_val2 <- ifelse(is.na(mos_m), 0, mos_m / 12)
      result   <- yrs_val2 + mos_val2
      ifelse(result == 0, NA_real_, round(result, 2))
    }
  )
}

group_cause <- function(x) {
  x <- str_to_lower(as.character(x))
  case_when(
    str_detect(x, "gunfire")                          ~ "Gunfire",
    str_detect(x, "automobile|vehicle pursuit|motorcycle") ~ "Vehicle crash",
    str_detect(x, "vehicular assault|struck by vehicle")   ~ "Struck by vehicle",
    str_detect(x, "aircraft")                         ~ "Aircraft",
    str_detect(x, "assault|stab|bomb")                ~ "Assault (other)",
    TRUE                                               ~ "Other / illness"
  )
}

odmp <- hw_raw %>%
  filter(!is.na(odmp_url)) %>%
  mutate(
    age_num    = as.numeric(ifelse(odmp_age == "Not available" | is.na(odmp_age),
                                   NA, odmp_age)),
    tour_years = parse_tour_years(odmp_tour),
    eow_date   = lubridate::mdy(odmp_end_of_watch),
    eow_year   = lubridate::year(eow_date),
    eow_month  = lubridate::month(eow_date, label = TRUE, abbr = TRUE),
    eow_decade = (eow_year %/% 10) * 10,
    cause_grp  = group_cause(odmp_cause),
    # Parse weapon from incident_details
    weapon     = case_when(
      str_detect(odmp_incident_details, regex("firearm|gun|shot|handgun|rifle|pistol",
                                              ignore_case=TRUE)) ~ "Firearm",
      str_detect(odmp_incident_details, regex("automobile|vehicle|car|truck",
                                              ignore_case=TRUE)) ~ "Vehicle",
      str_detect(odmp_incident_details, regex("knife|stab|blade",
                                              ignore_case=TRUE)) ~ "Knife/blade",
      str_detect(odmp_incident_details, regex("aircraft|plane|helicopter",
                                              ignore_case=TRUE)) ~ "Aircraft",
      TRUE ~ "Other/unknown"
    ),
    # Fuzzy score quality bin
    match_quality = cut(odmp_fuzzy_score,
                        breaks=c(0, 70, 80, 90, 100),
                        labels=c("Low (<70)","Medium (70–79)",
                                 "High (80–89)","Exact (90+)"),
                        include.lowest=TRUE)
  )

# ODMP match rate per state (against total highways)
odmp_state_rate <- hw_raw %>%
  group_by(state) %>%
  summarise(total_hw  = n(),
            odmp_n    = sum(!is.na(odmp_url)),
            .groups="drop") %>%
  filter(odmp_n > 0) %>%
  mutate(match_rate = odmp_n / total_hw,
         avg_age    = map_dbl(state, ~mean(odmp$age_num[odmp$state==.x], na.rm=TRUE)),
         avg_tour   = map_dbl(state, ~mean(odmp$tour_years[odmp$state==.x], na.rm=TRUE)),
         pct_gunfire= map_dbl(state, ~mean(odmp$cause_grp[odmp$state==.x]=="Gunfire",
                                           na.rm=TRUE)*100))

# Incident details word frequency
incident_words <- odmp %>%
  filter(!is.na(odmp_incident_details)) %>%
  mutate(words = str_extract_all(
    str_to_lower(odmp_incident_details), "[a-z]+")) %>%
  unnest(words) %>%
  filter(!words %in% c("cause","incident","date","weapon","offender","and","the",
                        "of","in","a","an","by","was","on","at","to","is","he","she",
                        "his","her","while","had","been","shot","killed","not")) %>%
  count(words, sort=TRUE)

cause_colours <- c(
  "Gunfire"          = CLR_GOP,
  "Vehicle crash"    = CLR_AMBER,
  "Struck by vehicle"= "#534AB7",
  "Aircraft"         = "#1a5fa0",
  "Assault (other)"  = "#8a3060",
  "Other / illness"  = CLR_NEUTRAL
)

# =============================================================================
# UI
# =============================================================================
sidebar_width <- 240

ui <- dashboardPage(
  skin = "green",

  dashboardHeader(
    title = span("Memorial Highways", style = "font-size:15px; font-weight:600;"),
    titleWidth = sidebar_width
  ),

  dashboardSidebar(
    width = sidebar_width,
    sidebarMenu(
      id = "tabs",
      menuItem("Summary",             tabName = "overview",    icon = icon("map")),
      menuItem("Geography",           tabName = "geography",   icon = icon("globe")),
      menuItem("Honorees",            tabName = "honorees",    icon = icon("user")),
      menuItem("Historical Eras",     tabName = "eras",        icon = icon("clock")),
      menuItem("Honoree Geography",   tabName = "geo2",        icon = icon("location-dot")),
      menuItem("Education",           tabName = "education",   icon = icon("graduation-cap")),
      menuItem("Lifespan",            tabName = "lifespan",    icon = icon("heart-pulse")),
      menuItem("State Profiles",      tabName = "profiles",    icon = icon("table")),
      menuItem("Highway Names",       tabName = "names",       icon = icon("road")),
      menuItem("Highway Categories",  tabName = "categories",  icon = icon("tags")),
      menuItem("Economic Demo.",      tabName = "economics",   icon = icon("chart-line")),
      menuItem("Race & Ethnicity",    tabName = "race",        icon = icon("users")),
      menuItem("Partisan Lean",       tabName = "elections",   icon = icon("flag")),
      menuItem("ODMP — Officer Data",  tabName = "odmp",        icon = icon("shield-halved"))
    ),
    tags$div(
      style = "padding:16px 20px; border-top:1px solid rgba(255,255,255,0.1); margin-top:auto; position:absolute; bottom:0; width:100%;",
      tags$p("5,335 highways · 34 states · 275 ODMP records",
             style = "color:rgba(255,255,255,0.5); font-size:11px; margin:0;")
    )
  ),

  dashboardBody(
    tags$head(tags$style(HTML(paste0("
      body, .content-wrapper, .right-side { background-color:", CLR_BG, "; }
      .box { background:", CLR_SURFACE, "; border-top:3px solid ", CLR_TEAL, "; border-radius:8px; }
      .box-header .box-title { color:", CLR_TEXT, "; font-weight:600; font-size:13px; }
      .small-box { border-radius:8px; }
      .small-box h3 { font-weight:300; }
      .nav-tabs-custom { background:", CLR_SURFACE, "; }
      .nav-tabs-custom>.tab-content { background:", CLR_SURFACE, "; }
      .select-input, .shiny-input-container select { background:", CLR_SURFACE, "; }
    ")))),

    tabItems(

      # ── OVERVIEW ─────────────────────────────────────────────────────────
      tabItem("overview",
        fluidRow(
          valueBoxOutput("vb_total",    width=3),
          valueBoxOutput("vb_states",   width=3),
          valueBoxOutput("vb_verified", width=3),
          valueBoxOutput("vb_mlk",      width=3)
        ),
        fluidRow(
          valueBoxOutput("vb_military",  width=3),
          valueBoxOutput("vb_political", width=3),
          valueBoxOutput("vb_gop_avg",   width=3),
          valueBoxOutput("vb_dem_avg",   width=3)
        ),
        fluidRow(
          box(title="Top 15 states by highway count", width=6, height=480,
              plotOutput("p_top15", height=420)),
          box(title="Highway count distribution", width=6, height=480,
              plotOutput("p_dist", height=420))
        )
      ),

      # ── GEOGRAPHY ────────────────────────────────────────────────────────
      tabItem("geography",
        fluidRow(
          box(title="US choropleth map", width=12,
              fluidRow(
                column(3, selectInput("map_var", "Overlay variable:",
                  choices = c("Highway count"="highways",
                              "Median income"="Median_Household_Income",
                              "Poverty rate"="Pct_Below_Poverty_Level",
                              "Median age"="Median_Age",
                              "GOP margin 2024"="m24"),
                  selected = "highways"))
              ),
              plotOutput("p_map", height=460))
        )
      ),

      # ── HONOREES ─────────────────────────────────────────────────────────
      tabItem("honorees",
        fluidRow(
          box(title="Gender breakdown (n=247 verified)", width=4, height=340,
              plotOutput("p_gender", height=280)),
          box(title="Background categories", width=4, height=340,
              plotOutput("p_bg", height=280)),
          box(title="Military & political honorees by state", width=4, height=340,
              plotOutput("p_milpol", height=280))
        ),
        fluidRow(
          box(title="% Female honorees by state (≥3 verified)", width=12, height=380,
              plotOutput("p_gender_state", height=320))
        )
      ),

      # ── ERAS ─────────────────────────────────────────────────────────────
      tabItem("eras",
        fluidRow(
          box(title="Honoree birth decades", width=6, height=400,
              plotOutput("p_birth", height=340)),
          box(title="Honoree death decades", width=6, height=400,
              plotOutput("p_death", height=340))
        )
      ),

      # ── HONOREE GEOGRAPHY ────────────────────────────────────────────────
      tabItem("geo2",
        fluidRow(
          valueBox(210,  "with known birth state", icon=icon("map-pin"),   color="olive", width=3),
          valueBox("47%","born in highway state",  icon=icon("home"),      color="green", width=3),
          valueBox(14,   "foreign-born honorees",  icon=icon("plane"),     color="yellow",width=3),
          valueBox(196,  "with known death state",  icon=icon("star"),     color="olive", width=3)
        ),
        fluidRow(
          box(title="Top 15 birth states", width=6, height=420,
              plotOutput("p_birth_state", height=360)),
          box(title="Top 15 death states", width=6, height=420,
              plotOutput("p_death_state", height=360))
        ),
        fluidRow(
          box(title="Born in honoring state?", width=5, height=380,
              plotOutput("p_birth_match", height=320)),
          box(title="Top cross-state connections", width=7, height=380,
              plotOutput("p_cross_state", height=320))
        )
      ),

      # ── EDUCATION ────────────────────────────────────────────────────────
      tabItem("education",
        fluidRow(
          box(title="Top 20 institutions attended by honorees", width=8, height=540,
              plotOutput("p_inst", height=480)),
          box(title="Institution type breakdown", width=4, height=540,
              plotOutput("p_inst_type", height=240),
              hr(),
              tags$p("Top 3 institutions — Morehouse College, Crozer Theological Seminary,
                      and Boston University — all reflect Dr. Martin Luther King Jr.'s
                      educational path.",
                     style=paste0("font-size:12px; color:", CLR_MUTED, "; padding:8px;")))
        )
      ),

      # ── LIFESPAN ─────────────────────────────────────────────────────────
      tabItem("lifespan",
        fluidRow(
          valueBox("70.3 yrs","Mean lifespan (n=214)", icon=icon("chart-bar"),color="green",width=3),
          valueBox("81.1 yrs","Female avg lifespan",   icon=icon("female"),   color="olive",width=3),
          valueBox("68.7 yrs","Male avg lifespan",     icon=icon("male"),     color="yellow",width=3),
          valueBox("23 yrs",  "Youngest honoree",      icon=icon("star"),     color="olive",width=3)
        ),
        fluidRow(
          box(title="Lifespan distribution", width=6, height=380,
              plotOutput("p_lifespan_hist", height=320)),
          box(title="Mean lifespan by gender", width=6, height=380,
              plotOutput("p_lifespan_gender", height=320))
        ),
        fluidRow(
          box(title="Mean lifespan by birth era", width=12, height=380,
              plotOutput("p_lifespan_era", height=320))
        )
      ),

      # ── STATE PROFILES ────────────────────────────────────────────────────
      tabItem("profiles",
        fluidRow(
          box(title="Wikipedia match rate by state", width=12, height=380,
              plotOutput("p_wiki_rate", height=320))
        ),
        fluidRow(
          box(title="Full state profile table", width=12,
              DTOutput("tbl_profiles"))
        )
      ),

      # ── HIGHWAY NAMES ────────────────────────────────────────────────────
      tabItem("names",
        fluidRow(
          valueBox(425,    "'Veterans' appearances",   icon=icon("flag"),  color="green", width=3),
          valueBox(72,     "Blue Star Mem. instances", icon=icon("star"),  color="olive", width=3),
          valueBox("John", "Most common first name",  icon=icon("user"),  color="yellow",width=3),
          valueBox(100,    "Highways use 'Sergeant'", icon=icon("medal"), color="olive", width=3)
        ),
        fluidRow(
          box(title="Top 25 words in highway names",
              subtitle = "Stopwords removed · Color = theme",
              width=7, height=620,
              plotOutput("p_words", height=560)),
          box(title="Honorary titles & ranks", width=5, height=300,
              plotOutput("p_prefix", height=240)),
          box(title="Most repeated highway names", width=5, height=300,
              plotOutput("p_top_names", height=240))
        )
      ),

      # ── CATEGORIES ───────────────────────────────────────────────────────
      tabItem("categories",
        fluidRow(
          box(title="Highway category breakdown", width=5, height=420,
              plotOutput("p_cat_donut", height=360)),
          box(title="MLK Jr. highways by state", width=7, height=420,
              plotOutput("p_mlk", height=360))
        )
      ),

      # ── ECONOMICS ────────────────────────────────────────────────────────
      tabItem("economics",
        fluidRow(
          box(title="Select demographic variable", width=12,
              selectInput("econ_var", NULL,
                choices = c(
                  "Median household income"   = "Median_Household_Income",
                  "% below poverty line"      = "Pct_Below_Poverty_Level",
                  "Unemployment rate (%)"     = "Unemployment_Rate",
                  "Median age (years)"        = "Median_Age",
                  "HS grad or higher"         = "HS_Grad_or_Higher",
                  "Bachelor's or higher"      = "Bachelors_or_Higher"
                ),
                selected = "Median_Household_Income",
                width = "300px"
              ))
        ),
        fluidRow(
          box(title="Demographic vs highway count", width=8, height=500,
              plotOutput("p_econ_scatter", height=440)),
          box(title="Income quartile totals", width=4, height=500,
              plotOutput("p_income_q", height=440))
        )
      ),

      # ── RACE & ETHNICITY ─────────────────────────────────────────────────
      tabItem("race",
        fluidRow(
          valueBox("+0.477","AIAN correlation",      icon=icon("chart-line"),color="green",width=3),
          valueBox("≈ 0.00","White correlation",     icon=icon("minus"),     color="olive",width=3),
          valueBox("−0.17", "Hispanic correlation",  icon=icon("arrow-down"),color="yellow",width=3),
          valueBox("+0.32", "Two+ races correlation",icon=icon("arrow-up"),  color="olive",width=3)
        ),
        fluidRow(
          box(title="Select racial group", width=12,
              selectInput("race_var", NULL,
                choices = c(
                  "% AIAN alone"          = "Pct_AIAN_Alone",
                  "% White alone"         = "Pct_White_Alone",
                  "% Black alone"         = "Pct_Black_Alone",
                  "% Hispanic"            = "Pct_Hispanic",
                  "% Asian alone"         = "Pct_Asian_Alone",
                  "% Two or more races"   = "Pct_TwoOrMore"
                ),
                selected = "Pct_AIAN_Alone",
                width = "300px"
              ))
        ),
        fluidRow(
          box(title="Race/ethnicity vs highway count", width=8, height=480,
              plotOutput("p_race_scatter", height=420)),
          box(title="Racial composition stacked (sorted by highways)", width=12, height=460,
              plotOutput("p_race_stack", height=400))
        )
      ),

      # ── ELECTIONS ────────────────────────────────────────────────────────
      tabItem("elections",
        fluidRow(
          valueBox(204,   "Avg highways — GOP states (2024)", icon=icon("flag"),color="red",   width=3),
          valueBox(81,    "Avg highways — DEM states (2024)", icon=icon("flag"),color="green", width=3),
          valueBox("−0.003","Margin × count correlation",    icon=icon("equals"),color="olive",width=3),
          valueBox("+4.5pp","Avg GOP swing 2020→2024",       icon=icon("arrow-up"),color="yellow",width=3)
        ),
        fluidRow(
          box(title="Election year", width=12,
              radioButtons("elec_year", NULL,
                           choices=c("2016"="m16","2020"="m20","2024"="m24"),
                           selected="m24", inline=TRUE))
        ),
        fluidRow(
          box(title="GOP margin vs highway count", width=8, height=520,
              plotOutput("p_partisan", height=460)),
          box(title="Highways by partisan grouping (2024)", width=4, height=520,
              plotOutput("p_party_bar", height=460))
        )
      ),  # end elections tabItem

      # ── ODMP — OFFICER DATA ───────────────────────────────────────────────
      tabItem("odmp",
        fluidRow(
          valueBoxOutput("vb_odmp_total",  width=3),
          valueBoxOutput("vb_odmp_states", width=3),
          valueBoxOutput("vb_odmp_age",    width=3),
          valueBoxOutput("vb_odmp_tour",   width=3)
        ),

        # ── Row 1: Cause of death ────────────────────────────────────────
        fluidRow(
          box(title="Cause of death — all ODMP-matched highways", width=7, height=400,
              plotOutput("p_odmp_cause", height=340)),
          box(title="Cause of death by state", width=5, height=400,
              plotOutput("p_odmp_cause_state", height=340))
        ),

        # ── Row 2: EOW timeline & monthly pattern ────────────────────────
        fluidRow(
          box(title="End of watch by decade", width=6, height=380,
              plotOutput("p_odmp_eow_decade", height=320)),
          box(title="End of watch — month of year", width=6, height=380,
              plotOutput("p_odmp_eow_month", height=320))
        ),

        # ── Row 3: Age & tour of duty ────────────────────────────────────
        fluidRow(
          box(title="Officer age at time of death", width=4, height=380,
              plotOutput("p_odmp_age_hist", height=320)),
          box(title="Years of service (tour of duty)", width=4, height=380,
              plotOutput("p_odmp_tour_hist", height=320)),
          box(title="Age vs years of service", width=4, height=380,
              plotOutput("p_odmp_age_tour", height=320))
        ),

        # ── Row 4: Mean age & service by cause ───────────────────────────
        fluidRow(
          box(title="Mean officer age by cause of death", width=6, height=360,
              plotOutput("p_odmp_age_by_cause", height=300)),
          box(title="Mean years of service by cause", width=6, height=360,
              plotOutput("p_odmp_tour_by_cause", height=300))
        ),

        # ── Row 5: Age over time & match quality ─────────────────────────
        fluidRow(
          box(title="Mean officer age at death by decade", width=6, height=360,
              plotOutput("p_odmp_age_over_time", height=300)),
          box(title="Match quality — fuzzy score distribution", width=6, height=360,
              plotOutput("p_odmp_fuzzy", height=300))
        ),

        # ── Row 6: State-level ODMP comparison ───────────────────────────
        fluidRow(
          box(title="State ODMP profile — avg age, service & gunfire rate", width=12, height=420,
              plotOutput("p_odmp_state_profile", height=360))
        ),

        # ── Row 7: Incident details & weapon analysis ─────────────────────
        fluidRow(
          box(title="Top words in incident details", width=5, height=400,
              plotOutput("p_odmp_incident_words", height=340)),
          box(title="Weapon type from incident details", width=7, height=400,
              plotOutput("p_odmp_weapon", height=340))
        ),

        # ── Row 8: Cause-filtered reactive plots ─────────────────────────
        fluidRow(
          box(title="Filter by cause of death", width=12,
              selectInput("odmp_cause_filter", NULL,
                          choices = c("All causes" = "all",
                                      "Gunfire"          = "Gunfire",
                                      "Vehicle crash"    = "Vehicle crash",
                                      "Struck by vehicle"= "Struck by vehicle",
                                      "Aircraft"         = "Aircraft",
                                      "Assault (other)"  = "Assault (other)",
                                      "Other / illness"  = "Other / illness"),
                          selected = "all", width = "280px"))
        ),
        fluidRow(
          box(title="EOW year — filtered by cause", width=6, height=400,
              plotOutput("p_odmp_cause_year", height=340)),
          box(title="Age distribution — filtered by cause", width=6, height=400,
              plotOutput("p_odmp_cause_age", height=340))
        ),

        # ── Row 9: Full ODMP table ────────────────────────────────────────
        fluidRow(
          box(title="Full ODMP record table", width=12,
              DTOutput("tbl_odmp"))
        )
      )

    ) # end tabItems
  ) # end dashboardBody
) # end dashboardPage

# =============================================================================
# SERVER
# =============================================================================
server <- function(input, output, session) {

  # ── VALUE BOXES ──────────────────────────────────────────────────────────
  output$vb_total    <- renderValueBox(valueBox(comma(nrow(hw_raw)), "Total highways",      icon=icon("road"),   color="green"))
  output$vb_states   <- renderValueBox(valueBox(n_distinct(hw_raw$state), "States covered", icon=icon("map"),    color="olive"))
  output$vb_verified <- renderValueBox(valueBox(247, "Verified gender",                     icon=icon("user"),   color="yellow"))
  output$vb_mlk      <- renderValueBox(valueBox(35,  "MLK Jr. highways",                    icon=icon("star"),   color="olive"))
  output$vb_military <- renderValueBox(valueBox(95,  "Military honorees",                   icon=icon("shield"), color="green"))
  output$vb_political<- renderValueBox(valueBox(167, "Political honorees",                  icon=icon("building-columns"), color="olive"))
  output$vb_gop_avg  <- renderValueBox(valueBox(204, "Avg highways — GOP states",           icon=icon("flag"),   color="red"))
  output$vb_dem_avg  <- renderValueBox(valueBox(81,  "Avg highways — DEM states",           icon=icon("flag"),   color="green"))

  # ── OVERVIEW PLOTS ────────────────────────────────────────────────────────
  output$p_top15 <- renderPlot({
    master %>%
      slice_max(highways, n=15) %>%
      mutate(state = fct_reorder(state, highways)) %>%
      ggplot(aes(highways, state, fill=winner24)) +
      geom_col(width=0.7) +
      geom_text(aes(label=comma(highways)), hjust=-0.15, size=3.2, colour=CLR_MUTED) +
      scale_fill_manual(values=c(GOP=CLR_GOP, DEM=CLR_DEM), name="2024") +
      scale_x_continuous(expand=expansion(mult=c(0,0.12)), labels=comma) +
      labs(title="Top 15 states by highway count",
           subtitle="Color = 2024 presidential winner",
           x="Highways", y=NULL) +
      theme_hw()
  }, bg=CLR_BG)

  output$p_dist <- renderPlot({
    master %>%
      mutate(bucket = cut(highways,
                          breaks=c(0,10,50,150,500,Inf),
                          labels=c("1–10","11–50","51–150","151–500","500+"),
                          right=TRUE)) %>%
      count(bucket) %>%
      ggplot(aes(bucket, n, fill=bucket)) +
      geom_col(width=0.65, show.legend=FALSE) +
      geom_text(aes(label=n), vjust=-0.5, size=3.5, colour=CLR_MUTED) +
      scale_fill_manual(values=c("#e8e4dc","#d0ccc0","#b0aa9c","#887c68","#4a4038")) +
      scale_y_continuous(expand=expansion(mult=c(0,0.12))) +
      labs(title="Highway count distribution", x="Range", y="Number of states") +
      theme_hw()
  }, bg=CLR_BG)

  # ── MAP ───────────────────────────────────────────────────────────────────
  output$p_map <- renderPlot({
    var   <- input$map_var
    label <- names(which(c("Highway count"="highways","Median income"="Median_Household_Income",
                            "Poverty rate"="Pct_Below_Poverty_Level","Median age"="Median_Age",
                            "GOP margin 2024"="m24") == var))
    map_df <- master %>%
      transmute(state, values = .data[[var]])

    palettes <- list(
      highways                = c("#ddd8cc","#b0a878","#7a6830",CLR_ACCENT="#4a5820"),
      Median_Household_Income = c("#e6f5f0","#9fe1cc","#3ab890","#1a6848"),
      Pct_Below_Poverty_Level = c("#faeeda","#f7c875","#e8951a","#703008"),
      Median_Age              = c("#eeedfe","#afa9ec","#7f77dd","#2e2870"),
      m24                     = c(CLR_DEM,"#8ab8a0","#d0c8c0","#d07060",CLR_GOP)
    )
    pal <- palettes[[var]]

    plot_usmap(data=map_df, values="values", colour=CLR_BG, linewidth=0.3) +
      scale_fill_gradientn(colours=pal, na.value="#e8e4dc",
                           name=label, labels=comma) +
      labs(title=paste("Memorial highways —", label),
           subtitle="Florida uses county-level demographic averages") +
      theme_hw() +
      theme(axis.text=element_blank(), axis.title=element_blank(),
            panel.grid=element_blank())
  }, bg=CLR_BG)

  # ── HONOREES ─────────────────────────────────────────────────────────────
  output$p_gender <- renderPlot({
    hw_raw %>%
      filter(gender %in% c("male","female")) %>%
      count(gender) %>%
      mutate(pct=n/sum(n), label=paste0(str_to_title(gender),"\n",percent(pct,accuracy=1))) %>%
      ggplot(aes(x=2, y=n, fill=gender)) +
      geom_col(width=1, colour=CLR_BG, linewidth=0.8) +
      coord_polar(theta="y") +
      xlim(0.5, 2.5) +
      scale_fill_manual(values=c(female=CLR_DEM, male=CLR_GOP)) +
      geom_text(aes(label=label), position=position_stack(vjust=0.5),
                size=3.5, colour="white", fontface="bold") +
      labs(title="Gender of verified honorees") +
      theme_hw() +
      theme(axis.text=element_blank(), axis.title=element_blank(),
            panel.grid=element_blank(), legend.position="none")
  }, bg=CLR_BG)

  output$p_bg <- renderPlot({
    tibble(category=c("Neither","Political","Military","Sports","Music"),
           n=c(1341,167,95,20,8)) %>%
      mutate(category=fct_reorder(category,n,.desc=TRUE)) %>%
      ggplot(aes(x=2, y=n, fill=category)) +
      geom_col(width=1, colour=CLR_BG, linewidth=0.5) +
      coord_polar(theta="y") +
      xlim(0.5, 2.5) +
      scale_fill_manual(values=c(Neither=CLR_NEUTRAL, Political=CLR_PURPLE,
                                  Military=CLR_AMBER, Sports=CLR_TEAL, Music=CLR_GOP)) +
      labs(title="Honoree backgrounds", fill=NULL) +
      theme_hw() +
      theme(axis.text=element_blank(), axis.title=element_blank(),
            panel.grid=element_blank())
  }, bg=CLR_BG)

  output$p_gender_state <- renderPlot({
    hw_raw %>%
      filter(gender %in% c("male","female")) %>%
      group_by(state) %>%
      summarise(male=sum(gender=="male"), female=sum(gender=="female"),
                known=n(), .groups="drop") %>%
      filter(known >= 3) %>%
      mutate(pct_female=female/known, state=fct_reorder(state,pct_female)) %>%
      ggplot(aes(pct_female, state, fill=pct_female)) +
      geom_col(width=0.7, show.legend=FALSE) +
      scale_fill_gradient(low=CLR_NEUTRAL, high=CLR_DEM) +
      scale_x_continuous(labels=percent_format(accuracy=1),
                         expand=expansion(mult=c(0,0.08))) +
      labs(title="% Female honorees by state",
           subtitle="States with ≥ 3 verified honorees",
           x="% female", y=NULL) +
      theme_hw()
  }, bg=CLR_BG)

  output$p_milpol <- renderPlot({
    hw_raw %>%
      group_by(state) %>%
      summarise(Military  = sum(involved_in_military=="yes", na.rm=TRUE),
                Political = sum(involved_in_politics=="yes",  na.rm=TRUE), .groups="drop") %>%
      filter(Military+Political > 0) %>%
      pivot_longer(c(Military,Political), names_to="type", values_to="count") %>%
      mutate(state=fct_reorder(state,count,sum)) %>%
      ggplot(aes(count, state, fill=type)) +
      geom_col(position="dodge", width=0.7) +
      scale_fill_manual(values=c(Military=CLR_AMBER, Political=CLR_PURPLE), name=NULL) +
      scale_x_continuous(expand=expansion(mult=c(0,0.08))) +
      labs(title="Military & political honorees by state", x="Count", y=NULL) +
      theme_hw()
  }, bg=CLR_BG)

  # ── ERAS ─────────────────────────────────────────────────────────────────
  output$p_birth <- renderPlot({
    eras %>%
      filter(birth_dec>=1700, birth_dec<=1990, !is.na(birth_dec)) %>%
      count(birth_dec) %>%
      ggplot(aes(factor(birth_dec), n, fill=birth_dec>=1900)) +
      geom_col(width=0.8, show.legend=FALSE) +
      scale_fill_manual(values=c(`FALSE`=CLR_NEUTRAL, `TRUE`=CLR_AMBER)) +
      scale_y_continuous(expand=expansion(mult=c(0,0.08))) +
      labs(title="Honoree birth decades",
           subtitle="Amber = 20th century  ·  Peak: 1920s",
           x="Decade", y="Count") +
      theme_hw() +
      theme(axis.text.x=element_text(angle=45, hjust=1, size=8))
  }, bg=CLR_BG)

  output$p_death <- renderPlot({
    eras %>%
      filter(death_dec>=1800, death_dec<=2020, !is.na(death_dec)) %>%
      count(death_dec) %>%
      ggplot(aes(factor(death_dec), n, fill=death_dec>=1960)) +
      geom_col(width=0.8, show.legend=FALSE) +
      scale_fill_manual(values=c(`FALSE`=CLR_NEUTRAL, `TRUE`=CLR_TEAL)) +
      scale_y_continuous(expand=expansion(mult=c(0,0.08))) +
      labs(title="Honoree death decades",
           subtitle="Teal = post-1960  ·  Deaths accelerate into the 2000s",
           x="Decade", y="Count") +
      theme_hw() +
      theme(axis.text.x=element_text(angle=45, hjust=1, size=8))
  }, bg=CLR_BG)

  # ── HONOREE GEOGRAPHY ────────────────────────────────────────────────────
  output$p_birth_state <- renderPlot({
    geo %>%
      filter(!is.na(birth_state)) %>%
      count(birth_state) %>%
      slice_max(n, n=15) %>%
      mutate(birth_state=fct_reorder(birth_state,n)) %>%
      ggplot(aes(n, birth_state, fill=birth_state=="Georgia")) +
      geom_col(width=0.7, show.legend=FALSE) +
      scale_fill_manual(values=c(`FALSE`=CLR_NEUTRAL, `TRUE`=CLR_AMBER)) +
      scale_x_continuous(expand=expansion(mult=c(0,0.08))) +
      labs(title="Top birth states of honorees", x="Count", y=NULL) +
      theme_hw()
  }, bg=CLR_BG)

  output$p_death_state <- renderPlot({
    geo %>%
      filter(!is.na(death_state)) %>%
      count(death_state) %>%
      slice_max(n, n=15) %>%
      mutate(death_state=fct_reorder(death_state,n)) %>%
      ggplot(aes(n, death_state, fill=death_state=="Florida")) +
      geom_col(width=0.7, show.legend=FALSE) +
      scale_fill_manual(values=c(`FALSE`=CLR_NEUTRAL, `TRUE`=CLR_TEAL)) +
      scale_x_continuous(expand=expansion(mult=c(0,0.08))) +
      labs(title="Top death states of honorees", x="Count", y=NULL) +
      theme_hw()
  }, bg=CLR_BG)

  output$p_birth_match <- renderPlot({
    geo_m <- geo %>% filter(!is.na(birth_state), !is.na(state)) %>%
      mutate(same=birth_state==state)
    tibble(cat=c("Born in\nhighway state","Born\nelsewhere"),
           n=c(sum(geo_m$same), sum(!geo_m$same))) %>%
      mutate(pct=n/sum(n)) %>%
      ggplot(aes(x=2, y=n, fill=cat)) +
      geom_col(width=1, colour=CLR_BG, linewidth=0.8) +
      coord_polar(theta="y") +
      xlim(0.5,2.5) +
      scale_fill_manual(values=c("Born in\nhighway state"=CLR_TEAL,
                                  "Born\nelsewhere"=CLR_NEUTRAL)) +
      geom_text(aes(label=paste0(cat,"\n",percent(pct,accuracy=1))),
                position=position_stack(vjust=0.5),
                size=3.5, colour="white", fontface="bold") +
      labs(title="Born in honoring state?") +
      theme_hw() +
      theme(axis.text=element_blank(), axis.title=element_blank(),
            panel.grid=element_blank(), legend.position="none")
  }, bg=CLR_BG)

  output$p_cross_state <- renderPlot({
    geo %>%
      filter(!is.na(birth_state), !is.na(state), birth_state!=state) %>%
      count(birth_state, state, sort=TRUE) %>%
      slice_max(n, n=10) %>%
      mutate(pair=paste(birth_state, "→", state),
             pair=fct_reorder(pair,n)) %>%
      ggplot(aes(n, pair, fill=n)) +
      geom_col(width=0.7, show.legend=FALSE) +
      scale_fill_gradient(low=CLR_NEUTRAL, high=CLR_PURPLE) +
      scale_x_continuous(expand=expansion(mult=c(0,0.1)), breaks=1:10) +
      labs(title="Top cross-state connections",
           subtitle="Born in X → honored in Y",
           x="Count", y=NULL) +
      theme_hw()
  }, bg=CLR_BG)

  # ── EDUCATION ────────────────────────────────────────────────────────────
  inst_data <- tibble(
    institution=c("Morehouse College","Crozer Theological Seminary","Boston University",
                  "University of Georgia","Yale University","US Military Academy",
                  "University of Florida","University of Kansas","Univ. Southern California",
                  "Harvard University","University of Utah","Univ. Wisconsin–Madison",
                  "University of Minnesota","University of Chicago",
                  "Alabama State Teachers College","Highlander Folk School",
                  "Kansas State University","Miami University","Florida A&M University",
                  "École Militaire (Paris)"),
    count=c(16,15,15,11,8,8,8,5,4,3,3,3,3,3,3,3,3,3,3,3),
    type=c("college","theological","college","college","college","military",
           "college","college","college","college","college","college",
           "college","college","college","college","college","college",
           "college","military")
  )

  output$p_inst <- renderPlot({
    inst_data %>%
      mutate(institution=fct_reorder(institution,count),
             colour=case_when(type=="theological"~CLR_AMBER,
                              type=="military"~CLR_GOP,
                              TRUE~CLR_DEM)) %>%
      ggplot(aes(count, institution, fill=type)) +
      geom_col(width=0.75) +
      scale_fill_manual(values=c(college=CLR_DEM, theological=CLR_AMBER, military=CLR_GOP),
                        name="Type") +
      scale_x_continuous(expand=expansion(mult=c(0,0.08))) +
      labs(title="Top institutions attended by honorees",
           subtitle="MLK Jr.'s three schools (Morehouse · Crozer · BU) dominate the top 3",
           x="Honoree count", y=NULL) +
      theme_hw()
  }, bg=CLR_BG)

  output$p_inst_type <- renderPlot({
    tibble(type=c("University/College","Theological","Military","Other"),
           n=c(180,21,12,5)) %>%
      mutate(type=fct_reorder(type,n,.desc=TRUE)) %>%
      ggplot(aes(x=2, y=n, fill=type)) +
      geom_col(width=1, colour=CLR_BG, linewidth=0.5) +
      coord_polar(theta="y") +
      xlim(0.5,2.5) +
      scale_fill_manual(values=c("University/College"=CLR_DEM, Theological=CLR_AMBER,
                                  Military=CLR_GOP, Other=CLR_NEUTRAL)) +
      labs(title="Institution types", fill=NULL) +
      theme_hw() +
      theme(axis.text=element_blank(), axis.title=element_blank(),
            panel.grid=element_blank())
  }, bg=CLR_BG)

  # ── LIFESPAN ─────────────────────────────────────────────────────────────
  lifespan_valid <- eras %>% filter(!is.na(lifespan), lifespan>0, lifespan<120)

  output$p_lifespan_hist <- renderPlot({
    lifespan_valid %>%
      mutate(bin=cut(lifespan, breaks=seq(20,100,10),
                     labels=paste0(seq(20,90,10),"–",seq(29,99,10)),
                     right=FALSE)) %>%
      filter(!is.na(bin)) %>%
      count(bin) %>%
      ggplot(aes(bin, n, fill=n)) +
      geom_col(width=0.8, show.legend=FALSE) +
      scale_fill_gradient(low=CLR_NEUTRAL, high=CLR_AMBER) +
      scale_y_continuous(expand=expansion(mult=c(0,0.1))) +
      labs(title="Lifespan distribution (n=214)",
           subtitle="Peak: 70–79 years",
           x="Age at death", y="Count") +
      theme_hw()
  }, bg=CLR_BG)

  output$p_lifespan_gender <- renderPlot({
    lifespan_valid %>%
      filter(gender %in% c("male","female")) %>%
      group_by(gender) %>%
      summarise(mean=mean(lifespan), n=n(), .groups="drop") %>%
      ggplot(aes(gender, mean, fill=gender)) +
      geom_col(width=0.5, show.legend=FALSE) +
      geom_text(aes(label=paste0(round(mean,1)," yrs\n(n=",n,")")),
                vjust=-0.3, size=3.5, colour=CLR_MUTED) +
      scale_fill_manual(values=c(male=CLR_GOP, female=CLR_DEM)) +
      scale_y_continuous(expand=expansion(mult=c(0,0.14)), limits=c(0,92)) +
      labs(title="Mean lifespan by gender",
           subtitle="Female honorees lived ~12 years longer",
           x=NULL, y="Mean lifespan (years)") +
      theme_hw()
  }, bg=CLR_BG)

  output$p_lifespan_era <- renderPlot({
    lifespan_valid %>%
      mutate(era=cut(dob_year, breaks=c(1600,1800,1850,1880,1900,1920,1940,1960,2000),
                     labels=c("Pre-1800","1800–49","1850–79","1880–99",
                              "1900–19","1920–39","1940–59","1960+"))) %>%
      filter(!is.na(era)) %>%
      group_by(era) %>%
      summarise(mean=mean(lifespan), n=n(), .groups="drop") %>%
      ggplot(aes(era, mean, fill=mean)) +
      geom_col(width=0.75, show.legend=FALSE) +
      geom_text(aes(label=paste0(round(mean,1),"\n(n=",n,")")),
                vjust=-0.3, size=2.8, colour=CLR_MUTED) +
      scale_fill_gradient(low=CLR_NEUTRAL, high=CLR_TEAL) +
      scale_y_continuous(expand=expansion(mult=c(0,0.14))) +
      labs(title="Mean lifespan by birth era",
           subtitle="1900–1919 cohort lived longest (avg 80.2 years)",
           x="Birth era", y="Mean lifespan (years)") +
      theme_hw() +
      theme(axis.text.x=element_text(angle=30, hjust=1))
  }, bg=CLR_BG)

  # ── STATE PROFILES ────────────────────────────────────────────────────────
  output$p_wiki_rate <- renderPlot({
    state_profiles %>%
      mutate(state=fct_reorder(state, highways)) %>%
      ggplot(aes(wiki_pct, state, fill=wiki_pct)) +
      geom_col(width=0.75, show.legend=FALSE) +
      scale_fill_gradient(low=CLR_NEUTRAL, high=CLR_TEAL) +
      scale_x_continuous(labels=percent_format(accuracy=1),
                         expand=expansion(mult=c(0,0.08))) +
      labs(title="Wikipedia match rate by state",
           subtitle="% of highways with a matched honoree  ·  Missouri = 0%",
           x="Match rate", y=NULL) +
      theme_hw()
  }, bg=CLR_BG)

  output$tbl_profiles <- renderDT({
    state_profiles %>%
      arrange(desc(highways)) %>%
      mutate(wiki_pct = percent(wiki_pct, accuracy=1)) %>%
      select(State=state, Highways=highways, `Wiki matched`=has_wiki,
             `Match %`=wiki_pct, Male=male, Female=female,
             Military=military, Political=politics, Sports=sports, Music=music) %>%
      datatable(options=list(pageLength=20, dom="ftp"),
                rownames=FALSE,
                style="bootstrap")
  })

  # ── HIGHWAY NAMES ─────────────────────────────────────────────────────────
  output$p_words <- renderPlot({
    word_freq %>%
      slice_max(n, n=25) %>%
      mutate(
        theme=case_when(
          words %in% c("veterans","army","war","sergeant","division","corporal",
                       "sgt","pfc","infantry","colonel","general","captain",
                       "medal","honor","korean","navy") ~ "Military",
          words %in% c("officer","trooper","deputy","police","sheriff")   ~ "Law enforcement",
          words %in% c("john","james","robert","william","david","charles",
                       "george","thomas","martin")                        ~ "Given name",
          TRUE ~ "Other"),
        words=fct_reorder(words,n)) %>%
      ggplot(aes(n, words, fill=theme)) +
      geom_col(width=0.75) +
      scale_fill_manual(values=c(Military=CLR_AMBER, `Law enforcement`=CLR_PURPLE,
                                  `Given name`=CLR_DEM, Other=CLR_NEUTRAL), name="Theme") +
      scale_x_continuous(expand=expansion(mult=c(0,0.08)), labels=comma) +
      labs(title="Top 25 words in highway names",
           subtitle="Stopwords removed",
           x="Frequency", y=NULL) +
      theme_hw()
  }, bg=CLR_BG)

  output$p_prefix <- renderPlot({
    tibble(title=c("Sergeant","Senator","Dr.","General","Captain",
                   "Governor","President","Colonel","Judge","Rev."),
           n    =c(100,65,53,46,40,30,23,19,9,4)) %>%
      mutate(title=fct_reorder(title,n)) %>%
      ggplot(aes(n, title, fill=n)) +
      geom_col(width=0.7, show.legend=FALSE) +
      scale_fill_gradient(low=CLR_NEUTRAL, high=CLR_AMBER) +
      scale_x_continuous(expand=expansion(mult=c(0,0.1))) +
      labs(title="Honorary titles in highway names", x="Count", y=NULL) +
      theme_hw()
  }, bg=CLR_BG)

  output$p_top_names <- renderPlot({
    tibble(name=c("Blue Star Memorial Hwy","Purple Heart Trail",
                  "Veterans Memorial Hwy","American Legion Memorial Hwy",
                  "Turkey Wheat Trail Hwy","Eisenhower Memorial Hwy",
                  "VFW Highway","Korean War Veterans Memorial Hwy",
                  "Pearl Harbor Memorial Hwy","WWII Veterans Memorial Hwy"),
           n=c(72,64,45,28,28,23,20,18,16,15)) %>%
      mutate(name=fct_reorder(name,n)) %>%
      ggplot(aes(n, name)) +
      geom_col(width=0.7, fill=CLR_TEXT, alpha=0.75) +
      scale_x_continuous(expand=expansion(mult=c(0,0.1))) +
      labs(title="Most repeated highway names", x="Instances", y=NULL) +
      theme_hw()
  }, bg=CLR_BG)

  # ── CATEGORIES ───────────────────────────────────────────────────────────
  output$p_cat_donut <- renderPlot({
    tibble(cat=c("Individual/Other","Military/Veterans","Law Enforcement",
                 "Political Figures","MLK Jr.","Religious Figures"),
           n  =c(3679,874,550,167,37,28)) %>%
      mutate(cat=fct_reorder(cat,n,.desc=TRUE)) %>%
      ggplot(aes(x=2, y=n, fill=cat)) +
      geom_col(width=1, colour=CLR_BG, linewidth=0.4) +
      coord_polar(theta="y") +
      xlim(0.5,2.5) +
      scale_fill_manual(
        values=c("Individual/Other"=CLR_NEUTRAL,"Military/Veterans"=CLR_AMBER,
                 "Law Enforcement"=CLR_PURPLE,"Political Figures"=CLR_DEM,
                 "MLK Jr."=CLR_GOP,"Religious Figures"=CLR_TEAL), name=NULL) +
      labs(title="Highway category breakdown",
           subtitle="5,335 highways classified by name") +
      theme_hw() +
      theme(axis.text=element_blank(), axis.title=element_blank(),
            panel.grid=element_blank())
  }, bg=CLR_BG)

  output$p_mlk <- renderPlot({
    hw_raw %>%
      filter(str_detect(highway_name,
                        regex("martin luther king|mlk|king jr", ignore_case=TRUE))) %>%
      count(state, name="count") %>%
      arrange(desc(count)) %>%
      mutate(state=fct_reorder(state,count),
             hi=state %in% c("Georgia","Florida")) %>%
      ggplot(aes(count, state, fill=hi)) +
      geom_col(width=0.7, show.legend=FALSE) +
      scale_fill_manual(values=c(`FALSE`=CLR_NEUTRAL, `TRUE`=CLR_GOP)) +
      scale_x_continuous(expand=expansion(mult=c(0,0.1)), breaks=1:10) +
      labs(title="MLK Jr. highways by state",
           subtitle="35 highways across 13 states  ·  GA and FL lead",
           x="Highway count", y=NULL) +
      theme_hw()
  }, bg=CLR_BG)

  # ── ECONOMICS (reactive) ─────────────────────────────────────────────────
  output$p_econ_scatter <- renderPlot({
    var   <- input$econ_var
    label <- names(which(c(
      "Median household income"="Median_Household_Income",
      "% below poverty line"="Pct_Below_Poverty_Level",
      "Unemployment rate (%)"="Unemployment_Rate",
      "Median age (years)"="Median_Age",
      "HS grad or higher"="HS_Grad_or_Higher",
      "Bachelor's or higher"="Bachelors_or_Higher") == var))

    p <- master %>%
      filter(!is.na(.data[[var]])) %>%
      ggplot(aes(.data[[var]], highways, colour=winner24, label=state)) +
      geom_point(size=3, alpha=0.85) +
      geom_text_repel(size=2.7, max.overlaps=12, seed=42, colour=CLR_MUTED) +
      scale_colour_manual(values=c(GOP=CLR_GOP, DEM=CLR_DEM), name="2024 winner") +
      labs(title=paste(label, "vs highway count"), x=label, y="Highway count") +
      theme_hw()

    if (var == "Median_Household_Income")
      p <- p + scale_x_continuous(labels=dollar_format(scale=1e-3, suffix="k"))
    if (var %in% c("Pct_Below_Poverty_Level","Unemployment_Rate"))
      p <- p + scale_x_continuous(labels=function(x) paste0(x,"%"))
    p
  }, bg=CLR_BG)

  output$p_income_q <- renderPlot({
    master %>%
      filter(!is.na(Median_Household_Income)) %>%
      mutate(q=ntile(Median_Household_Income,4),
             q=factor(q,labels=c("Q1\nLowest","Q2","Q3","Q4\nHighest"))) %>%
      group_by(q) %>%
      summarise(tot=sum(highways), .groups="drop") %>%
      ggplot(aes(q,tot,fill=q)) +
      geom_col(width=0.65, show.legend=FALSE) +
      geom_text(aes(label=comma(tot)), vjust=-0.4, size=3.5, colour=CLR_MUTED) +
      scale_fill_manual(values=c("#c07820","#d09040","#b8b0a4","#6a6560")) +
      scale_y_continuous(expand=expansion(mult=c(0,0.1)), labels=comma) +
      labs(title="Total highways by income quartile",
           x="Income quartile", y="Total highways") +
      theme_hw()
  }, bg=CLR_BG)

  # ── RACE (reactive) ──────────────────────────────────────────────────────
  output$p_race_scatter <- renderPlot({
    var   <- input$race_var
    label <- names(which(c(
      "% AIAN alone"="Pct_AIAN_Alone","% White alone"="Pct_White_Alone",
      "% Black alone"="Pct_Black_Alone","% Hispanic"="Pct_Hispanic",
      "% Asian alone"="Pct_Asian_Alone","% Two or more races"="Pct_TwoOrMore") == var))
    master %>%
      filter(!is.na(.data[[var]])) %>%
      ggplot(aes(.data[[var]], highways, colour=winner24, label=state)) +
      geom_point(size=3, alpha=0.85) +
      geom_text_repel(size=2.7, max.overlaps=12, seed=42, colour=CLR_MUTED) +
      scale_colour_manual(values=c(GOP=CLR_GOP, DEM=CLR_DEM), name="2024 winner") +
      labs(title=paste(label, "vs highway count"), x=label, y="Highway count") +
      theme_hw()
  }, bg=CLR_BG)

  output$p_race_stack <- renderPlot({
    master %>%
      filter(!is.na(Pct_White_Alone)) %>%
      select(state, highways, White=Pct_White_Alone, Black=Pct_Black_Alone,
             Hispanic=Pct_Hispanic, Asian=Pct_Asian_Alone,
             AIAN=Pct_AIAN_Alone, `Two+`=Pct_TwoOrMore) %>%
      pivot_longer(-c(state,highways), names_to="group", values_to="value") %>%
      mutate(state=fct_reorder(state,highways,.desc=TRUE),
             group=factor(group,levels=c("White","Black","Hispanic","Asian","AIAN","Two+"))) %>%
      ggplot(aes(state, value, fill=group)) +
      geom_col(width=0.85) +
      scale_fill_manual(values=c(White="#5a4010",Black=CLR_DEM,Hispanic=CLR_AMBER,
                                  Asian=CLR_TEAL,AIAN=CLR_PURPLE,`Two+`=CLR_NEUTRAL),
                        name=NULL) +
      labs(title="Racial composition by state (sorted by highway count)",
           subtitle="Census index values  ·  Florida = county averages",
           x=NULL, y="Census index") +
      theme_hw() +
      theme(axis.text.x=element_text(angle=50, hjust=1, size=7))
  }, bg=CLR_BG)

  # ── ELECTIONS (reactive) ──────────────────────────────────────────────────
  output$p_partisan <- renderPlot({
    yr_col <- input$elec_year
    yr_label <- c(m16="2016", m20="2020", m24="2024")[yr_col]
    win_col  <- if (yr_col=="m24") "winner24" else
                  factor(if_else(master[[yr_col]]>0,"GOP","DEM"), levels=c("GOP","DEM"))

    master %>%
      filter(!is.na(.data[[yr_col]])) %>%
      mutate(winner = factor(if_else(.data[[yr_col]]>0,"GOP","DEM"),
                             levels=c("GOP","DEM"))) %>%
      ggplot(aes(.data[[yr_col]], highways, size=highways,
                 colour=winner, label=state)) +
      geom_point(alpha=0.75) +
      geom_text_repel(size=2.5, max.overlaps=15, seed=42,
                      colour=CLR_MUTED, show.legend=FALSE) +
      scale_colour_manual(values=c(GOP=CLR_GOP, DEM=CLR_DEM), name="Winner") +
      scale_size_continuous(range=c(2,14), guide="none") +
      scale_x_continuous(labels=function(x) paste0(ifelse(x>0,"+",""),round(x,0),"pp")) +
      geom_vline(xintercept=0, linetype="dashed", colour=CLR_FAINT, linewidth=0.5) +
      labs(title=paste("GOP margin vs highway count —", yr_label),
           subtitle="Bubble size ∝ highway count  ·  Positive = GOP win",
           x=paste("GOP margin (pp) —", yr_label), y="Highway count") +
      theme_hw()
  }, bg=CLR_BG)

  output$p_party_bar <- renderPlot({
    master %>%
      filter(!is.na(winner24)) %>%
      arrange(winner24, desc(highways)) %>%
      mutate(state=fct_inorder(state)) %>%
      ggplot(aes(highways, state, fill=winner24)) +
      geom_col(width=0.75, show.legend=FALSE) +
      scale_fill_manual(values=c(GOP=CLR_GOP, DEM=CLR_DEM)) +
      scale_x_continuous(labels=comma) +
      labs(title="Highways by state\n(grouped by 2024 winner)",
           x="Highways", y=NULL) +
      theme_hw() +
      theme(axis.text.y=element_text(size=8))
  }, bg=CLR_BG)

  # ── ODMP VALUE BOXES ─────────────────────────────────────────────────────
  output$vb_odmp_total  <- renderValueBox(valueBox(
    nrow(odmp), "ODMP-matched highways", icon=icon("shield-halved"), color="green"))
  output$vb_odmp_states <- renderValueBox(valueBox(
    n_distinct(odmp$state), "States with ODMP data", icon=icon("map"), color="olive"))
  output$vb_odmp_age    <- renderValueBox(valueBox(
    paste0(round(mean(odmp$age_num, na.rm=TRUE), 1), " yrs"),
    "Mean officer age at death", icon=icon("user"), color="yellow"))
  output$vb_odmp_tour   <- renderValueBox(valueBox(
    paste0(round(mean(odmp$tour_years, na.rm=TRUE), 1), " yrs"),
    "Mean tour of duty", icon=icon("clock"), color="olive"))

  # ── CAUSE OF DEATH ────────────────────────────────────────────────────────
  output$p_odmp_cause <- renderPlot({
    odmp %>%
      count(cause_grp) %>%
      mutate(cause_grp = fct_reorder(cause_grp, n)) %>%
      ggplot(aes(n, cause_grp, fill=cause_grp)) +
      geom_col(width=0.75, show.legend=FALSE) +
      geom_text(aes(label=n), hjust=-0.2, size=3.5, colour=CLR_MUTED) +
      scale_fill_manual(values=cause_colours) +
      scale_x_continuous(expand=expansion(mult=c(0, 0.12))) +
      labs(title="Cause of death (n=275 ODMP records)",
           subtitle="Gunfire accounts for nearly half of all line-of-duty deaths",
           x="Count", y=NULL) +
      theme_hw()
  }, bg=CLR_BG)

  output$p_odmp_cause_state <- renderPlot({
    odmp %>%
      count(state, cause_grp) %>%
      group_by(state) %>%
      mutate(total=sum(n), state=fct_reorder(state, total)) %>%
      ungroup() %>%
      ggplot(aes(n, state, fill=cause_grp)) +
      geom_col(width=0.75) +
      scale_fill_manual(values=cause_colours, name=NULL) +
      scale_x_continuous(expand=expansion(mult=c(0, 0.06))) +
      labs(title="Cause of death by state",
           x="Count", y=NULL) +
      theme_hw() +
      theme(legend.position="bottom",
            legend.text=element_text(size=8))
  }, bg=CLR_BG)

  # ── END OF WATCH TIMELINE ────────────────────────────────────────────────
  output$p_odmp_eow_decade <- renderPlot({
    odmp %>%
      filter(!is.na(eow_decade)) %>%
      count(eow_decade) %>%
      ggplot(aes(factor(eow_decade), n, fill=n)) +
      geom_col(width=0.8, show.legend=FALSE) +
      geom_text(aes(label=n), vjust=-0.4, size=3.2, colour=CLR_MUTED) +
      scale_fill_gradient(low=CLR_NEUTRAL, high=CLR_GOP) +
      scale_y_continuous(expand=expansion(mult=c(0, 0.12))) +
      labs(title="End of watch — by decade",
           subtitle="1970s peak reflects Vietnam-era highway naming; 2010s also high",
           x="Decade", y="Officers") +
      theme_hw() +
      theme(axis.text.x=element_text(angle=30, hjust=1))
  }, bg=CLR_BG)

  output$p_odmp_eow_month <- renderPlot({
    odmp %>%
      filter(!is.na(eow_month)) %>%
      count(eow_month) %>%
      ggplot(aes(eow_month, n, fill=n)) +
      geom_col(width=0.8, show.legend=FALSE) +
      geom_text(aes(label=n), vjust=-0.4, size=3.2, colour=CLR_MUTED) +
      scale_fill_gradient(low=CLR_NEUTRAL, high=CLR_AMBER) +
      scale_y_continuous(expand=expansion(mult=c(0, 0.12))) +
      labs(title="End of watch — month of year",
           subtitle="May and August see the most line-of-duty deaths",
           x=NULL, y="Officers") +
      theme_hw()
  }, bg=CLR_BG)

  # ── AGE & TOUR ────────────────────────────────────────────────────────────
  output$p_odmp_age_hist <- renderPlot({
    odmp %>%
      filter(!is.na(age_num)) %>%
      ggplot(aes(age_num, fill=after_stat(count))) +
      geom_histogram(binwidth=5, colour=CLR_BG, linewidth=0.4, show.legend=FALSE) +
      scale_fill_gradient(low=CLR_NEUTRAL, high=CLR_GOP) +
      scale_x_continuous(breaks=seq(20, 70, 10)) +
      scale_y_continuous(expand=expansion(mult=c(0, 0.08))) +
      geom_vline(xintercept=mean(odmp$age_num, na.rm=TRUE),
                 linetype="dashed", colour=CLR_GOP, linewidth=0.8) +
      annotate("text", x=mean(odmp$age_num, na.rm=TRUE)+1.5,
               y=Inf, vjust=2, hjust=0, size=3,
               label=paste0("Mean: ", round(mean(odmp$age_num, na.rm=TRUE),1)),
               colour=CLR_GOP) +
      labs(title="Officer age at time of death",
           subtitle="Mean age 37 · Distribution skews young",
           x="Age (years)", y="Count") +
      theme_hw()
  }, bg=CLR_BG)

  output$p_odmp_tour_hist <- renderPlot({
    odmp %>%
      filter(!is.na(tour_years)) %>%
      ggplot(aes(tour_years, fill=after_stat(count))) +
      geom_histogram(binwidth=3, colour=CLR_BG, linewidth=0.4, show.legend=FALSE) +
      scale_fill_gradient(low=CLR_NEUTRAL, high=CLR_AMBER) +
      scale_y_continuous(expand=expansion(mult=c(0, 0.08))) +
      geom_vline(xintercept=mean(odmp$tour_years, na.rm=TRUE),
                 linetype="dashed", colour=CLR_AMBER, linewidth=0.8) +
      annotate("text", x=mean(odmp$tour_years, na.rm=TRUE)+0.8,
               y=Inf, vjust=2, hjust=0, size=3,
               label=paste0("Mean: ", round(mean(odmp$tour_years, na.rm=TRUE),1), " yrs"),
               colour=CLR_AMBER) +
      labs(title="Years of service at time of death",
           subtitle="Heavy left skew — many officers die within first 5 years",
           x="Years of service", y="Count") +
      theme_hw()
  }, bg=CLR_BG)

  output$p_odmp_age_tour <- renderPlot({
    odmp %>%
      filter(!is.na(age_num), !is.na(tour_years)) %>%
      ggplot(aes(tour_years, age_num, colour=cause_grp)) +
      geom_point(size=2.5, alpha=0.75) +
      geom_smooth(method="lm", se=TRUE, colour=CLR_TEXT, linewidth=0.7,
                  fill="#c8c4bc", alpha=0.3, show.legend=FALSE) +
      scale_colour_manual(values=cause_colours, name="Cause") +
      labs(title="Age vs years of service",
           subtitle=paste0("Correlation: r = ",
                           round(cor(odmp$age_num, odmp$tour_years, use="complete.obs"), 2),
                           "  ·  Color = cause of death"),
           x="Years of service", y="Age at death") +
      theme_hw() +
      theme(legend.position="bottom",
            legend.text=element_text(size=8))
  }, bg=CLR_BG)

  # ── CAUSE-FILTERED REACTIVE PLOTS ─────────────────────────────────────────
  odmp_filtered <- reactive({
    if (input$odmp_cause_filter == "all") odmp
    else odmp %>% filter(cause_grp == input$odmp_cause_filter)
  })

  output$p_odmp_cause_year <- renderPlot({
    df <- odmp_filtered()
    req(nrow(df) > 0)
    df %>%
      filter(!is.na(eow_year)) %>%
      count(eow_year, cause_grp) %>%
      ggplot(aes(eow_year, n, fill=cause_grp)) +
      geom_col(width=0.85) +
      scale_fill_manual(values=cause_colours, name=NULL) +
      scale_x_continuous(breaks=seq(1890, 2030, 20)) +
      scale_y_continuous(expand=expansion(mult=c(0, 0.08))) +
      labs(title="End of watch year",
           x="Year", y="Officers") +
      theme_hw() +
      theme(legend.position="bottom",
            legend.text=element_text(size=8))
  }, bg=CLR_BG)

  output$p_odmp_cause_age <- renderPlot({
    df <- odmp_filtered()
    req(nrow(df) > 0)
    df %>%
      filter(!is.na(age_num)) %>%
      ggplot(aes(age_num, fill=cause_grp)) +
      geom_histogram(binwidth=5, colour=CLR_BG, linewidth=0.4, show.legend=FALSE) +
      scale_fill_manual(values=cause_colours) +
      facet_wrap(~cause_grp, scales="free_y", ncol=2) +
      scale_y_continuous(expand=expansion(mult=c(0, 0.1))) +
      labs(title="Age at death by cause",
           x="Age (years)", y="Count") +
      theme_hw() +
      theme(strip.text=element_text(size=8))
  }, bg=CLR_BG)

  # ── ODMP TABLE ────────────────────────────────────────────────────────────
  output$tbl_odmp <- renderDT({
    odmp %>%
      select(
        State       = state,
        `Highway`   = highway_name,
        `Officer`   = odmp_name,
        `Age`       = age_num,
        `Service (yrs)` = tour_years,
        `Cause`     = cause_grp,
        `End of Watch` = odmp_end_of_watch,
        `Fuzzy score`  = odmp_fuzzy_score,
        `ODMP URL`     = odmp_url
      ) %>%
      mutate(`ODMP URL` = paste0('<a href="', `ODMP URL`, '" target="_blank">View</a>')) %>%
      datatable(
        escape       = FALSE,
        rownames     = FALSE,
        style        = "bootstrap",
        options      = list(pageLength=15, dom="ftp",
                            order=list(list(6, "desc"))),
        filter       = "top"
      )
  })

  # ── MEAN AGE & TOUR BY CAUSE ─────────────────────────────────────────────
  output$p_odmp_age_by_cause <- renderPlot({
    odmp %>%
      filter(!is.na(age_num)) %>%
      group_by(cause_grp) %>%
      summarise(mean_age=mean(age_num), se=sd(age_num)/sqrt(n()),
                n=n(), .groups="drop") %>%
      mutate(cause_grp=fct_reorder(cause_grp, mean_age)) %>%
      ggplot(aes(mean_age, cause_grp, fill=cause_grp)) +
      geom_col(width=0.7, show.legend=FALSE) +
      geom_errorbarh(aes(xmin=mean_age-se, xmax=mean_age+se),
                     height=0.25, colour=CLR_MUTED, linewidth=0.7) +
      geom_text(aes(label=paste0(round(mean_age,1)," yrs  (n=",n,")")),
                hjust=-0.1, size=3, colour=CLR_MUTED) +
      scale_fill_manual(values=cause_colours) +
      scale_x_continuous(expand=expansion(mult=c(0,0.18)),
                         limits=c(0, NA)) +
      labs(title="Mean officer age at death by cause",
           subtitle="Error bars = ±1 SE",
           x="Mean age (years)", y=NULL) +
      theme_hw()
  }, bg=CLR_BG)

  output$p_odmp_tour_by_cause <- renderPlot({
    odmp %>%
      filter(!is.na(tour_years)) %>%
      group_by(cause_grp) %>%
      summarise(mean_tour=mean(tour_years), se=sd(tour_years)/sqrt(n()),
                n=n(), .groups="drop") %>%
      mutate(cause_grp=fct_reorder(cause_grp, mean_tour)) %>%
      ggplot(aes(mean_tour, cause_grp, fill=cause_grp)) +
      geom_col(width=0.7, show.legend=FALSE) +
      geom_errorbarh(aes(xmin=pmax(0,mean_tour-se), xmax=mean_tour+se),
                     height=0.25, colour=CLR_MUTED, linewidth=0.7) +
      geom_text(aes(label=paste0(round(mean_tour,1)," yrs  (n=",n,")")),
                hjust=-0.1, size=3, colour=CLR_MUTED) +
      scale_fill_manual(values=cause_colours) +
      scale_x_continuous(expand=expansion(mult=c(0,0.18)),
                         limits=c(0, NA)) +
      labs(title="Mean years of service by cause of death",
           subtitle="Error bars = ±1 SE",
           x="Mean service years", y=NULL) +
      theme_hw()
  }, bg=CLR_BG)

  # ── AGE OVER TIME & FUZZY SCORE ──────────────────────────────────────────
  output$p_odmp_age_over_time <- renderPlot({
    odmp %>%
      filter(!is.na(age_num), !is.na(eow_decade)) %>%
      group_by(eow_decade) %>%
      summarise(mean_age=mean(age_num), se=sd(age_num)/sqrt(n()),
                n=n(), .groups="drop") %>%
      ggplot(aes(eow_decade, mean_age)) +
      geom_ribbon(aes(ymin=mean_age-se, ymax=mean_age+se),
                  fill=CLR_AMBER, alpha=0.25) +
      geom_line(colour=CLR_AMBER, linewidth=1.2) +
      geom_point(aes(size=n), colour=CLR_AMBER, fill=CLR_BG,
                 shape=21, stroke=1.5) +
      geom_text(aes(label=paste0(round(mean_age,1),"\n(n=",n,")")),
                vjust=-1.1, size=2.8, colour=CLR_MUTED) +
      scale_size_continuous(range=c(2,8), guide="none") +
      scale_x_continuous(breaks=seq(1890, 2030, 10)) +
      scale_y_continuous(limits=c(25, 55),
                         breaks=seq(25, 55, 5)) +
      labs(title="Mean officer age at death — by decade of EOW",
           subtitle="Point size ∝ number of records  ·  Shaded band = ±1 SE",
           x="End-of-watch decade", y="Mean age (years)") +
      theme_hw() +
      theme(axis.text.x=element_text(angle=30, hjust=1))
  }, bg=CLR_BG)

  output$p_odmp_fuzzy <- renderPlot({
    odmp %>%
      filter(!is.na(odmp_fuzzy_score)) %>%
      ggplot(aes(odmp_fuzzy_score, fill=match_quality)) +
      geom_histogram(binwidth=5, colour=CLR_BG, linewidth=0.4) +
      scale_fill_manual(
        values=c("Low (<70)"="#b8b0a4","Medium (70–79)"=CLR_AMBER,
                 "High (80–89)"=CLR_TEAL,"Exact (90+)"=CLR_DEM),
        name="Match quality") +
      geom_vline(xintercept=80, linetype="dashed",
                 colour=CLR_MUTED, linewidth=0.6) +
      annotate("text", x=81, y=Inf, vjust=2, hjust=0, size=2.8,
               colour=CLR_MUTED, label="80 threshold") +
      scale_x_continuous(breaks=seq(50,100,10)) +
      scale_y_continuous(expand=expansion(mult=c(0,0.1))) +
      labs(title="ODMP match quality — fuzzy score distribution",
           subtitle="Score reflects how closely highway name matched officer name",
           x="Fuzzy match score (0–100)", y="Count") +
      theme_hw() +
      theme(legend.position="bottom")
  }, bg=CLR_BG)

  # ── STATE ODMP PROFILE ───────────────────────────────────────────────────
  output$p_odmp_state_profile <- renderPlot({
    profile_long <- odmp_state_rate %>%
      filter(!is.na(avg_age)) %>%
      select(state, odmp_n,
             `Avg age\nat death`=avg_age,
             `Avg service\n(years)`=avg_tour,
             `% gunfire\ndeaths`=pct_gunfire) %>%
      pivot_longer(-c(state,odmp_n), names_to="metric", values_to="value") %>%
      mutate(state=fct_reorder(state, odmp_n, .desc=TRUE))

    ggplot(profile_long, aes(state, value, fill=metric)) +
      geom_col(width=0.75, show.legend=FALSE) +
      facet_wrap(~metric, scales="free_y", nrow=1) +
      scale_fill_manual(values=c(
        "Avg age\nat death"   = CLR_GOP,
        "Avg service\n(years)"= CLR_AMBER,
        "% gunfire\ndeaths"   = CLR_PURPLE
      )) +
      labs(title="State ODMP profile — avg officer age, service length & gunfire rate",
           subtitle="States sorted by number of ODMP-matched highways (left = most)",
           x=NULL, y=NULL) +
      theme_hw() +
      theme(axis.text.x=element_text(angle=40, hjust=1, size=8),
            strip.text=element_text(size=9, face="bold"))
  }, bg=CLR_BG)

  # ── INCIDENT DETAILS & WEAPON ────────────────────────────────────────────
  output$p_odmp_incident_words <- renderPlot({
    incident_words %>%
      slice_max(n, n=20) %>%
      mutate(words=fct_reorder(words, n)) %>%
      ggplot(aes(n, words, fill=n)) +
      geom_col(width=0.75, show.legend=FALSE) +
      geom_text(aes(label=n), hjust=-0.2, size=3, colour=CLR_MUTED) +
      scale_fill_gradient(low=CLR_NEUTRAL, high=CLR_DEM) +
      scale_x_continuous(expand=expansion(mult=c(0,0.15))) +
      labs(title="Top words in incident details",
           subtitle="From odmp_incident_details field",
           x="Frequency", y=NULL) +
      theme_hw()
  }, bg=CLR_BG)

  output$p_odmp_weapon <- renderPlot({
    odmp %>%
      count(weapon, cause_grp) %>%
      group_by(weapon) %>%
      mutate(total=sum(n), weapon=fct_reorder(weapon, total)) %>%
      ungroup() %>%
      ggplot(aes(n, weapon, fill=cause_grp)) +
      geom_col(width=0.7) +
      scale_fill_manual(values=cause_colours, name="Cause") +
      scale_x_continuous(expand=expansion(mult=c(0,0.06))) +
      labs(title="Weapon type parsed from incident details",
           subtitle="Stacked by cause of death category",
           x="Count", y=NULL) +
      theme_hw() +
      theme(legend.position="bottom",
            legend.text=element_text(size=8))
  }, bg=CLR_BG)

} # end server

# =============================================================================
# RUN
# =============================================================================
shinyApp(ui, server)
