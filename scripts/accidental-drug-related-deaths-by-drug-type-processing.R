library(dplyr)
library(datapkg)
library(data.table)

##################################################################
#
# Processing Script for Accidental Drug Related Deaths by Drug Type
# Created by Jenna Daly
# On 08/29/2017
#
##################################################################

#Setup environment
sub_folders <- list.files()
raw_location <- grep("raw", sub_folders, value=T)
path_to_raw_data <- (paste0(getwd(), "/", raw_location))
data_location <- grep("data$", sub_folders, value=T)
path_to_data <- (paste0(getwd(), "/", data_location))
drug_type <- dir(path_to_raw_data, recursive=T, pattern = "Drug") 

drug_type_df <- read.csv(paste0(path_to_raw_data, "/", drug_type), stringsAsFactors = FALSE, header=T, check.names = F) 

#Take care of known issues in raw data file
##Unmarked Causes by Case Number
# cases_to_fix <- c("15-10299", "15-10442", "15-10607", "15-11223", "15-1124", "15-427", "15-708", "15-7453", "15-7926", "15-8567", "15-9355", 
#                   "14-14455", "14-16253", "14-19695", "14-1041", "14-2865", "14-2822", "14-8058", "14-9125", "14-9547", "13-4966", "13-8249", 
#                   "13-14158", "13-19012", "13-177", "13-1386", "13-4979", "13-6190", "13-10345", "13-10551", "13-11032", "13-13389", "13-15279", 
#                   "12-1455", "12-4992", "12-1326", "12-1775", "12-1876", "12-2984", "12-4355", "12-7340", "12-9074", "12-10148", "12-10641", 
#                   "12-12304", "12-13501", "12-14519", "12-17804", "14-9876", "12-7789")

#Study of these cases resulted in the following generalized fixes
drug_type_df <- within(drug_type_df, Cocaine[grep("Cocaine", drug_type_df$ImmediateCauseA, ignore.case=T)] <- "Y")
drug_type_df <- within(drug_type_df, Methadone[grep("Methadone", drug_type_df$ImmediateCauseA, ignore.case=T)] <- "Y")
drug_type_df <- within(drug_type_df, EtOH[grep("Ethanol", drug_type_df$ImmediateCauseA, ignore.case=T)] <- "Y")
drug_type_df <- within(drug_type_df, Tramad[grep("Tramadol", drug_type_df$ImmediateCauseA, ignore.case=T)] <- "Y")
drug_type_df <- within(drug_type_df, Oxycodone[grep("oxycodone", drug_type_df$ImmediateCauseA, ignore.case=T)] <- "Y")
drug_type_df <- within(drug_type_df, Benzodiazepine[grep("clonazepam", drug_type_df$ImmediateCauseA, ignore.case=T)] <- "Y")
drug_type_df <- within(drug_type_df, Fentanyl[grep("Fentanyl", drug_type_df$ImmediateCauseA, ignore.case=T)] <- "Y")
drug_type_df <- within(drug_type_df, `Any Opioid`[grep("Buprenorphine", drug_type_df$ImmediateCauseA, ignore.case=T)] <- "Y")
drug_type_df <- within(drug_type_df, `Any Opioid`[grep("opioid", drug_type_df$ImmediateCauseA, ignore.case=T)] <- "Y")
drug_type_df <- within(drug_type_df, `Any Opioid`[grep("meperidine", drug_type_df$ImmediateCauseA, ignore.case=T)] <- "Y")
drug_type_df <- within(drug_type_df, `Any Opioid`[grep("opiate", drug_type_df$ImmediateCauseA, ignore.case=T)] <- "Y")
drug_type_df <- within(drug_type_df, `Any Opioid`[grep("opiate", drug_type_df$Other, ignore.case=T)] <- "Y")

# Any rows that list morphine in either cause of death or `Other` columns will have "Any Opioid" marked as "Y"
drug_type_df <- within(drug_type_df, `Any Opioid`[grep("morphine", drug_type_df$ImmediateCauseA, ignore.case=T)] <- "Y")
drug_type_df <- within(drug_type_df, `Any Opioid`[grep("morphine", drug_type_df$Other, ignore.case=T)] <- "Y")

# Any row where any of the following are marked - set "Any Opioid" as "Y"
drug_type_df <- within(drug_type_df, `Any Opioid`[grep("Y", drug_type_df$Heroin, ignore.case=T)] <- "Y")
drug_type_df <- within(drug_type_df, `Any Opioid`[grep("Y", drug_type_df$Fentanyl, ignore.case=T)] <- "Y")
drug_type_df <- within(drug_type_df, `Any Opioid`[grep("Y", drug_type_df$Oxycodone, ignore.case=T)] <- "Y")
drug_type_df <- within(drug_type_df, `Any Opioid`[grep("Y", drug_type_df$Oxymorphone, ignore.case=T)] <- "Y")
drug_type_df <- within(drug_type_df, `Any Opioid`[grep("Y", drug_type_df$Hydrocodone, ignore.case=T)] <- "Y")
drug_type_df <- within(drug_type_df, `Any Opioid`[grep("Y", drug_type_df$Methadone, ignore.case=T)] <- "Y")
drug_type_df <- within(drug_type_df, `Any Opioid`[grep("Y", drug_type_df$Tramad, ignore.case=T)] <- "Y")
drug_type_df <- within(drug_type_df, `Any Opioid`[grep("Y", drug_type_df$`Morphine (not heroin)`, ignore.case=T)] <- "Y")

# Rename geo column to Town, sex column to gender
drug_type_df <- drug_type_df %>% 
  rename(Town = `Death City`, Gender = Sex)
  
# Trim cases that we cannot compute
drug_type_df <- drug_type_df[(drug_type_df$Gender != "") & (drug_type_df$Date != ""),] #(remove 4 cases)

# Pull years out of date stamps
drug_type_df$Year <- gsub("([0-9]{2})/([0-9]{2})/([0-9]{4})", "\\3", drug_type_df$Date)
drug_type_df$Date <- NULL

#if town column is blank, and location column = Residence, and residence city is populated, set town to residence city (12-7789)
drug_type_df <- drug_type_df %>% 
  mutate(Town = ifelse((drug_type_df$Town == "") & (drug_type_df$Location == "Residence") & (drug_type_df$`Residence City` != ""), `Residence City`, Town))

#Clean up town names
source('./scripts/ctnamecleaner.R')
drug_type_df <- ctnamecleaner(Town, drug_type_df)

drug_type_df$Town <- NULL
drug_type_df <- drug_type_df %>% rename(Town = real.town.name)

# Pull ethnicity out of race column
drug_type_df$Ethnicity <- "Not Hispanic"
drug_type_df$Ethnicity[which(grepl("Hispanic", drug_type_df$Race))] <- "Hispanic"

# Recode race column
drug_type_df$Race[which(grepl("Hispanic, White", drug_type_df$Race))] <- "White"
drug_type_df$Race[which(grepl("Hispanic, Black", drug_type_df$Race))] <- "Black"
other_races <- c("Unknown", "Other", "", "Asian Indian", "Chinese", "Hawaiian")
drug_type_df$Race[which(drug_type_df$Race %in% other_races)] <- "Other"
drug_type_df$Race[which(grepl("Other", drug_type_df$Race))] <- "Other"

# Code/categorize age bands
# Get numeric value for age in new column
drug_type_df$num.age <- as.numeric(drug_type_df$Age)

# Recode each band
drug_type_df$Age[which(drug_type_df$num.age < 21)] <- "Under 21 years"
drug_type_df$Age[which(between(drug_type_df$num.age, 21, 45))] <- "21 to 45 years"
drug_type_df$Age[which(between(drug_type_df$num.age, 46, 60))] <- "46 to 60 years"
drug_type_df$Age[which(drug_type_df$num.age > 60)] <- "61 years and over"

# Remove working column
drug_type_df$num.age <- NULL

drug_type_df$Heroin[drug_type_df$Heroin == "y"] <- "Y"

################################################################################################################
#Now start to aggregate totals
# This will help classify the "other" drug cases
raw <- as.data.table(drug_type_df)

raw[
  ,
  listedDrugs := paste(
    Heroin, Cocaine, Fentanyl, Oxycodone, Oxymorphone, EtOH, `Hydro-codeine`, Benzodiazepine, 
    Methadone, Amphet, Tramad, `Morphine (not heroin)`, sep = ""),
  by = list(Heroin, Cocaine, Fentanyl, Oxycodone, Oxymorphone, EtOH, `Hydro-codeine`, Benzodiazepine, 
            Methadone, Amphet, Tramad, `Morphine (not heroin)`)
  ]

drugs <- list(
  Total = copy(raw),  # no filter
  `Any Opioid` = raw[`Any Opioid` == "Y"], # any opioid is Y
  `Only Opioids` = raw[ # any opioid is Y and the 4 non-opioids are not
    `Any Opioid` == "Y" &
      `Amphet` != "Y" &
      `Cocaine` != "Y" &
      `Benzodiazepine` != "Y" &
      `EtOH` != "Y"
  ],
  `Any Non-Heroin Opioid` = raw[ # any opioid is Y and heroin is not
    `Any Opioid` == "Y" &
      Heroin != "Y"
  ],
  `Only Non-Heroin Opioids` = raw[ # any opioid is Y and Heroin and the 4 non-opioids are not
    `Any Opioid` == "Y" &
      `Heroin` != "Y" &
      `Amphet` != "Y" &
      `Cocaine` != "Y" &
      `Benzodiazepine` != "Y" &
      `EtOH` != "Y"
  ],
  `Any Non-Opioid` = raw[
    `Amphet` == "Y" |
      `Cocaine` == "Y" |
      `Benzodiazepine` == "Y" |
      `EtOH` == "Y"
  ],
  `Only Non-Opioids` = raw[
    ( #any of the non opioids
      `Amphet` == "Y" |
        `Cocaine` == "Y" |
        `Benzodiazepine` == "Y" |
        `EtOH` == "Y"
    ) & ( # none of the opioids
      `Any Opioid` != "Y"
    )
  ]
)

for (drug in names(drugs)) {
  print(paste("Working on", drug))
  
  drugData <- drugs[[drug]]
  drugData <- drugData[, list(Year, Age, Gender, Race, Ethnicity, Town)]
  
  drugData$`Drug Type` = drug
  
  # get counts by all columns, least aggregated values
  drugData <- drugData[, list(Value = .N), by = list(Year, Age, Gender, Race, Ethnicity, Town, `Drug Type`)]
  
  # individual interactions
  age.totals <- copy(drugData)[, list(Age = "Total", Value = sum(Value)), by = list(Year, Gender, Race, Ethnicity, Town, `Drug Type`)]
  gender.totals <- copy(drugData)[, list(Gender = "Total", Value = sum(Value)), by = list(Year, Age, Race, Ethnicity, Town, `Drug Type`)]
  race.totals <- copy(drugData)[, list(Race = "Total", Value = sum(Value)), by = list(Year, Age, Gender, Ethnicity, Town, `Drug Type`)]
  ethnicity.totals <- copy(drugData)[, list(Ethnicity = "Total", Value = sum(Value)), by = list(Year, Age, Gender, Race, Town, `Drug Type`)]
  
  # double interactions
  age.gender.totals <- copy(drugData)[, list(Age = "Total", Gender = "Total", Value = sum(Value)), by = list(Year, Race, Ethnicity, Town, `Drug Type`)]
  age.race.totals <- copy(drugData)[, list(Age = "Total", Race = "Total", Value = sum(Value)), by = list(Year, Gender, Ethnicity, Town, `Drug Type`)]
  age.ethnicity.totals <- copy(drugData)[, list(Age = "Total", Ethnicity = "Total", Value = sum(Value)), by = list(Year, Gender, Race, Town, `Drug Type`)]
  
  gender.race.totals <- copy(drugData)[, list(Gender = "Total", Race = "Total", Value = sum(Value)), by = list(Year, Age, Ethnicity, Town, `Drug Type`)]
  gender.ethnicity.totals <- copy(drugData)[, list(Gender = "Total", Ethnicity = "Total", Value = sum(Value)), by = list(Year, Age, Race, Town, `Drug Type`)]
  
  race.ethnicity.totals <- copy(drugData)[, list(Race = "Total", Ethnicity = "Total", Value = sum(Value)), by = list(Year, Age, Gender, Town, `Drug Type`)]
  
  # triple interactions
  age.gender.race.totals <- copy(drugData)[, list(Age = "Total", Gender = "Total", Race = "Total", Value = sum(Value)), by = list(Year, Ethnicity, Town, `Drug Type`)]
  age.gender.ethnicity.totals <- copy(drugData)[, list(Age = "Total", Gender = "Total", Ethnicity = "Total", Value = sum(Value)), by = list(Year, Race, Town, `Drug Type`)]
  age.race.ethnicity.totals <- copy(drugData)[, list(Age = "Total", Race = "Total", Ethnicity = "Total", Value = sum(Value)), by = list(Year, Gender, Town, `Drug Type`)]
  gender.race.ethnicity.totals <- copy(drugData)[, list(Gender = "Total", Race = "Total", Ethnicity = "Total", Value = sum(Value)), by = list(Year, Age, Town, `Drug Type`)]
  
  # all four factor interactions
  age.gender.race.ethnicity.totals <- copy(drugData)[, list(Age = "Total", Gender = "Total", Race = "Total", Ethnicity = "Total", Value = sum(Value)), by = list(Year, Town, `Drug Type`)]
  
  drugs[[drug]] <- rbind(
    drugData,
    age.totals,
    gender.totals,
    race.totals,
    ethnicity.totals,
    age.gender.totals,
    age.race.totals,
    age.ethnicity.totals,
    gender.race.totals,
    gender.ethnicity.totals,
    race.ethnicity.totals,
    age.gender.race.totals,
    age.gender.ethnicity.totals,
    age.race.ethnicity.totals,
    gender.race.ethnicity.totals,
    age.gender.race.ethnicity.totals
  )
}

# combind all the drug datasets into one
drug_data <- rbindlist(drugs)

print("Rolling up state totals")
# Rollup state totals
state <- drug_data[, list(Town = "Connecticut", Value = sum(Value)), by = list(Year, Age, Gender, Race, Ethnicity, `Drug Type`)]
drug_data <- rbind(drug_data, state)

# cleanup
remove(
  drug,
  drugs,
  drugData,
  age.totals,
  gender.totals,
  race.totals,
  ethnicity.totals,
  age.gender.totals,
  age.race.totals,
  age.ethnicity.totals,
  gender.race.totals,
  gender.ethnicity.totals,
  race.ethnicity.totals,
  age.gender.race.totals,
  age.gender.ethnicity.totals,
  age.race.ethnicity.totals,
  gender.race.ethnicity.totals,
  age.gender.race.ethnicity.totals
)

# ## Backfill zero values so dataset is totally symmetrical
grid.factors <- list()
for (col in names(drug_data)[c(1:5,7)]) {
  grid.factors[[col]] <- unique(drug_data[, get(col)])
}
#Merge in FIPS
town_fips_dp_URL <- 'https://raw.githubusercontent.com/CT-Data-Collaborative/ct-town-list/master/datapackage.json'
town_fips_dp <- datapkg_read(path = town_fips_dp_URL)
fips <- (town_fips_dp$data[[1]])
fips <- as.data.table(fips)

grid.factors[["Town"]] <- unique(fips$Town)
backfill <- as.data.table(expand.grid(grid.factors))

drug_data <-  merge(backfill, drug_data, by=names(backfill), all.x = T)
drug_data[is.na(Value), Value := 0]

drug_data$Variable <- "Accidental Drug Related Deaths"
drug_data$`Measure Type` <- "Number"

# Bind in town fips
setkey(fips, Town)
setkey(drug_data, Town)
drug_data <- fips[drug_data]

#Set factors for sorting
drug_data <- transform(drug_data, Age = factor(Age, levels = c("Total", "Under 21 years", "21 to 45 years", "46 to 60 years", "61 years and over"), ordered=TRUE),
                                  Race = factor(Race, levels = c("Total", "White", "Black", "Other"), ordered=TRUE),
                                  Ethnicity = factor(Ethnicity, levels = c("Total", "Not Hispanic", "Hispanic"), ordered=TRUE), 
                                  Gender = factor(Gender, levels = c("Total", "Female", "Male"), ordered=TRUE),
                                  `Drug Type` = factor(`Drug Type`, levels = c("Total", "Any Opioid", "Only Opioids", "Any Non-Heroin Opioid", 
                                                                               "Only Non-Heroin Opioids", "Any Non-Opioid", "Only Non-Opioids"), ordered=TRUE)) 
                                                                               
#Order and sort columns         
drug_data <- drug_data %>% 
  select(Town, FIPS, Year, Age, Gender, Race, Ethnicity, `Drug Type`, `Measure Type`, Variable, Value) %>% 
  arrange(desc(Value))

# Write to File
write.table(
  drug_data,
  file.path(getwd(), "data", "accidental-drug-related-deaths-by-drug-type_2012-2016.csv"),
  sep = ",",
  row.names = F,
  na = "-9999"
)
