		    #**************************************************************************************
			
									      #### CUM_AMT && CUM_SUM CALCULATION ####
			
			#*************************************************************************************
			
library(dplyr)
library(data.table)
Analysis <- Country_Txn_Data
Country_Txn_Data_1 <- Analysis
Country_Txn_Data_1 <- subset(Analysis, ACCT_NBR == 5523438414339217)
# Sorting before performing cumulative count and cumulative amount
Country_Txn_Data_1 <- Country_Txn_Data_1[order(Country_Txn_Data_1$ACCT_NBR,Country_Txn_Data_1$AUTH_DTTM_2), ]
# Applying cumulative condition and filtering

# TIME for Calculation in seconds (ex: 30 mins => 1800 secs)
timethre <- 60

# # # Filtering for date as well
# mydata_udv <- read_excel("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/vba_test_UDV.xlsm")

# # Segregating Type I and II to fetch data
# if(tolower(mydata_udv$Type[i]) == "i"){
# type_dt_udv <- format(as.Date(Sys.Date() -1), format = "%Y-%m-%d")
# type_dt_udv <- format(as.Date(type_dt_udv), "%d-%m-%Y")
# # type_dt_udv <- dmy(type_dt_udv)
# } else {
# type_dt_udv <- format(as.Date(Sys.Date() -2), format = "%Y-%m-%d")
# type_dt_udv <- format(as.Date(type_dt_udv), "%d-%m-%Y")
# # type_dt_udv <- dmy(type_dt_udv)
# }

# Initial dimension of "Country_Txn_Data_1"
# dim(Country_Txn_Data_1)
# Creating data frame "Country_Txn_Data_1" and creating "Country_Txn_Data_1_UDV"
# Subsetting the UDV data dump for needed date range
# Country_Txn_Data_1$MER_CNTY_CD <- as.numeric(Country_Txn_Data_1$MER_CNTY_CD)

Country_Txn_Data_1_UDV <- sqldf(paste0("Select * from Country_Txn_Data_1 where 
TRN_AUTH_POST = 'A' and
AUTH_DECISION_XCD = 'A' and
TRN_POS_ENT_CD in ('E', 'K', 'G') and 
TRN_TYP in ('C', 'M', 'P') and
USD > 0"))
# MER_CNTY_CD != 344 and
#, " and trn_dt1 >= '" ,type_dt_udv, " '"))

# Temp Dimension of "Country_Txn_Data_1"
dim(Country_Txn_Data_1)
# Final Dimension of "Country_Txn_Data_1_UDV"
dim(Country_Txn_Data_1_UDV)
# Final Dimension of "Country_Txn_Data_1_excluding_UDV"
# dim(Country_Txn_Data_1_excluding_UDV)

# UDV Start time
start.time <- Sys.time()
print("UDV Start time: ")
print(start.time)

# Looping across all records to perform cumulative count and cumulative amount for each ACCT_NBR and AUTH_DTTM_2
dt <- data.table(Country_Txn_Data_1_UDV)
# Country_Txn_Data_1 <- subset(Analysis, ACCT_NBR == 4058038019570392)

# order by ACCT_NBR and AUTH_DTTM_2
setkeyv(dt, c("ACCT_NBR", "AUTH_DTTM_2"))

# separate into date and time columns
dt[, `:=`(date_1 = as.IDate(AUTH_DTTM_2), time_1 = as.ITime(AUTH_DTTM_2))]
dt[ , lag_date := shift(AUTH_DTTM_2),by=.(ACCT_NBR)]
# dt[ , datetimediff := AUTH_DTTM_2 - lag_date,by=.(ACCT_NBR)]
dt[ , datetimediff := difftime(AUTH_DTTM_2, lag_date, units = "mins"),by=.(ACCT_NBR)]
dt[,datetimediff_mins := as.integer(datetimediff)]
dt$datetimediff_mins[is.na(dt$datetimediff_mins)] <- 0

# Removing unwanted columns
dt$AUTH_DTTM_1 <- NULL
dt$AUTH_DTTM <- NULL
dt$date_1 <- NULL
dt$time_1 <- NULL

#new


#defining functions
  cumsum <- 0
  group <- 0
  result <- numeric()
cumsum_with_reset_group <- function(x) {
for (i in 1:length(x)) {
    cumsum <<- cumsum + x[i]
		print(cumsum)
    if (cumsum > timethre) {
      group <- group + 1
      cumsum <<- 0
	  }
    result = c(result, group) 
	}
  return (result)
}

cumsum_with_reset_group <- cmpfun(cumsum_with_reset_group)
sapply(dt$datetimediff_mins, cumsum_with_reset_group)


# cumsum with reset
cumsum_with_reset <- function(x) {
  cumsum <- 0
  group <- 0
  result <- numeric()
 
  for (i in 1:length(x)) {

    cumsum <<- cumsum + x[i]
    
    if (cumsum > timethre) {
      group <- group + 1
      cumsum <<- 0
	  }
    
    result = c(result, cumsum)
    
  }

  return (result)
}


# Now lets compile these functions, for a modest speed boost.
# We can either use enableJIT or cmpfun
# Using byte-code compiler
require(compiler)
# enableJIT(3)
# help(cmpfun)
cumsum_with_reset <- cmpfun(cumsum_with_reset)
cumsum_with_reset_group <- cmpfun(cumsum_with_reset_group)

# Using sapply for speed in processing
# use functions above as window functions inside mutate statement
#calling function
dt1 <- as.data.table(dt %>% group_by() %>%
  mutate(
    cumsum_10 = sapply(dt$datetimediff_mins, cumsum_with_reset),
    group_10 = sapply(dt$datetimediff_mins, cumsum_with_reset_group)
  )  %>% 
  ungroup())


##################### SRVC_Set_CNP_HOURLY_CNT ##########################

# Getting the CUM_COUNT count variable
# Method 1 with gg:
# dt1$gg <- cumsum(dt1$group_10 > 0)
# dt1$SRVC_Set_CNP_HOURLY_CNT_1 <- ave(dt1$group_10, dt1$gg, FUN=seq)
# Method 2 with data table
# library(data.table)
# setDT(dt1)[, SRVC_Set_CNP_HOURLY_CNT:=seq_len(.N), by=cumsum(dt1$group_10 > 0)]

# order by ACCT_NBR and group_10
# setkeyv(dt1, c("ACCT_NBR", "group_10"))
# setDT(dt1)[, SRVC_Set_CNP_HOURLY_CNT:=seq_len(.N), by=list(ACCT_NBR, cumsum(dt1$group_10 > 1))]
setDT(dt1)[, SRVC_Set_CNP_HOURLY_CNT:=seq_len(.N), by=list(ACCT_NBR, cumsum(dt1$group_10 >= 1))]

##################### SRVC_Set_CNP_HOURLY_AMT ##########################

# https://stackoverflow.com/questions/32994060/r-cumulative-sum-by-condition-with-reset
dt1$SRVC_Set_CNP_HOURLY_AMT <- unlist(tapply(dt1$USD, cumsum(c(0, diff(dt1$SRVC_Set_CNP_HOURLY_CNT) < 1)), cumsum))


# Country_Txn_Data_1 <- rbind(dt1, dt2)
Country_Txn_Data_1 <- dt1
# Final Dimension of "Country_Txn_Data_1"
dim(Country_Txn_Data_1)

# Sorting before performing cumulative count and cumulative amount
Country_Txn_Data_1 <- Country_Txn_Data_1[order(Country_Txn_Data_1$ACCT_NBR,Country_Txn_Data_1$AUTH_DTTM_2),]
# This format conversion is needed to load data into FALT002 and FALT003
# Country_Txn_Data_1$date_1 <- as.POSIXct(Country_Txn_Data_1$date_1 , format = "%d.%m.%Y %H:%M:%S");
# Country_Txn_Data_1$time_1 <- as.POSIXct(Country_Txn_Data_1$time_1 , format = "%d.%m.%Y %H:%M:%S");

# Converting character to numeric
Country_Txn_Data_1$SRVC_Set_CNP_HOURLY_CNT <- as.numeric(as.character(Country_Txn_Data_1$SRVC_Set_CNP_HOURLY_CNT))
Country_Txn_Data_1$SRVC_Set_CNP_HOURLY_AMT <- as.numeric(as.character(Country_Txn_Data_1$SRVC_Set_CNP_HOURLY_AMT))

# UDV End time
end.time <- Sys.time()
print("UDV End time: ")
print(end.time)

# UDV run time
print("UDV Run time: ")
time.taken <- end.time - start.time
print(time.taken)

# Creating needed View table for data check
UDV_Calculation_Table_Ref_1 <- Country_Txn_Data_1 %>%
  select(TRN_AUTH_POST, AUTH_DECISION_XCD, TRN_POS_ENT_CD, TRN_TYP, ACCT_NBR, AUTH_DTTM_2, datetimediff_mins , cumsum_10, group_10, USD, SRVC_Set_CNP_HOURLY_CNT, SRVC_Set_CNP_HOURLY_AMT)

UDV_Calculation_Table_Ref_2 <- Country_Txn_Data_1 %>%
  select(ACCT_NBR, AUTH_DTTM_2,AA_SCOR, lag_date, datetimediff_mins , group_10, SRVC_Set_CNP_HOURLY_CNT,  USD, SRVC_Set_CNP_HOURLY_AMT)
  
# subset(UDV_Calculation_Table_Ref_1, ACCT_NBR == 5523438414339217)

# View output
head(UDV_Calculation_Table_Ref_1)
UDV_Calculation_Table_Ref_1
# View(UDV_Calculation_Table_Ref_2)

# Backup of data with Cumulative count and Cumulative Sum - including USD and AUTH_DTTM calculations
Analysis_USD_AUTH_CUM <- Country_Txn_Data_1

# Transaction data
# head(Country_Txn_Data_1)
# View(Country_Txn_Data_1)