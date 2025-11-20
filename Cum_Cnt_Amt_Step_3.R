		    #**************************************************************************************
			
									      #### CUM_AMT && CUM_SUM CALCULATION ####
			
			#*************************************************************************************
			
library(dplyr)
library(data.table)
Analysis <- Country_Txn_Data
Country_Txn_Data_1 <- Analysis
# Country_Txn_Data_1 <- subset(Analysis, ACCT_NBR == 4325650270169602)
# Sorting before performing cumulative count and cumulative amount
Country_Txn_Data_1 <- Country_Txn_Data_1[order(Country_Txn_Data_1$ACCT_NBR,Country_Txn_Data_1$AUTH_DTTM_2), ]
# Applying cumulative condition and filtering

# TIME for Calculation in seconds (ex: 30 mins => 1800 secs)
timethre <- 60


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
cumulative_sum_with_reset <- cmpfun(cumulative_sum_with_reset)
cumsum_with_reset_group <- cmpfun(cumsum_with_reset_group)

# Using sapply for speed in processing
# use functions above as window functions inside mutate statement
#calling function
dt1 <- as.data.table(dt %>% group_by() %>%
  mutate(
    cumsum_10 = sapply(dt$datetimediff_mins, cumulative_sum_with_reset),
    group_10 = sapply(dt$datetimediff_mins, cumsum_with_reset_group)
  )  %>% 
  ungroup())


##################### SRVC_Set_CNP_HOURLY_CNT ##########################

# Getting the CUM_COUNT count variable

setDT(dt1)[, SRVC_Set_CNP_HOURLY_CNT:=seq_len(.N), by=list(ACCT_NBR, cumsum(dt1$group_10 >= 1))]

##################### SRVC_Set_CNP_HOURLY_AMT ##########################

# Getting the CUM_AMOUNT count variable

dt1$SRVC_Set_CNP_HOURLY_AMT <- unlist(tapply(dt1$USD, cumsum(c(0, diff(dt1$SRVC_Set_CNP_HOURLY_CNT) < 1)), cumsum))

# Country_Txn_Data_1 <- rbind(dt1, dt2)
Country_Txn_Data_1 <- dt1
# Final Dimension of "Country_Txn_Data_1"
dim(Country_Txn_Data_1)

# Sorting before performing cumulative count and cumulative amount
Country_Txn_Data_1 <- Country_Txn_Data_1[order(Country_Txn_Data_1$ACCT_NBR,Country_Txn_Data_1$AUTH_DTTM_2),]

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
  select(TRN_AUTH_POST, AUTH_DECISION_XCD, FI_TRANSACTION_ID, AA_SCOR, TRN_POS_ENT_CD, TRN_TYP, ACCT_NBR, AUTH_DTTM_2, lag_date, datetimediff_mins , cumsum_10, group_10, USD, SRVC_Set_CNP_HOURLY_CNT, SRVC_Set_CNP_HOURLY_AMT)

# subset(UDV_Calculation_Table_Ref_1, ACCT_NBR == 4509360650841527 | ACCT_NBR == 4509360650583434 | ACCT_NBR == 5533981232619052)

# View output
# View (UDV_Calculation_Table_Ref_1)

# Backup of data with Cumulative count and Cumulative Sum - including USD and AUTH_DTTM calculations
Analysis_USD_AUTH_CUM <- Country_Txn_Data_1

# Transaction data
# head(Country_Txn_Data_1)
# View(Country_Txn_Data_1)