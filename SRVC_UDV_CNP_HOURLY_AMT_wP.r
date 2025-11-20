		    #**************************************************************************************
			
									      #### CUM_AMT && CUM_SUM CALCULATION ####
			
			#**************************************************************************************

# Subsetting for testing
# subz <- subset(Country_Txn_Data, ACCT_NBR == 4364623600371117)
# Fetch first 50 rows
# ttt <- Country_Txn_Data[1:50, ]
# Ordering by ACCT_NBR, AUTH_DTTM_2
# Country_Txn_Data <- Country_Txn_Data[with(Country_Txn_Data, order(ACCT_NBR, AUTH_DTTM_2)), ]
# Country_Txn_Data <- Country_Txn_Data[order(Country_Txn_Data$ACCT_NBR,Country_Txn_Data$AUTH_DTTM_2),]
# Filling up blank columns values
# subz[apply(subz, 2, function(x) x=="")] = NA
# To Sort based on ACCT_NBR & AUTH_DTTM
# subz_0 <- arrange(subz, AUTH_DTTM_2)
# Feature engineering the Cum_Sum and Cum_Amt attribites
# Country_Txn_Data <- all_data
# Country_Txn_Data <- Country_Txn_Data[1:500, ]
# Analysis <- Country_Txn_Data

# Data Dump of all original Country_Txn_Data
Analysis <- Country_Txn_Data

# To find Acct_Nbr and Count
# sqldf('Select distinct ACCT_NBR, count(ACCT_NBR) as cnt1 from Country_Txn_Data group by ACCT_NBR having count(ACCT_NBR) > 1 order by ACCT_NBR, cnt1 desc ')
# Subset Country_Txn_Data for analysis
# Country_Txn_Data <- subset(Country_Txn_Data, ACCT_NBR == 4706614429115543 | ACCT_NBR == 4706614429111419 | ACCT_NBR == 4706614429111971)
# Country_Txn_Data <- subset(Country_Txn_Data, ACCT_NBR == 4706614401843591)
# Reset the Country Data from "Analysis" dump
Country_Txn_Data <- Analysis
# Sorting before performing cumulative count and cumulative amount
Country_Txn_Data <- Country_Txn_Data[order(Country_Txn_Data$ACCT_NBR,Country_Txn_Data$AUTH_DTTM_2),]

# Initialize ACCT_NBR & Date_Time with default values
ACCT_NBR_PREV = "0000000000000000"
SRVC_UDV_CNP_HOURLY_AMT_wP_TIME = as.POSIXct("01.01.1960 00:00:00", format = "%d.%m.%Y %H:%M:%S", tz = "GMT");
# Initializing date format
# Country_Txn_Data$SRVC_UDV_CNP_HOURLY_AMT_wP_TIME = SRVC_UDV_CNP_HOURLY_AMT_wP_TIME

# SUBSET DATA FOR TESTING
# To find Acct_Nbr and Count
# sqldf('Select distinct ACCT_NBR, count(ACCT_NBR) as cnt1 from Country_Txn_Data group by ACCT_NBR having count(ACCT_NBR) > 10 order by ACCT_NBR, cnt1 desc ')
# Subset Country_Txn_Data for analysis
# Country_Txn_Data <- subset(Country_Txn_Data, ACCT_NBR == 5243600000860802 | ACCT_NBR == 5243600000895238 | ACCT_NBR == 5243600000792328)

# Applying cumulative condition and filtering

# Initial dimension of "Country_Txn_Data"
dim(Country_Txn_Data)
# Creating data frame "Country_Txn_Data_UDV"
Country_Txn_Data_UDV <- sqldf("Select * from Country_Txn_Data where 
CRD_CLNT_ID = 'SC_CCMSHK_CR' and
TRN_AUTH_POST = 'A' and
AUTH_DECISION_XCD = 'A' and
TRN_TYP in ('C', 'M', 'P') and
TRN_POS_ENT_CD in ('E', 'K', 'G') and 
TRN_AMT > 0"
)

# Obtaining rows from Country_Txn_Data and excluding the ones present in Country_Txn_Data_UDV
Country_Txn_Data_excluding_UDV <- sqldf("
Select * from Country_Txn_Data where FI_TRANSACTION_ID not in (
Select distinct (FI_TRANSACTION_ID) from Country_Txn_Data_UDV)")

# Temp Dimension of "Country_Txn_Data"
dim(Country_Txn_Data)
# Final Dimension of "Country_Txn_Data_UDV"
dim(Country_Txn_Data_UDV)
# Final Dimension of "Country_Txn_Data_excluding_UDV"
dim(Country_Txn_Data_excluding_UDV)

# UDV Start time
start.time <- Sys.time()
print("UDV Start time: ")
print(start.time)

# Looping across all records to perform cumulative count and cumulative amount for each ACCT_NBR and AUTH_DTTM_2
dt <- data.table(Country_Txn_Data_UDV)
# View(dt)
setkeyv(dt, c("ACCT_NBR", "AUTH_DTTM_2"))
dt[, `:=`(date_2 = as.IDate(AUTH_DTTM_2), time_2 = as.ITime(AUTH_DTTM_2))]
# dt[, datetime := NULL]

#---- counts and sums 10 mins ----
# new time by each 10 Minutes
dt[, SRVC_UDV_CNP_HOURLY_AMT_wP_TIME := floor_date(as.POSIXct(date_2) + time_2, "60 minutes")]

# count and sum by 10 Minutes
dt[,`:=`(SRVC_UDV_CNP_HOURLY_AMT_wP_CNT = as.numeric(seq_len(.N)), SRVC_UDV_CNP_HOURLY_AMT_wP_AMT = as.numeric(cumsum(USD), digits = 2)) , by = .(ACCT_NBR, SRVC_UDV_CNP_HOURLY_AMT_wP_TIME)]

# Rowbind the results to the original dataset after adding needed variables
dt1 <- data.table(Country_Txn_Data_excluding_UDV)
setkeyv(dt1, c("ACCT_NBR", "AUTH_DTTM_2"))
dt1[, `:=`(date_2 = as.IDate(AUTH_DTTM_2), time_2 = as.ITime(AUTH_DTTM_2))]
dt1$SRVC_UDV_CNP_HOURLY_AMT_wP_TIME = as.POSIXct("01.01.1960 00:00:00", format = "%d.%m.%Y %H:%M:%S", tz = "GMT");
dt1$SRVC_UDV_CNP_HOURLY_AMT_wP_CNT = ""
dt1$SRVC_UDV_CNP_HOURLY_AMT_wP_AMT = ""

Country_Txn_Data <- rbind(dt1, dt)
# Final Dimension of "Country_Txn_Data"
dim(Country_Txn_Data)

# Sorting before performing cumulative count and cumulative amount
Country_Txn_Data <- Country_Txn_Data[order(Country_Txn_Data$ACCT_NBR,Country_Txn_Data$AUTH_DTTM_2),]
# This format conversion is needed to load data into FALT002 and FALT003
Country_Txn_Data$date_2 <- as.POSIXct(Country_Txn_Data$date_2 , format = "%d.%m.%Y %H:%M:%S", tz = "GMT");
Country_Txn_Data$time_2 <- as.POSIXct(Country_Txn_Data$time_2 , format = "%d.%m.%Y %H:%M:%S", tz = "GMT");

# Converting character to numeric
Country_Txn_Data$SRVC_UDV_CNP_HOURLY_AMT_wP_CNT <- as.numeric(as.character(Country_Txn_Data$SRVC_UDV_CNP_HOURLY_AMT_wP_CNT))
Country_Txn_Data$SRVC_UDV_CNP_HOURLY_AMT_wP_AMT <- as.numeric(as.character(Country_Txn_Data$SRVC_UDV_CNP_HOURLY_AMT_wP_AMT))

# UDV End time
end.time <- Sys.time()
print("UDV End time: ")
print(end.time)

# UDV run time
print("UDV Run time: ")
time.taken <- end.time - start.time
print(time.taken)

# Creating needed View table for data check
UDV_Calculation_Table_Ref_1 <- Country_Txn_Data %>%
  select(ACCT_NBR, AUTH_DTTM_2, TRN_AUTH_POST, AUTH_DECISION_XCD, TRN_POS_ENT_CD, TRN_TYP, USD, date_2, time_2, SRVC_UDV_CNP_HOURLY_AMT_wP_TIME, SRVC_UDV_CNP_HOURLY_AMT_wP_CNT, SRVC_UDV_CNP_HOURLY_AMT_wP_AMT)
 
# View output
# head(UDV_Calculation_Table_Ref_1)
# View(UDV_Calculation_Table_Ref_1)

# Backup of data with Cumulative count and Cumulative Sum - including USD and AUTH_DTTM calculations
# Analysis_USD_AUTH_CUM <- Country_Txn_Data