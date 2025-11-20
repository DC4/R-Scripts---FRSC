		    #**************************************************************************************
			
									      #### CUM_AMT && CUM_SUM CALCULATION ####
			
			#*************************************************************************************
			
library(dplyr)
library(data.table)
# Analysis <- Country_Txn_Data
# Country_Txn_Data_2 <- Analysis
# To load CR / DB data for UDV Creation
Country_Txn_Data_2 <- data.table(Country_Txn_Data_DB)
# Sorting before performing cumulative count and cumulative amount
Country_Txn_Data_2 <- Country_Txn_Data_2[order(Country_Txn_Data_2$ACCT_NBR,Country_Txn_Data_2$AUTH_DTTM_2), ]

# TIME for Calculation in seconds (ex: 30 mins => 1800 secs)
timethre <- 1440

# Applying cumulative condition and filtering

Country_Txn_Data_2_UDV <- sqldf(paste0("Select * from Country_Txn_Data_2 where 
(
CRD_CLNT_ID not in  ('SC_HOGANHK_DB', 'SC_SPARROWBD_DB', 'SC_TANDEMTW_DB', 'SC_EURONETID_DB') and
TRN_AUTH_POST = 'A' and
TRN_POS_ENT_CD  IN ('E', 'K', 'G', '') and
USR_IND_4_NEW <> 'Y2'and
(
UPPER(MER_NM) LIKE '%FACEBK%' OR
UPPER(MER_NM) LIKE '%PAYPAL%' OR
UPPER(MER_NM) LIKE '%AMAZON%' OR
UPPER(MER_NM) LIKE '%WALMART%' OR
UPPER(MER_NM) LIKE '%ITUNES%' OR
UPPER(MER_NM) LIKE '%GOURMONDO%' OR
UPPER(MER_NM) LIKE '%MICROSOFT%' OR
UPPER(MER_NM) LIKE '%GOOGLE%'
)
)"
))

# MER_CNTY_CD != 344 and
#, " and trn_dt1 >= '" ,type_dt_udv, " '"))
# TRN_AUTH_POST = 'A' and
# AUTH_DECISION_XCD = 'A' and
# TRN_POS_ENT_CD in ('E', 'K', 'G') and 
# TRN_TYP in ('C', 'M', 'P') and
# USD > 0

# Converting to Data table
dt <- data.table(Country_Txn_Data_2_UDV)

# order by ACCT_NBR and AUTH_DTTM_2
setkeyv(dt, c("ACCT_NBR", "AUTH_DTTM_2"))

# separate into date and time columns
dt[, `:=`(date_1 = as.IDate(AUTH_DTTM_2), time_1 = as.ITime(AUTH_DTTM_2))]
dt[ , lag_date := shift(AUTH_DTTM_2),by=.(ACCT_NBR)]
# dt[ , datetimediff := AUTH_DTTM_2 - lag_date,by=.(ACCT_NBR)]
dt[ , datetimediff := difftime(AUTH_DTTM_2, lag_date, units = "mins"),by=.(ACCT_NBR)]
dt[,datetimediff_mins := as.integer(datetimediff)]
dt$datetimediff_mins[is.na(dt$datetimediff_mins)] <- 0
# Rounding datetimediff
# To eliminate the error "Error in asfn(rs[[i]]) : need explicit units for numeric conversion" - we remove the attribute
dt$datetimediff = round(as.numeric(dt$datetimediff), digits = 2)

# Removing unwanted columns
dt$AUTH_DTTM_1 <- NULL
dt$AUTH_DTTM <- NULL
dt$date_1 <- NULL
dt$time_1 <- NULL

#defining functions
# Looping across all records to perform cumulative count and cumulative amount for each ACCT_NBR and AUTH_DTTM_2

  cumsum <- 0
  group <- 0
  result <- numeric()
cumsum_with_reset_group <- function(x) {
for (i in 1:length(x)) {
    cumsum <<- cumsum + x[i]
    if (cumsum > timethre) {
      group <- group + 1
      cumsum <<- 0
	  }
    result = c(result, group) 
	}
  return (result)
}


# Now lets compile these functions, for a modest speed boost.
# We can either use enableJIT or cmpfun
# Using byte-code compiler
require(compiler)
# enableJIT(3)
# help(cmpfun)
cumsum_with_reset_group <- cmpfun(cumsum_with_reset_group)

# Using sapply for speed in processing
# use functions above as window functions inside mutate statement
dt1 <- as.data.table(dt %>% group_by() %>%
  mutate(
    group_10 = sapply(dt$datetimediff_mins, cumsum_with_reset_group)
  )  %>% 
  ungroup())


##################### DB_GBL_SRVC_UDV_2trans_1day_Count  ##########################

# Getting the CUM_COUNT count variable

setDT(dt1)[, DB_GBL_SRVC_UDV_2trans_1day_Count :=seq_len(.N), by=list(ACCT_NBR, cumsum(dt1$group_10 >= 1))]


dt1 %>%
  select(ATMONUS, AUTH_DTTM_2, lag_date, datetimediff_mins, group_10, Count)

dt1 %>%
  select(ATMONUS, datetimediff_mins, group_10, Count, AUTH_DTTM_2, ATMONUS_FIRST_TXN_DATE, ATMONUS_FIRST_APPROVAL_DATE )

##################### DB_GBL_SRVC_UDV_2trans_1day_Amount ##########################

# Getting the CUM_AMOUNT count variable

dt1$DB_GBL_SRVC_UDV_2trans_1day_Amount <- unlist(tapply(dt1$USD, cumsum(c(0, diff(dt1$DB_GBL_SRVC_UDV_2trans_1day_Count ) < 1)), cumsum))

# Storing data with cumulative Sum and Count
Country_Txn_Data_2 <- dt1

# Sorting before performing cumulative count and cumulative amount
Country_Txn_Data_2 <- Country_Txn_Data_2[order(Country_Txn_Data_2$ACCT_NBR,Country_Txn_Data_2$AUTH_DTTM_2),]

# Converting character to numeric
Country_Txn_Data_2$DB_GBL_SRVC_UDV_2trans_1day_Count  <- as.numeric(as.character(Country_Txn_Data_2$DB_GBL_SRVC_UDV_2trans_1day_Count ))
Country_Txn_Data_2$DB_GBL_SRVC_UDV_2trans_1day_Amount <- as.numeric(as.character(Country_Txn_Data_2$DB_GBL_SRVC_UDV_2trans_1day_Amount))

# Backup of data with Cumulative count and Cumulative Sum - including USD and AUTH_DTTM calculations
Analysis_USD_AUTH_CUM_2 <- Country_Txn_Data_2

# Rbinding the subsequent Cumulative results for each rule
Country_Txn_Data_1 <- full_join(Country_Txn_Data_1, Country_Txn_Data_2)

# Head of Country_Txn_Data_1
head(Country_Txn_Data_1)
