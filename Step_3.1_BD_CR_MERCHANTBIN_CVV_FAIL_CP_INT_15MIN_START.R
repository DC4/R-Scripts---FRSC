		    #**************************************************************************************
			
									      #### CUM_AMT && CUM_SUM CALCULATION ####
			
			#*************************************************************************************
			
library(dplyr)
library(data.table)
# Analysis <- Country_Txn_Data
# Country_Txn_Data_1 <- Analysis
# To load CR / DB data for UDV Creation
Country_Txn_Data_1 <- data.table(Country_Txn_Data_CR)

Country_Txn_Data_1$MERCHANTBIN = cbind(trim(Country_Txn_Data_1$MER_ID), trim(substring(Country_Txn_Data_1$ACCT_NBR,1,6)))

# Sorting before performing cumulative count and cumulative amount
Country_Txn_Data_1 <- Country_Txn_Data_1[order(Country_Txn_Data_1$MERCHANTBIN,Country_Txn_Data_1$AUTH_DTTM_2), ]



# TIME for Calculation in seconds (ex: 30 mins => 1800 secs)
timethre <- 15

# Applying cumulative condition and filtering

Country_Txn_Data_1_UDV <- sqldf(paste0("Select * from Country_Txn_Data_1 where 
CRD_CLNT_ID IN ('SC_C400BD_CR') and
TRN_AUTH_POST = 'A' and
DECI_CD_ORIG = 'C' and
TRN_POS_ENT_CD NOT IN ('E', 'K', 'G') and
mer_id <> '' and
MER_CNTY_CD <> '050'"))

#USD > 0

# Converting to Data table
dt <- data.table(Country_Txn_Data_1_UDV)

# order by MERCHANTBIN and AUTH_DTTM_2
setkeyv(dt, c("MERCHANTBIN", "AUTH_DTTM_2"))

# separate into date and time columns
dt[, `:=`(date_1 = as.IDate(AUTH_DTTM_2), time_1 = as.ITime(AUTH_DTTM_2))]
dt[ , lag_date := shift(AUTH_DTTM_2),by=.(MERCHANTBIN)]
# dt[ , datetimediff_CVV_FAIL := AUTH_DTTM_2 - lag_date,by=.(MERCHANTBIN)]
dt[ , datetimediff_CVV_FAIL := difftime(AUTH_DTTM_2, lag_date, units = "mins"),by=.(MERCHANTBIN)]
dt[,datetimediff_CVV_FAIL_mins := as.numeric(datetimediff_CVV_FAIL)]
dt$datetimediff_CVV_FAIL_mins[is.na(dt$datetimediff_CVV_FAIL_mins)] <- 0
# Rounding datetimediff_CVV_FAIL
# To eliminate the error "Error in asfn(rs[[i]]) : need explicit units for numeric conversion" - we remove the attribute
dt$datetimediff_CVV_FAIL = round(as.numeric(dt$datetimediff_CVV_FAIL), digits = 2)

# Removing unwanted columns
dt$AUTH_DTTM_1 <- NULL
dt$AUTH_DTTM <- NULL
dt$date_1 <- NULL
dt$time_1 <- NULL

#defining functions
# Looping across all records to perform cumulative count and cumulative amount for each MERCHANTBIN and AUTH_DTTM_2

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
    group_10 = sapply(dt$datetimediff_CVV_FAIL_mins, cumsum_with_reset_group)
  )  %>% 
  ungroup())


##################### CR_BD_MERCHANTBIN_CVV_FAIL_CP_INT_15MIN_CNT ##########################

# Getting the CUM_COUNT count variable

setDT(dt1)[, CR_BD_MERCHANTBIN_CVV_FAIL_CP_INT_15MIN_CNT:=seq_len(.N), by=list(MERCHANTBIN, cumsum(dt1$group_10 >= 1))]

##################### CR_BD_MERCHANTBIN_CVV_FAIL_CP_INT_15MIN_AMT ##########################

# Getting the CUM_AMOUNT count variable

dt1$CR_BD_MERCHANTBIN_CVV_FAIL_CP_INT_15MIN_AMT <- unlist(tapply(dt1$TRN_AMT, cumsum(c(0, diff(dt1$CR_BD_MERCHANTBIN_CVV_FAIL_CP_INT_15MIN_CNT) < 1)), cumsum))

# Storing data with cumulative Sum and Count
Country_Txn_Data_1 <- dt1

# Sorting before performing cumulative count and cumulative amount
Country_Txn_Data_1 <- Country_Txn_Data_1[order(Country_Txn_Data_1$MERCHANTBIN,Country_Txn_Data_1$AUTH_DTTM_2),]

# Converting character to numeric
Country_Txn_Data_1$CR_BD_MERCHANTBIN_CVV_FAIL_CP_INT_15MIN_CNT <- as.numeric(as.character(Country_Txn_Data_1$CR_BD_MERCHANTBIN_CVV_FAIL_CP_INT_15MIN_CNT))
Country_Txn_Data_1$CR_BD_MERCHANTBIN_CVV_FAIL_CP_INT_15MIN_AMT <- as.numeric(as.character(Country_Txn_Data_1$CR_BD_MERCHANTBIN_CVV_FAIL_CP_INT_15MIN_AMT))

# Backup of data with Cumulative count and Cumulative Sum - including USD and AUTH_DTTM calculations
Analysis_USD_AUTH_CUM_2 <- Country_Txn_Data_1

# Head of Country_Txn_Data_1
head(Country_Txn_Data_1)
dim(Country_Txn_Data_1)