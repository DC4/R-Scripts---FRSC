		    #**************************************************************************************
			
									      #### CUM_AMT && CUM_SUM CALCULATION ####
			
			#*************************************************************************************
			
library(dplyr)
library(data.table)
# Analysis <- Country_Txn_Data
# Country_Txn_Data_1 <- Analysis
################################### CHANGE HERE FOR EVERYTIME DEPENDING ON DATA ###################################
# To load CR / DB data for UDV Creation => CHANGE HERE FOR EVERYTIME DEPENDING ON DATA
Country_Txn_Data_1 <- data.table(Country_Txn_Data_CR)

###################################### TEST #############################################
#########################################################################################
Country_Txn_Data_1 <- subset(Country_Txn_Data_1, ACCT_NBR ==  "4703471000946629")

# Sorting before performing cumulative count and cumulative amount
Country_Txn_Data_1 <- Country_Txn_Data_1[order(Country_Txn_Data_1$ATMONUS,Country_Txn_Data_1$AUTH_DTTM_2), ]

# TIME for Calculation in seconds (ex: 30 mins => 1800 secs)
timethre <- 1440

# Remove dups
# Country_Txn_Data_1[!duplicated(Country_Txn_Data_1), ]
Country_Txn_Data_1 <- subset(Country_Txn_Data_1, !duplicated(subset(Country_Txn_Data_1, select=c(FI_TRANSACTION_ID))))

# New Date
# Country_Txn_Data_1$New_Date <- as.Date(Country_Txn_Data_1$AUTH_DTTM_2, "%d%b%Y")
# Country_Txn_Data_1$New_Date <- as.POSIXct(Country_Txn_Data_1$AUTH_DTTM_2, "%Y-%m-%d")
###################################### TEST #############################################
#########################################################################################
Country_Txn_Data_1 <- subset(Country_Txn_Data_1, AUTH_DTTM_2 > "2021-05-01")

Country_Txn_Data_1 <- sqldf(paste0('Select * from Country_Txn_Data_1 where 
CRD_CLNT_ID = "SC_CCMSTW_CR"'))

# Applying cumulative condition and filtering

# Converting to Data table
dt <- data.table(Country_Txn_Data_1)

# order by ATMONUS and AUTH_DTTM_2
setkeyv(dt, c("ATMONUS", "AUTH_DTTM_2"))

# separate into date and time columns
dt[, `:=`(date_1 = as.IDate(AUTH_DTTM_2), time_1 = as.ITime(AUTH_DTTM_2))]
dt[ , lag_date := shift(AUTH_DTTM_2),by=.(ATMONUS)]
# dt[ , datetimediff := AUTH_DTTM_2 - lag_date,by=.(ATMONUS)]
dt[ , datetimediff := difftime(AUTH_DTTM_2, lag_date, units = "mins"),by=.(ATMONUS)]
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

# defining functions
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

  
##################### Txn_No Determination  ##########################

# Getting the CUM_COUNT count variable

setDT(dt1)[, Txn_No :=seq_len(.N), by=list(ATMONUS, cumsum(dt1$group_10 >= 1))]

##############  ATMONUS_FIRST_TXN_DATE  ##############


dt2 <- sqldf(paste0('Select * from dt1 where 
TRN_AUTH_POST = "A" AND
MER_ID <> "" AND
Txn_No = "1"'))
  
# Get the First Txn date for each txn
xxx <- dt2 %>% 
  group_by(ATMONUS) %>% 
  summarize(Min_Date = min(AUTH_DTTM_2))

# Converting to Data frame
yyy <- data.frame(xxx)

# Writing the First txn date for each ATMONUS
dt1$ATMONUS_FIRST_TXN_DATE = yyy[match(dt1$ATMONUS, yyy$ATMONUS), 2]

##############  ATMONUS_FIRST_APPROVAL_DATE  ##############

# Create subset for the condition
dt3 <- sqldf(paste0('Select * from dt1 where 
TRN_TYP IN ("C", "M", "P") AND (AUTH_DECISION_XCD = "A" OR DECI_CD_ORIG = "A")AND
Txn_No = "1"'))

# Map the min date for the records
dt3$ATMONUS_FIRST_APPROVAL_DATE = yyy[match(dt3$ATMONUS, yyy$ATMONUS), 2]

# Recods not matching the condition
dt4 <- anti_join(dt1, dt3, by = "FI_TRANSACTION_ID")
dt4$ATMONUS_FIRST_APPROVAL_DATE = '1960-01-01 00:00:01'

dt1 <- rbind(dt3, dt4)

# Sorting before performing cumulative count and cumulative amount
dt1 <- dt1[order(dt1$ATMONUS,dt1$AUTH_DTTM_2), ]

#################################  ATMONUS LOGIC  #####################################

dt11 <- sqldf(paste0("Select * from dt1 where Txn_No = '1'"))

# Recods not matching the condition
dt1 <- anti_join(dt1, dt11, by = "FI_TRANSACTION_ID")

dt11$TIME_RESET_ATMONUS_DAILY = 0;
dt11$CUM_COUNT_ATMONUS_DAILY = 0;
dt11$CUM_AMOUNT_ATMONUS_DAILY = 0;
dt11$CUM_COUNT_APPR_ATMONUS_DAILY = 0;
dt11$CUM_AMOUNT_APPR_ATMONUS_DAILY = 0;
dt11$ATMONUS_DAILY_BASE_SCORE=0;
dt11$ATMONUS_DAILY_AA_SCORE=0;
dt11$CUM_COUNT_MAX_ATMONUS_DAILY=0;
dt11$CUM_AMOUNT_MAX_ATMONUS_DAILY=0;
dt11$ATMONUS_FIRST_TXN_DATE=dt11$AUTH_DTTM_2;

# Joining
dt1 <- rbind(setDT(dt1), setDT(dt11), fill=TRUE)
# Sorting before performing cumulative count and cumulative amount
dt1 <- dt1[order(dt1$ATMONUS,dt1$AUTH_DTTM_2), ]

dt1$TIME_RESET_ATMONUS_DAILY[is.na(dt1$TIME_RESET_ATMONUS_DAILY)] <- 1
dt1$CUM_COUNT_ATMONUS_DAILY[is.na(dt1$CUM_COUNT_ATMONUS_DAILY)] <- 1
dt1$CUM_AMOUNT_ATMONUS_DAILY[is.na(dt1$CUM_AMOUNT_ATMONUS_DAILY)] <- 1
dt1$CUM_COUNT_APPR_ATMONUS_DAILY[is.na(dt1$CUM_COUNT_APPR_ATMONUS_DAILY)] <- 1
dt1$CUM_AMOUNT_APPR_ATMONUS_DAILY[is.na(dt1$CUM_AMOUNT_APPR_ATMONUS_DAILY)] <- 1
dt1$ATMONUS_DAILY_BASE_SCORE[is.na(dt1$ATMONUS_DAILY_BASE_SCORE)] <- 1
dt1$ATMONUS_DAILY_AA_SCORE[is.na(dt1$ATMONUS_DAILY_AA_SCORE)] <- 1
dt1$CUM_COUNT_MAX_ATMONUS_DAILY[is.na(dt1$CUM_COUNT_MAX_ATMONUS_DAILY)] <- 1
dt1$CUM_AMOUNT_MAX_ATMONUS_DAILY[is.na(dt1$CUM_AMOUNT_MAX_ATMONUS_DAILY)] <- 1


##############################  Step : 1  #########################

# Getting the CUM_COUNT_ATMONUS_DAILY count variable
setDT(dt1)[, CUM_COUNT_ATMONUS_DAILY:=as.numeric(seq_len(.N)), by=list(ATMONUS, cumsum(dt1$group_10 >= 1))]

# Getting the CUM_AMOUNT_ATMONUS_DAILY count variable
dt1$CUM_AMOUNT_ATMONUS_DAILY <- unlist(tapply(dt1$TRN_AMT, cumsum(c(0, diff(dt1$CUM_COUNT_ATMONUS_DAILY) < 1)), cumsum))

# Getting the APPR variables
setDT(dt1)[, CUM_COUNT_APPR_ATMONUS_DAILY := as.numeric(0), by=list(ATMONUS, (dt1$group_10 >= 1))]
setDT(dt1)[, CUM_AMOUNT_APPR_ATMONUS_DAILY := as.numeric(0), by=list(ATMONUS, (dt1$group_10 >= 1))]

##############################  Step : 2  #########################

# Getting the CUM_COUNT_MAX_ATMONUS_DAILY & CUM_AMOUNT_MAX_ATMONUS_DAILY count variable
setDT(dt1)[, CUM_COUNT_MAX_ATMONUS_DAILY := as.numeric(max(CUM_COUNT_MAX_ATMONUS_DAILY, CUM_COUNT_APPR_ATMONUS_DAILY)), by=list(ATMONUS, dt1$group_10 >= 1)]

setDT(dt1)[, CUM_AMOUNT_MAX_ATMONUS_DAILY := as.numeric(max(CUM_AMOUNT_MAX_ATMONUS_DAILY, CUM_AMOUNT_APPR_ATMONUS_DAILY)), by=list(ATMONUS, dt1$group_10 >= 1)]

# Getting the ATMONUS_DAILY_DAYS count variable
setDT(dt1)[, ATMONUS_DAILY_DAYS:=seq_len(.N), by=list(ATMONUS, cumsum(dt1$group_10 >= 1))]

##############################  Step : 3  #########################

# Create subset for the condition
dt5 <- sqldf(paste0('Select * from dt1 where 
TRN_TYP IN ("C", "M", "P") AND (AUTH_DECISION_XCD = "A" OR DECI_CD_ORIG = "A") AND TRN_AMT > 0'))

# Recods not matching the condition
dt1 <- anti_join(dt1, dt5, by = "FI_TRANSACTION_ID")

# Sorting before performing cumulative count and cumulative amount
dt5 <- dt5[order(dt5$ATMONUS,dt5$AUTH_DTTM_2), ]

# Getting the CUM_COUNT_APPR_ATMONUS_DAILY count variable
setDT(dt5)[, CUM_COUNT_APPR_ATMONUS_DAILY:=as.numeric(seq_len(.N)), by=list(ATMONUS, cumsum(group_10 >= 1))]

# Getting the CUM_AMOUNT_APPR_ATMONUS_DAILY count variable
dt5$CUM_AMOUNT_APPR_ATMONUS_DAILY <- unlist(tapply(dt5$TRN_AMT, cumsum(c(0, diff(dt5$CUM_COUNT_APPR_ATMONUS_DAILY) < 1)), cumsum))

##############################  Step : 4  #########################

# Getting the ATMONUS_DAILY_BASE_SCORE & ATMONUS_DAILY_AA_SCORE count variable
setDT(dt5)[, ATMONUS_DAILY_BASE_SCORE := as.numeric(max(ATMONUS_DAILY_BASE_SCORE, FRD_SCOR)), by=list(ATMONUS, group_10 < 1)]

setDT(dt5)[, ATMONUS_DAILY_AA_SCORE := as.numeric(max(ATMONUS_DAILY_AA_SCORE, AA_SCOR)), by=list(ATMONUS, group_10 < 1)]

##############################  Step : 5  #########################

# Getting the ATMONUS_DAILY_PREV, lag_date, ATMONUS_DAILY_BASE_SCORE, ATMONUS_DAILY_AA_SCOR variables

setDT(dt5)[, ATMONUS_DAILY_PREV := lag_date, by=list(ATMONUS, group_10 < 1)]

setDT(dt5)[, lag_date := AUTH_DTTM_2, by=list(ATMONUS, group_10 < 1)]

setDT(dt5)[, ATMONUS_DAILY_BASE_SCORE := FRD_SCOR, by=list(ATMONUS, group_10 < 1)]

setDT(dt5)[, ATMONUS_DAILY_AA_SCOR := AA_SCOR, by=list(ATMONUS, group_10 < 1)]

# Joining
dt1 <- rbind(setDT(dt1), setDT(dt5), fill=TRUE)

dt1 %>%
  select(ATMONUS, AUTH_DTTM_2, lag_date, datetimediff_mins, group_10, Txn_No, TIME_RESET_ATMONUS_DAILY ,CUM_COUNT_ATMONUS_DAILY ,CUM_AMOUNT_ATMONUS_DAILY ,CUM_COUNT_APPR_ATMONUS_DAILY
,CUM_AMOUNT_APPR_ATMONUS_DAILY ,ATMONUS_DAILY_BASE_SCORE ,ATMONUS_DAILY_AA_SCORE ,CUM_COUNT_MAX_ATMONUS_DAILY
,CUM_AMOUNT_MAX_ATMONUS_DAILY ,ATMONUS_FIRST_TXN_DATE)

  
  



















# Getting the CUM_AMOUNT_APPR_ATMONUS_DAILY count variable
dt5$CUM_AMOUNT_APPR_ATMONUS_DAILY <- unlist(tapply(dt5$TRN_AMT, cumsum(c(0, diff(dt5$CUM_COUNT_APPR_ATMONUS_DAILY) < 1)), cumsum))


dt5$ATMONUS_DAILY_BASE_SCORE = max(dt5$ATMONUS_DAILY_BASE_SCORE, dt5$FRD_SCOR);
dt5$ATMONUS_DAILY_AA_SCORE = max(dt5$ATMONUS_DAILY_AA_SCORE, dt5$AA_SCOR);

# Records not matching the condition in dt5
dt6 <- anti_join(dt1, dt5, by = "FI_TRANSACTION_ID")
dt6$CUM_COUNT_MAX_ATMONUS_DAILY = max(dt6$CUM_COUNT_MAX_ATMONUS_DAILY, dt6$CUM_COUNT_APPR_ATMONUS_DAILY);
dt6$CUM_AMOUNT_MAX_ATMONUS_DAILY = max(dt6$CUM_AMOUNT_MAX_ATMONUS_DAILY, dt6$CUM_AMOUNT_APPR_ATMONUS_DAILY);

# Getting the CUM_COUNT_APPR_ATMONUS_DAILY count variable
setDT(dt6)[, ATMONUS_DAILY_DAYS:=seq_len(.N), by=list(ATMONUS, cumsum(dt6$group_10 >= 1))]

dt6$CUM_COUNT_ATMONUS_DAILY = 1;

dt6$CUM_AMOUNT_ATMONUS_DAILY = dt6$TRN_AMT;
dt6$CUM_COUNT_APPR_ATMONUS_DAILY = 0;
dt6$CUM_AMOUNT_APPR_ATMONUS_DAILY = 0;



dt1 %>%
  select(ATMONUS, AUTH_DTTM_2, lag_date, datetimediff_mins, group_10, Txn_No, CUM_COUNT_ATMONUS_DAILY, CUM_AMOUNT_ATMONUS_DAILY, TRN_AMT)














##################### Getting the data ready for UDV - pre logic ##########################

# To find out first occurance of different ATMONUS
ATMONUS_first <- dt1[match(unique(dt1$ATMONUS), dt1$ATMONUS),]
# removing subset data frame
dt1 <- anti_join(dt1,ATMONUS_first)

ATMONUS_first$ATMONUS_FIRST_TXN_DATE = 0
ATMONUS_first$ATMONUS_FIRST_APPROVAL_DATE = 0
dt1$ATMONUS_FIRST_TXN_DATE = 1
dt1$ATMONUS_FIRST_APPROVAL_DATE = 1

# Appending the rows after logic is applied
dt1 <- rbind(dt1, ATMONUS_first)

# order by ATMONUS and AUTH_DTTM_2
setkeyv(dt1, c("ATMONUS", "AUTH_DTTM_2"))

dt111 <- subset(dt1 , TRN_AUTH_POST == 'A' & MER_ID != '' & ATMONUS_FIRST_TXN_DATE == 0)
df22 <- subset(dt1 , TRN_AUTH_POST == 'A' & MER_ID != '' & ATMONUS_FIRST_APPROVAL_DATE == 0 & TRN_TYP %in% c('C', 'M', 'P') & (AUTH_DECISION_XCD == 'A' | DECI_CD_ORIG == 'A') & TRN_AMT > 0)

# removing subset data frame
dt1 <- anti_join(dt1,dt111)
dt1 <- anti_join(dt1,df22)

# Applying the needed logic in Subset dataframe
dt111$ATMONUS_FIRST_TXN_DATE <- dt111$AUTH_DTTM_2
df22$ATMONUS_FIRST_APPROVAL_DATE <- df22$AUTH_DTTM_2

# Converting data type for Rbind
dt1$ATMONUS_FIRST_TXN_DATE <- as.POSIXct(dt1$ATMONUS_FIRST_TXN_DATE, origin = '2011-07-15 13:00:00')
dt1$ATMONUS_FIRST_APPROVAL_DATE <- as.POSIXct(dt1$ATMONUS_FIRST_APPROVAL_DATE, origin = '2011-07-15 13:00:00')

dt111$ATMONUS_FIRST_APPROVAL_DATE <- as.POSIXct(dt111$ATMONUS_FIRST_APPROVAL_DATE, origin = '2011-07-15 13:00:00')
df22$ATMONUS_FIRST_TXN_DATE <- as.POSIXct(df22$ATMONUS_FIRST_TXN_DATE, origin = '2011-07-15 13:00:00')

# Appending the rows after logic is applied
dt1 <- rbind(dt1, dt111)
dt1 <- rbind(dt1, df22)

# Sorting based in ATMONUS and AUTH_DTTM
# dt1 <- dt1[order(dt1$ATMONUS,dt1$AUTH_DTTM_2), ]
setkeyv(dt1, c("ATMONUS", "AUTH_DTTM_2"))
dt1 <- subset(dt1, !duplicated(subset(dt1, select=c(FI_TRANSACTION_ID))))




dt1 %>%
  select(ATMONUS, datetimediff_mins, group_10, AUTH_DTTM_2, ATMONUS_FIRST_TXN_DATE, ATMONUS_FIRST_APPROVAL_DATE )




##################### ATMONUS UDV LOGIC ##########################

# IF FIRST.ATMONUS THEN DO; => This part

setDT(dt1)[, TIME_RESET_ATMONUS_DAILY:=seq_len(.N), by=list(ATMONUS, cumsum(dt1$group_10 >= 1))]
setDT(dt1)[, CUM_COUNT_ATMONUS_DAILY:=seq_len(.N), by=list(ATMONUS, cumsum(dt1$group_10 >= 1))]
setDT(dt1)[, CUM_AMOUNT_ATMONUS_DAILY:=seq_len(.N), by=list(ATMONUS, cumsum(dt1$group_10 >= 1))]
setDT(dt1)[, CUM_COUNT_APPR_ATMONUS_DAILY:=seq_len(.N), by=list(ATMONUS, cumsum(dt1$group_10 >= 1))]
setDT(dt1)[, CUM_AMOUNT_APPR_ATMONUS_DAILY:=seq_len(.N), by=list(ATMONUS, cumsum(dt1$group_10 >= 1))]
setDT(dt1)[, ATMONUS_DAILY_BASE_SCORE:=seq_len(.N), by=list(ATMONUS, cumsum(dt1$group_10 >= 1))]
setDT(dt1)[, ATMONUS_DAILY_AA_SCORE:=seq_len(.N), by=list(ATMONUS, cumsum(dt1$group_10 >= 1))]
setDT(dt1)[, CUM_COUNT_MAX_ATMONUS_DAILY:=seq_len(.N), by=list(ATMONUS, cumsum(dt1$group_10 >= 1))]
setDT(dt1)[, CUM_AMOUNT_MAX_ATMONUS_DAILY:=seq_len(.N), by=list(ATMONUS, cumsum(dt1$group_10 >= 1))]

# We need count to assign AUTH_DTTM2 to the variable ATMONUS_FIRST_TXN_DATE
setDT(dt1)[, SRVC_Set_CNP_HOURLY_CNT:=seq_len(.N), by=list(ATMONUS, cumsum(dt1$group_10 >= 1))]
df33 <- subset(dt1 , SRVC_Set_CNP_HOURLY_CNT = '0')
# removing subset data frame
dt1 <- anti_join(dt1,df33)
# Applying the needed logic in Subset dataframe
df33$ATMONUS_FIRST_TXN_DATE <- df33$AUTH_DTTM_2
# Appending the rows after logic is applied
dt1 <- rbind(dt1, df33)
# Sorting based in ATMONUS and AUTH_DTTM
dt1 <- dt1[order(dt1$ATMONUS,dt1$AUTH_DTTM_2), ]

# IF &CUMULATIVE_CONDITION. THEN DO; => This part

dt2 <- subset(dt1 , TRN_AUTH_POST == 'A' & MER_ID != '')
# removing subset data frame
dt1 <- anti_join(dt1,dt2)
setDT(dt2)[, CUM_COUNT_ATMONUS_DAILY:=seq_len(.N), by=list(ATMONUS, cumsum(dt2$group_10 >= 1))]
#################################### MODIFIED to LE previous logic was LT #########################################
dt2$CUM_AMOUNT_ATMONUS_DAILY <- unlist(tapply(dt2$TRN_AMT, cumsum(c(0, diff(dt2$CUM_COUNT_ATMONUS_DAILY) < 1)), cumsum))

df44 <- subset(dt2 , TRN_TYP %in% c('C', 'M', 'P') & (AUTH_DECISION_XCD == 'A' | DECI_CD_ORIG == 'A') & TRN_AMT > 0)
# removing subset data frame
dt2 <- anti_join(dt2,df44)
# Applying the needed logic in Subset dataframe
setDT(df44)[, CUM_COUNT_APPR_ATMONUS_DAILY:=seq_len(.N), by=list(ATMONUS, cumsum(df44$group_10 >= 1))]
df44$CUM_AMOUNT_APPR_ATMONUS_DAILY <- unlist(tapply(df44$TRN_AMT, cumsum(c(0, diff(df44$CUM_COUNT_APPR_ATMONUS_DAILY) < 1)), cumsum))
# Appending the rows after logic is applied
dt2 <- rbind(dt2, df44)
dt1 <- rbind(dt1, dt2)

dt1$ATMONUS_DAILY_BASE_SCORE = max(dt1$ATMONUS_DAILY_BASE_SCORE,dt1$FRD_SCOR)
dt1$ATMONUS_DAILY_AA_SCORE = max(dt1$ATMONUS_DAILY_AA_SCORE,dt1$AA_SCOR)

# Sorting based in ATMONUS and AUTH_DTTM
dt1 <- dt1[order(dt1$ATMONUS,dt1$AUTH_DTTM_2), ]

# IF &CUMULATIVE_CONDITION. ELSE PART

#################################### MODIFIED to LE previous logic was LT #########################################
dt1$CUM_AMOUNT_ATMONUS_DAILY <- unlist(tapply(dt1$TRN_AMT, cumsum(c(0, diff(dt1$CUM_COUNT_ATMONUS_DAILY) > 1)), cumsum))

dt1$CUM_COUNT_MAX_ATMONUS_DAILY = max(dt1$CUM_COUNT_MAX_ATMONUS_DAILY, dt1$CUM_COUNT_APPR_ATMONUS_DAILY);
dt1$CUM_AMOUNT_MAX_ATMONUS_DAILY = max(dt1$CUM_AMOUNT_MAX_ATMONUS_DAILY, dt1$CUM_AMOUNT_APPR_ATMONUS_DAILY);
dt1$ATMONUS_DAILY_DAYS = dt1$ATMONUS_DAILY_DAYS + 1;
dt1$CUM_COUNT_ATMONUS_DAILY = 1;
dt1$CUM_AMOUNT_ATMONUS_DAILY = dt1$TRN_AMT;
dt1$CUM_COUNT_APPR_ATMONUS_DAILY = 0;
dt1$CUM_AMOUNT_APPR_ATMONUS_DAILY = 0;
dt55 <- subset(dt1 , TRN_TYP %in% c('C', 'M', 'P') & (AUTH_DECISION_XCD == 'A' | DECI_CD_ORIG == 'A') & TRN_AMT > 0)
# removing subset data frame
dt1 <- anti_join(dt1,dt55)
dt55$CUM_COUNT_APPR_ATMONUS_DAILY = 1 ;
dt55$CUM_AMOUNT_APPR_ATMONUS_DAILY = dt1$TRN_AMT;
# Appending the rows after logic is applied
dt1 <- rbind(dt1, dt55)

dt1$ATMONUS_DAILY_PREV = dt1$TIME_RESET_ATMONUS_DAILY;
dt1$TIME_RESET_ATMONUS_DAILY = dt1$AUTH_DTTM;
dt1$ATMONUS_DAILY_BASE_SCORE = dt1$FRD_SCOR;
dt1$ATMONUS_DAILY_AA_SCORE = dt1$AA_SCOR;

##################### SRVC_Set_CNP_HOURLY_AMT ##########################

# Getting the CUM_AMOUNT count variable
# dt1$SRVC_Set_CNP_HOURLY_AMT <- unlist(tapply(dt1$TRN_AMT, cumsum(c(0, diff(dt1$SRVC_Set_CNP_HOURLY_CNT) < 1)), cumsum))

# Storing data with cumulative Sum and Count
Country_Txn_Data_1 <- dt1

# Sorting before performing cumulative count and cumulative amount
Country_Txn_Data_1 <- Country_Txn_Data_1[order(Country_Txn_Data_1$ATMONUS,Country_Txn_Data_1$AUTH_DTTM_2),]

# Converting character to numeric
Country_Txn_Data_1$ATMONUS  <- as.numeric(as.character(Country_Txn_Data_1$ATMONUS))
Country_Txn_Data_1$CUM_COUNT_ATMONUS_DAILY  <- as.numeric(as.character(Country_Txn_Data_1$CUM_COUNT_ATMONUS_DAILY))
Country_Txn_Data_1$CUM_AMOUNT_ATMONUS_DAILY  <- as.numeric(as.character(Country_Txn_Data_1$CUM_AMOUNT_ATMONUS_DAILY))
Country_Txn_Data_1$CUM_COUNT_APPR_ATMONUS_DAILY  <- as.numeric(as.character(Country_Txn_Data_1$CUM_COUNT_APPR_ATMONUS_DAILY))
Country_Txn_Data_1$CUM_AMOUNT_APPR_ATMONUS_DAILY  <- as.numeric(as.character(Country_Txn_Data_1$CUM_AMOUNT_APPR_ATMONUS_DAILY))
Country_Txn_Data_1$ATMONUS_DAILY_BASE_SCORE  <- as.numeric(as.character(Country_Txn_Data_1$ATMONUS_DAILY_BASE_SCORE))
Country_Txn_Data_1$ATMONUS_DAILY_AA_SCORE  <- as.numeric(as.character(Country_Txn_Data_1$ATMONUS_DAILY_AA_SCORE))
Country_Txn_Data_1$CUM_COUNT_MAX_ATMONUS_DAILY  <- as.numeric(as.character(Country_Txn_Data_1$CUM_COUNT_MAX_ATMONUS_DAILY))
Country_Txn_Data_1$CUM_AMOUNT_MAX_ATMONUS_DAILY  <- as.numeric(as.character(Country_Txn_Data_1$CUM_AMOUNT_MAX_ATMONUS_DAILY))
Country_Txn_Data_1$SRVC_Set_CNP_HOURLY_CNT  <- as.numeric(as.character(Country_Txn_Data_1$SRVC_Set_CNP_HOURLY_CNT))
Country_Txn_Data_1$ATMONUS_DAILY_DAYS  <- as.numeric(as.character(Country_Txn_Data_1$ATMONUS_DAILY_DAYS))
Country_Txn_Data_1$ATMONUS_DAILY_PREV  <- as.numeric(as.character(Country_Txn_Data_1$ATMONUS_DAILY_PREV))
# Country_Txn_Data_1$TIME_RESET_ATMONUS_DAILY  <- as.numeric(as.character(Country_Txn_Data_1$TIME_RESET_ATMONUS_DAILY))
# Country_Txn_Data_1$ATMONUS_FIRST_TXN_DATE  <- as.numeric(as.character(Country_Txn_Data_1$ATMONUS_FIRST_TXN_DATE))
# Country_Txn_Data_1$ATMONUS_FIRST_APPROVAL_DATE  <- as.numeric(as.character(Country_Txn_Data_1$ATMONUS_FIRST_APPROVAL_DATE))
# Country_Txn_Data_1$AUTH_DTTM_2  <- as.numeric(as.character(Country_Txn_Data_1$AUTH_DTTM_2))

# Backup of data with Cumulative count and Cumulative Sum - including TRN_AMT and AUTH_DTTM calculations
Analysis_USD_AUTH_CUM_1 <- Country_Txn_Data_1

# Head of Country_Txn_Data_1
head(Country_Txn_Data_1)
