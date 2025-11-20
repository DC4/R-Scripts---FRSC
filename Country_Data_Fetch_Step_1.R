
			#**************************************************************************************
			
											#### R SIMULATION ####
			
			#**************************************************************************************

			#**************************************************************************************
			
								#### FALT002 - Country Transaction Data ####
			
			#**************************************************************************************

# Country Transaction Data

rm(list=ls())
rm()
.libPaths("C:/FRSC/R_Packages1")
.libPaths()
ls()
setwd("H:/Falcon_data/")

#define libraries required
library("dataframes2xls")
library(readr)
library(haven)
library(stringr)
library(data.table)
library(sqldf)
library(DBI)
library(dplyr)	
library(RCurl)
library(rio)
library(lubridate)

# Accessing the tracker and contents for the simulation
library(readxl)
mydata <- read_excel("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/vba_test_UDV.xlsm")
# mydata <- read_excel("C:/Users/o.pss.1510806/Desktop/R_Sim/vba_test.xlsm")

# Converting "mydata" to dataframe
mydata <- data.frame(mydata)

# Fetching countries needed from SAS tracker
cntrylist = c()
for (c in 1:nrow(mydata))
{
cntry_needed <- mydata$Tenant[c]
cntry_split <- strsplit(cntry_needed, ",")
for (j in 1:lengths(cntry_split))
{
#print (cntry_split[[1]][j])
individual_cntry <- (cntry_split[[1]][j])
cntrylist <- c(cntrylist, individual_cntry)
}
}
# Country list for entire list of SAS simulation for the day 	
# Removing single quotes and spaces from elements in the list
cntrylist <- gsub("'", '', cntrylist)
cntrylist <- gsub(" ", "", cntrylist)
# Fetching only unique countries from the entire list - to prevent multiple data fetches (both txn and Rule)
cntrylist <- unique(cntrylist)
print(cntrylist)

			#**************************************************************************************
			
							        	#### FALT002 - Country Txn Data ####
			
			#**************************************************************************************

# Obtianing FALT002 data
DBlist<- c(list.files("H:/Falcon_data/",pattern = ".db",recursive = T,full.names = FALSE))

End_date <- format(as.Date(Sys.Date() -1), format = "%Y-%m-%d")
End_date

prev_2_m1 <- format(as.Date(Sys.Date() -2), format = "%Y-%m-01")
prev_2_m1

month_seq = c(tolower(format(seq(as.Date(prev_2_m1), as.Date(End_date), by = "month"),"%b%Y")))

New_DBlist=c()
for (a in month_seq)
{
	month_list <- DBlist[c(grep(a,DBlist))]
	print(month_list)
	New_DBlist <- c(New_DBlist ,month_list)
	print(New_DBlist)
}

print(New_DBlist)

str(New_DBlist)

for (dbl in New_DBlist)
  
{
  
  chk1 <- match(dbl,New_DBlist)
  print(dbl)
  print(chk1)
 # This is obtianed initially before in the code
 # cntrylist <- c('GH','KE','QA','JE','BN','TZ','BW','NG','ZW','UG','ZM','GM','LK','NP','BH','VN','JO','BD','ID','TW','SL','CI','CM','JO')
 # cntrylist <- c('ZM','TW')
  
  for (cty in cntrylist)
  {
    print(Sys.time())
    z <-strsplit(dbl, "/")
    dbn <-  substr(strsplit(dbl, "/")[[1]][2],1,10)
    tablen <- paste0("falt002_",tolower(cty),"_",tolower(dbn),collapse = "")
    # Txn Data - Printing the tables for which data will be extracted for FALT002 from the DB:
	print(paste("Data extracted for the FALT002 table:", tablen))
	
    squery <- paste0("select '",tolower(cty),"' as cnty,'",tolower(dbn), "' as Dbname,
substr(trn_dt,1,10) as trn_dt1,
ACCT_NBR,
TRN_DT,
TRN_TYP,
TRN_AUTH_POST,
DECI_CD,
DECI_CD_ORIG,
TRN_POS_ENT_CD,
TRN_POST_DT,
CRD_CLNT_ID,
SIC_CD,
MER_ID,
MER_NM,
mer_cty,
MER_CNTY_CD,
USR_IND_2,
USR_IND_4,
MAST_ACCT_NBR,
CVV2_PRESENT,
CVV2_RESPONSE,
ACQUIRER_ID,
ACQUIRER_CNTRY,
TERMINAL_ID,
TERMINAL_TYPE,
TERMINAL_ENTRY_CAP,
ACQUIRER_MERCH_ID,
TRN_AMT,
FRD_SCOR,
AA_SCOR,
FI_TRANSACTION_ID,
TRANSACTION_ADVICE_XCD,
AUTHORIZATION_XID,
TRANSACTION_PIN_VERIFY_XCD,
CVV_VERIFY_XCD,
AUTH_DECISION_XCD,
FRD_IND,
USER_DATA_4_STRG,
USR_DAT_2,
USR_DAT_1,
Filename,
date1
from falt002_",tolower(cty),"_",tolower(dbn),
collapse = "")

    print(paste0(squery))
    
    mydb <- dbConnect(RSQLite::SQLite(),synchronous = NULL, paste0("H:/Falcon_data/",dbl,collapse=""))
    
    if (dbExistsTable(mydb,tablen) == TRUE)
      {
        
            a <- dbGetQuery(mydb, squery)
            dbListTables(mydb)
            chk <- match(cty,cntrylist)
            
            
            if (chk == 1 & chk1 ==1) 
			{ 
      	        all_data <- subset(a, FALSE)
		      }
            
            all_data <- rbind(all_data,a)
      }
        print(Sys.time())
      
     dbDisconnect(mydb)
    }
  
}

# View(all_data)
Country_Txn_Data <- all_data
# Head of Country_Txn_Data
head(Country_Txn_Data)
# Tail of Country_Txn_Data
tail(Country_Txn_Data)
# Unique Txn dates present in FALT002 date
unique(Country_Txn_Data$trn_dt1)
# export(Country_Txn_Data,"//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Dinesh/Country_Txn_Data.xlsx")

			#**************************************************************************************
			
										  #### AUTH_DTTM CREATION ####
			
			#**************************************************************************************

			
# Creating the TRAN_DATE and AUTH_DTTM Attributes
Country_Txn_Data$Date <- all_data$TRN_DT
Country_Txn_Data$TRAN_DATE = ""
Country_Txn_Data$AUTH_DTTM = ""
Country_Txn_Data$TRN_DT <- gsub(" ", '-', Country_Txn_Data$TRN_DT)
Country_Txn_Data$TRN_DT <- gsub("\\.", '-', Country_Txn_Data$TRN_DT)

Month_df <- data.frame(
months = c("Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"), 
months_numeric = c('01','02','03','04','05','06','07','08','09','10','11','12'))

DT1 <- data.frame(str_split_fixed(Country_Txn_Data$TRN_DT, "-", 8))
Country_Txn_Data$TRAN_DATE = paste0(DT1[,1],'.',Month_df[match(DT1[,2], Month_df$months), 2],'.','20',DT1[,3])
Country_Txn_Data$AUTH_DTTM = paste0(Country_Txn_Data$TRAN_DATE,' ',DT1[,4],':',DT1[,5],':',DT1[,6])
#Date format - 1
# Country_Txn_Data$AUTH_DTTM_1 <- as.POSIXct(Country_Txn_Data$AUTH_DTTM, format = "%d.%m.%Y %H:%M:%S", tz = "GMT")
Country_Txn_Data$AUTH_DTTM_1 <- as.POSIXct(Country_Txn_Data$AUTH_DTTM, format = "%d.%m.%Y %H:%M:%S")
#Date format - 2
# Country_Txn_Data$AUTH_DTTM <- strptime(Country_Txn_Data$AUTH_DTTM, "%d.%m.%Y %H:%M:%OS")
# Creating the column Country_Txn_Data$AUTH_DTTM_2 for order by and sorting for Cumsum and Cumamt
Country_Txn_Data$AUTH_DTTM_2 <- Country_Txn_Data$AUTH_DTTM_1


			#**************************************************************************************
			
									       #### SRVC_UDV ####
			
			#**************************************************************************************
																	
# convert <- data.frame(country = c("USD", "euro", "HKD"), fact= c(1.4, 1, 8))
# Value <- convert[match("USD", convert$country), 2]

# Debit - FX conversion

convert_db <- data.frame(
country = c("SC_EURONETAE_DB", "SC_EURONETMY_DB", "SC_EURONETID_DB", "SC_EURONETIN_DB", "SC_TANDEMTW_DB", 
"SC_EURONETBH_DB", "SC_SPARROWBW_DB","SC_SPARROWGH_DB","SC_SPARROWJO_DB","SC_SPARROWKE_DB","SC_SPARROWLK_DB",
"SC_SPARROWNG_DB","SC_SPARROWNP_DB","SC_EURONETVN_DB","SC_SPARROWZM_DB","SC_EURONETBN_DB","SC_EURONETSG_DB",
"SC_SPARROWBW_DB","SC_SPARROWGM_DB","SC_EURONETIN_DB","SC_EURONETQA_DB","SC_SPARROWTZ_DB","SC_SPARROWUG_DB",
"SC_SPARROWZW_DB","SC_HOGANHK_DB","SC_SPARROWBD_DB","SC_SPARROWCI_DB","SC_SPARROWSL_DB","SC_SPARROWCM_DB"), Fx = c(3.67,4.221743,13500,65,30.116853,0.377132,10.2587,4.42718,0.708893,103.824,153.05,
365.467,102.69, 22700,8.98193,1.36370,1.36370,10.3382,45.8258,63.75,3.64146,2240.11,3603.55,361.900,7.8,
83.60,562.55,8300,570
))

# Credit - FX conversion

convert_cr <- data.frame(
country = c("SC_CCMSSG_CR","SC_CCMSBN_CR","SC_CCMSMY_CR","SC_CCMSPH_CR","SC_CCMSTH_CR","SC_CCMSID_CR","SC_CCMSIN_CR",
"SC_CCMSTW_CR","SC_C400AE_CR","SC_C400BH_CR","SC_C400BW_CR","SC_C400GH_CR","SC_C400JO_CR","SC_C400JE_CR",
"SC_C400KE_CR","SC_C400LK_CR","SC_C400NG_CR","SC_C400NP_CR","SC_C400VN_CR","SC_C400ZM_CR","SC_CCMSHK_CR",
"SC_C400BD_CR"), Fx = c(1.255698,1.255698,3.221743,43.88082,32.675467,11627.906977,63.75,30.116853,3.67,0.377132,
10.2587,4.42718,0.708893,0.75,103.824,153.05,365.467,102.69,22727.50,8.98193,7.8,83.60
))

# Including USD Variable in "Country_Txn_Data"
# Fetch unique CRD_CLT_IDs in the Country_Txn_Data
uniq_crd_clnt_id_txn_data <- unique(Country_Txn_Data$CRD_CLNT_ID)
print("Unique CRD_CLNT_IDs present in the Country_Txn_Data data:")
print(uniq_crd_clnt_id_txn_data)
# Declare a dataframe for rbind to add the subsetted data
sub_merge = data.frame()

# Subsetting the dataframe for speed of processing
for (aa in uniq_crd_clnt_id_txn_data)
{
print("Subsetting for the below:")
print(aa)
sub <- subset(Country_Txn_Data, CRD_CLNT_ID == aa)
# Segregating Debit and Credit to fetch data for the Fx rate
{
if(tolower(str_sub(sub$CRD_CLNT_ID,-2,-1)) == 'cr'){
# Obtaining the "Fx" Value for each row
sub["Fx"] = convert_cr[match(sub$CRD_CLNT_ID, convert_cr$country), 2]
} else if(tolower(str_sub(sub$CRD_CLNT_ID,-2,-1)) == 'db') {
# Obtaining the "Fx" Value for each row
sub["Fx"] = convert_db[match(sub$CRD_CLNT_ID, convert_db$country), 2]
} else {
next
}
}
# Final data with Fx rate values
sub_merge <- rbind(sub_merge, sub)
}
# Calculating the USD column
sub_merge$USD = round((sub_merge$TRN_AMT/sub_merge$Fx), digits = 2) 
# Assigning the data frame for extracting data later using sql
Country_Txn_Data <- sub_merge
# Exporting the final dataset for analysis
# export(Country_Txn_Data,paste0("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/","Country_Txn_Data_1",".xlsx",collapse=""),which="Country_Txn_Data_1")
