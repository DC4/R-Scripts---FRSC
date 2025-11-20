##########################################################  FRD 15 Stats ###########################################################
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
library(mailR)
library(xml2)
library(data.table)
library(sqldf)
library(DBI)
library(dplyr)
library(RCurl)
library(rio)
library(xtable)
library(openxlsx)
library(readxl)

#define libraries required
library("dataframes2xls")


cntrylist = c('AE', 'BH', 'BN', 'BW', 'GH', 'ID', 'HK', 'DR', 'IN', 'JE', 'JO', 'KE', 'LK', 'MY', 'NG', 'NP', 'SG', 'VN', 'ZM', 'AE', 'BH', 'BN', 'BW', 'CI', 'CM', 'GH', 'GM', 'IN', 'JO', 'KE', 'LK', 'NG', 'NP', 'QA', 'SG', 'SL', 'TZ', 'UG', 'VN', 'ZM', 'ZW')

# cntrylist = c('IN', 'BD')

cntrylist <- gsub("'", '', cntrylist)
cntrylist <- gsub(" ", "", cntrylist)
# Fetching only unique countries from the entire list - to prevent multiple data fetches (both txn and Rule)
cntrylist <- unique(cntrylist)
print(cntrylist)

DBlist<- c(list.files("H:/Falcon_data/",pattern = ".db",recursive = T,full.names = FALSE))


End_date <- format(as.Date(Sys.Date() -1), format = "%Y-%m-%d")
rundt2 <- format(as.Date(End_date), format = "%d%b%Y")
date_num <- as.numeric(as.Date(End_date, origin = "1960-01-01"))

# prev_2_m1 <- format(as.Date(Sys.Date() -7), format = "%Y-%m-01")
prev_2_m1 <- format(as.Date(Sys.Date() -8), format = "%Y-%m-%d")

End_date

prev_2_m1 

month_seq = c(tolower(format(seq(as.Date(prev_2_m1), as.Date(End_date), by = "month"),"%b%Y")))


# tablen1 <- read_sas("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Falcon 6.4 data download/FRD15/frd15.sas7bdat")

tablen1 = read_sas("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Falcon 6.4 data download/FRD15/frd15.sas7bdat")

class(tablen1)

tablen1$date1 <- as.Date( as.character(tablen1$date), "%Y-%m-%d")

names(tablen1)

# tablen1_last <- subset(tablen1, date == as.Date(End_date) & clientIdFromHeader %in% c("SC_CCMSIN_CR", "SC_EURONETIN_DB"))

tablen1_last <- subset(tablen1, date == as.Date(End_date))

# View(tablen1_last)

new = tablen1_last$fiTransactionIdReference


fi = ""
for (j in new)

{
if (grep(j,new) == 1) 
{
list <- new[c(grep(j,new))]
fi <- paste0(fi, "'",str_trim(list, side = c("both", "left", "right")),"'",collapse = "")
}
else 
{
list <- new[c(grep(j,new))]
fi <- paste0(fi, ", '",str_trim(list, side = c("both", "left", "right")),"'",collapse = "")
}
}





New_DBlist=c()
for (a in month_seq)
{
	month_list <- DBlist[c(grep(a,DBlist))]

	New_DBlist <- c(New_DBlist ,month_list)

}

	print(New_DBlist)

str(New_DBlist)


for (dbl in New_DBlist)
  
{
  
  chk1 <- match(dbl,New_DBlist)
  
  # cntrylist <- c('IN')

  for ( cty in cntrylist)
  {
    
    print(Sys.time())
    z<-strsplit(dbl, "/")
    dbn <-  substr(strsplit(dbl, "/")[[1]][2],1,10)
    
    tablen <- paste0("falt002_",tolower(cty),"_",tolower(dbn),collapse = "")
    
    squery <- paste0("select '",tolower(cty),"' as cnty,'",tolower(dbn), "' as Dbname,substr(trn_dt,1,10) as trn_dt1, 
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
Filename,
date1
from falt002_",tolower(cty),"_",tolower(dbn)," where substr(usr_ind_4,1,2) = 'Y2' and (", 
sprintf("fi_transaction_id in (%s)", fi ),
" or (",
sprintf("frd_ind in ('Y') and date1 = %i", date_num), "))"
,collapse = "")

    print(paste0(squery))
    
    mydb <- dbConnect(RSQLite::SQLite(),synchronous = NULL, paste0("H:/Falcon_data/",dbl,collapse=""))
    
    if (dbExistsTable(mydb,tablen) == TRUE)
      {
        
            a <- dbGetQuery(mydb, squery)
            dbListTables(mydb)
            chk <- match(cty,cntrylist )
            
            
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

all_data <- distinct(all_data, FI_TRANSACTION_ID, .keep_all = TRUE)

# export(all_data,paste0("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/IN_3D_SECURED/India_3D_Secured_",rundt2,".xlsx",collapse=""))

Country_Txn_Data <- all_data


# Joining FRD15 and Txn_Data
# Renaming Columns in FRD15
FRD_15 <- data.frame(tablen1)
names(FRD_15)[names(FRD_15) == 'fiTransactionIdReference'] <- 'FI_TRANSACTION_ID'
names(FRD_15)[names(FRD_15) == 'transactionAmount'] <- 'TRN_AMT'
FRD_15$TRN_AMT <- as.numeric(FRD_15$TRN_AMT)
FRD_15_New <- FRD_15 %>%
  select(FI_TRANSACTION_ID, TRN_AMT)

Needed_Data <- left_join(FRD_15_New, Country_Txn_Data, by = "FI_TRANSACTION_ID")

Country_Txn_Data <- Needed_Data

# Country_Txn_final <- left_join(Country_Txn_Data, tablen1,  by = "FI_TRANSACTION_ID")
# jaja <- merge(x = Country_Txn_Data, y = tablen1[ , c("transactionAmount")], by = c("FI_TRANSACTION_ID", "fiTransactionIdReference", all.x=TRUE))
# jaja <- merge(Country_Txn_Data, tablen1, by=c("FI_TRANSACTION_ID","fiTransactionIdReference"))


			#**************************************************************************************
			
									       #### SRVC_UDV ####
			
			#**************************************************************************************
																	
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
"SC_C400BD_CR", "SC_PMTHK_CR"), Fx = c(1.255698,1.255698,3.221743,43.88082,32.675467,11627.906977,63.75,30.116853,3.67,0.377132,
10.2587,4.42718,0.708893,0.75,103.824,153.05,365.467,102.69,22727.50,8.98193,7.8,83.60, 7.8
))


# BD SRVC_UDV_TRN_AMT_Local
test <- subset(Country_Txn_Data, CRD_CLNT_ID == "SC_SPARROWBD_DB" & substr(ACCT_NBR,1,6) == ("411144") & substr(ACCT_NBR,1,6) == ("421451") & substr(ACCT_NBR,1,6) == ("469626") & substr(ACCT_NBR,1,6) == ("470691"))
dim(Country_Txn_Data)
Country_Txn_Data_mod <- anti_join(Country_Txn_Data, test, by = "FI_TRANSACTION_ID")
dim(Country_Txn_Data_mod)
test$TRN_AMT = round((test$TRN_AMT * 83.60), digits = 2)
# merge two data frames by ID
Country_Txn_Data <- rbind(Country_Txn_Data_mod, test)
dim(Country_Txn_Data)

# Including USD Variable in "Country_Txn_Data"
# Fetch unique CRD_CLT_IDs in the Country_Txn_Data
uniq_crd_clnt_id_txn_data <- unique(Country_Txn_Data$CRD_CLNT_ID)
print("Unique CRD_CLNT_IDs present in the Country_Txn_Data data:")
print(uniq_crd_clnt_id_txn_data)
# Declare a dataframe for rbind to add the subsetted data
sub_merge = data.frame()
# Remove #NA
uniq_crd_clnt_id_txn_data <- uniq_crd_clnt_id_txn_data[!is.na(uniq_crd_clnt_id_txn_data)]

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
} else if (tolower(str_sub(sub$CRD_CLNT_ID,-2,-1)) == 'db') {
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
sub_merge$USD = round((sub_merge$TRN_AMT.x/sub_merge$Fx), digits = 2)

# Assigning the data frame for extracting data later using sql
Country_Txn_Data <- sub_merge

# Unique Txn dates present in FALT002 date
unique(Country_Txn_Data$trn_dt1)

# Identify if data dataframe is empty or not
if(nrow(Country_Txn_Data) == 0)
{
df8 <- paste0("Dear All,<br>", "<br>No Fraud trend identified for today.<br>", 
"<br>Thanks & Regards","<br>FRSC_MIS")

# Mailing to team

dt <- Sys.Date()
send.mail(from="mis.frsc@sc.com",
	   #to=c("Dinesh.Charan@sc.com"),
	   #cc=c("Dinesh.Charan@sc.com"),
           to=c("Subramanian.Paramasivam@sc.com","Prakash.C@sc.com","Parthasarathi.KP@sc.com","Manjunath.Km@sc.com","HarishB.Darshankar@sc.com","Srinadhareddy.Tatiparthi@sc.com","Abhishek.Bhatia1@sc.com","JanakDinesh.Mundada@sc.com","BonuVidhya.Sahiti@sc.com","Dinesh.Charan@sc.com"),
           cc = c("Saravanakumar.K@sc.com","Anishramkrishnan.Iyer@sc.com","Ajanta.Dhilipkumarbehera@sc.com","JohnPaulRaja.A@sc.com"),
		  subject=paste0(" UAT (Testing) - FRD15 Trend for Past 7 Days as of - ",dt),
          body=df8,
          html=TRUE,
          smtp=list(host.name = "inmail01apps.zone1.scb.net",
                    port = 25,
                    user.name = "mis.frsc@sc.com",
                    ssl = F),
          authenticate=F,
          send = TRUE
)
}else
{

# Aggregating the needed data
Final_Data <- Country_Txn_Data %>%  select(CRD_CLNT_ID, USD)
Plot_Data <- sqldf("Select CRD_CLNT_ID, SUM(USD) as TOTAL_USD from Final_Data GROUP BY CRD_CLNT_ID")

# html format conversion
df7 <- print(xtable(Plot_Data, align="ccc"), # align columns
   type = "html", include.rownames = F, print.results=FALSE)
   
df8 <- paste0("Dear All,<br>", "<br>Below is the FRD15 data based USD trend for the past 7 days<br><br>", "<html>", df7, "</html>", 
"<br>Thanks & Regards","<br>FRSC_MIS")

# Mailing to team

FRD_Txns <- Plot_Data
rundt2 <- format(as.Date(Sys.Date()), format = "%d%b%Y")
export(Country_Txn_Data,paste0("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Cashops/Output/FRD_Txns",rundt2,".xlsx",collapse=""))
library(mailR)
source("C:/FRSC/R_Codes_Prod/ZfileR.R")
compress7z("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Cashops/Output/",paste0("FRD_Txns_",rundt2,".zip",collapse=""),paste0("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Cashops/Output/FRD_Txns",rundt2,".xlsx",collapse=""),"scb123")

dt <- Sys.Date()
send.mail(from="mis.frsc@sc.com",
	   #to=c("Dinesh.Charan@sc.com"),
	   #cc=c("Dinesh.Charan@sc.com"),
           to=c("Subramanian.Paramasivam@sc.com","Prakash.C@sc.com","Parthasarathi.KP@sc.com","Manjunath.Km@sc.com","HarishB.Darshankar@sc.com","Srinadhareddy.Tatiparthi@sc.com","Abhishek.Bhatia1@sc.com","JanakDinesh.Mundada@sc.com","BonuVidhya.Sahiti@sc.com","Dinesh.Charan@sc.com"),
           cc = c("Saravanakumar.K@sc.com","Anishramkrishnan.Iyer@sc.com","Ajanta.Dhilipkumarbehera@sc.com","JohnPaulRaja.A@sc.com"),
		  subject=paste0(" UAT (Testing) - FRD15 Trend for Past 7 Days as of - ",dt),
          body=df8,
          html=TRUE,
          smtp=list(host.name = "inmail01apps.zone1.scb.net",
                    port = 25,
                    user.name = "mis.frsc@sc.com",
                    ssl = F),
          authenticate=F,
		  attach.files=(paste0("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Cashops/Output/FRD_Txns_",rundt2,".zip",collapse="")),
          send = TRUE
)
}
