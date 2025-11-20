rm(list=ls())
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


DBlist<- c(list.files("H:/Falcon_data/",pattern = ".db",recursive = T,full.names = FALSE))


End_date <- format(as.Date(Sys.Date() -1), format = "%Y-%m-%d")


prev_2_m1 <- format(as.Date(Sys.Date() -90), format = "%Y-%m-01")

End_date

prev_2_m1 

month_seq = c(tolower(format(seq(as.Date(prev_2_m1), as.Date(End_date), by = "month"),"%b%Y")))

New_DBlist=c()
for (a in month_seq)
{
	month_list <- DBlist[c(grep(a,DBlist))]

	New_DBlist <- c(New_DBlist ,month_list)

}

	print(New_DBlist )

str(New_DBlist)


for (dbl in New_DBlist)
  
{
  
  chk1 <- match(dbl,New_DBlist)
  
 cntrylist <- c('GH','KE','QA','JE','BN','TZ','BW','NG','ZW','UG','ZM','GM','LK','NP','BH','VN','JO','BD','ID','TW','SL','CI','CM','JO')
 #cntrylist <- c('IN', 'AE', 'HK', 'MY', 'SG')


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
from falt002_",tolower(cty),"_",tolower(dbn)," where ACCT_NBR in 
(
"000445391740997",
"233889000156182",
"833534000"
)",collapse = "")

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

View(all_data)
export(all_data,"//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Saravana/Dispute Merchant Analysis/Data_1.xlsx") 
