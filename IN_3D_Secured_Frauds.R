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
rundt2 <- format(as.Date(End_date), format = "%d%b%Y")
date_num <- as.numeric(as.Date(End_date, origin = "1960-01-01"))

prev_2_m1 <- format(as.Date(Sys.Date() -60), format = "%Y-%m-01")

End_date

prev_2_m1 

month_seq = c(tolower(format(seq(as.Date(prev_2_m1), as.Date(End_date), by = "month"),"%b%Y")))





tablen1 <- read_sas("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Falcon 6.4 data download/FRD15/frd15.sas7bdat")

class(tablen1)

tablen1$date1 <- as.Date( as.character(tablen1$date), "%Y-%m-%d")

names(tablen1)

tablen1_last <- subset(tablen1, date == as.Date(End_date) & clientIdFromHeader %in% c("SC_CCMSIN_CR", "SC_EURONETIN_DB"))

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

	print(New_DBlist )

str(New_DBlist)


for (dbl in New_DBlist)
  
{
  
  chk1 <- match(dbl,New_DBlist)
  
 cntrylist <- c('IN')

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

distinct(all_data, fi_transaction_id, .keep_all = TRUE)

export(all_data,paste0("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/IN_3D_SECURED/India_3D_Secured_",rundt2,".xlsx",collapse=""))



library(mailR)

source("C:/FRSC/R_Codes_Prod/ZfileR.R")

compress7z("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/IN_3D_SECURED/",paste0("India_3D_Secured_",rundt2,".zip",collapse=""),paste0("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/IN_3D_SECURED/India_3D_Secured_",rundt2,".xlsx",collapse=""),"scb123")

file.remove(paste0("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/IN_3D_SECURED/India_3D_Secured_",rundt2,".xlsx",collapse=""))




send.mail(from="mis.frsc@sc.com",
          to = c("Abhishek.Bhatia1@sc.com","Dinesh.Charan@sc.com"),
          cc = c("Anishramkrishnan.Iyer@sc.com","Saravanakumar.K@sc.com","BonuVidhya.Sahiti@sc.com","JanakDinesh.Mundada@sc.com"),
	    subject=paste0("IN 3D Secured Frauds ",End_date,collapse=""),
          body="Hi All,<br> <br> Please find the attached IN CR and DB 3D secured frauds for yesterday.<br> <br> Thanks <br> FRSC_MIS",
          html=T,
          smtp=list(host.name = "inmail01apps.zone1.scb.net",
                    port = 25,
                    user.name = "mis.frsc@sc.com",
                    ssl = F),
          authenticate=F,
	    attach.files=(paste0("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/IN_3D_SECURED/India_3D_Secured_",rundt2,".zip",collapse=""))
)






