			#**************************************************************************************
			
							        	#### FALT003 - Country Rule Data ####
			
			#**************************************************************************************

# Country Rule Data

.libPaths("C:/FRSC/R_Packages1")
.libPaths()
setwd("H:/Rule_Hit/")

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

DBlist<- c(list.files("H:/Rule_Hit/",pattern = ".db",recursive = T,full.names = FALSE))

End_date <- format(as.Date(Sys.Date() -1), format = "%Y-%m-%d")
End_date

prev_2_m1 <- format(as.Date(Sys.Date() -2), format = "%Y-%m-01")
prev_2_m1 

month_seq = c(tolower(format(seq(as.Date(prev_2_m1), as.Date(End_date), by = "month"),"%b%Y")))

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

  for (cty in cntrylist)
  {
    
    print(Sys.time())
    z<-strsplit(dbl, "/")
    dbn <-  substr(strsplit(dbl, "/")[[1]][2],9,18)
    
    tablen <- paste0("falt003_",tolower(cty),"_",tolower(dbn),collapse = "")
    
	# Rule Data - Printing the tables for which data will be extracted for FALT003 from the DB:
	print(paste("Data extracted for the FALT003 table:", tablen))
	
	squery <- paste0("select '",tolower(cty),"' as cnty,'",tolower(dbn), "' as Dbname,substr(CREATED_DTTM,1,10) as trn_dt1, 
  SCORE_CUSTOMER_ACCOUNT_XID,
  RULE_NAME_STRG,
  CREATED_DTTM,
  FI_TRANSACTION_ID,
  RULE_BASE_STRG,
  RULE_SET_STRG,
  CLIENT_XID,
  Filename,
  date1
  from falt003_",tolower(cty),"_",tolower(dbn),
  collapse = "")
	
    print(paste0(squery))
    
    mydb <- dbConnect(RSQLite::SQLite(),synchronous = NULL, paste0("H:/Rule_Hit/",dbl,collapse=""))
    
    if (dbExistsTable(mydb,tablen) == TRUE)
      {
        
            a <- dbGetQuery(mydb, squery)
            dbListTables(mydb)
            chk <- match(cty,cntrylist)
            
            
            if (chk == 1 & chk1 ==1) 
			{ 
      	        all_data_rule <- subset(a, FALSE)
		      }
            
            all_data_rule <- rbind(all_data_rule,a)
      }
        print(Sys.time())
      
     dbDisconnect(mydb)
    }
  
}

# View(all_data_rule)
Country_Rule_Data <- all_data_rule

# Feature engineering to compare county and project - For Rule hit count
Country_Rule_Data$Rule_cnty = ""
Country_Rule_Data$Rule_Dbname = ""

for (i in 1 : nrow(Country_Rule_Data))
{
a = nchar(strsplit(Country_Rule_Data$CLIENT_XID[i], "_")[[1]][2])-1
b = nchar(strsplit(Country_Rule_Data$CLIENT_XID[i], "_")[[1]][2])
Country_Rule_Data$Rule_cnty[i] <- substr(strsplit(Country_Rule_Data$CLIENT_XID[i], "_")[[1]][2],a,b)
Country_Rule_Data$Rule_Dbname[i] <- substr(strsplit(Country_Rule_Data$CLIENT_XID[i], "_")[[1]][3],1,2)
}

# View(all_data_rule)
# Unique Txn dates present in FALT003 date
unique(Country_Rule_Data$trn_dt1)

		