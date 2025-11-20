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
 # cntrylist <- c('IN', 'AE', 'HK', 'MY', 'SG')


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
from falt002_",tolower(cty),"_",tolower(dbn)," where mer_id in 
(
'210760000200925',
'XQP1R7DWK5QEPSU',
'160146000762203',
'ZPC7NTPEBH8GEKM',
'Z4HHC3IL3GUEJGN',
'JR0J9RFDL9YARER',
'103800100000001',
'418889000200232',
'E0NI1OWGPANZPJY',
'418114000762203',
'NKDHX4LXNXBSFSD',
'6MJFN7HR5Q4XILS',
'OQ1FD8GOF0NL3LS',
'372176392887',
'YEDUBD75VBOW2N8',
'OQDZJQVAODF27CF',
'HEPKRZSMEVRC0MQ',
'TK07LZNWT0CI0VH',
'000445120538993',
'7P2ZDMEPIJ7OPHO',
'9118335',
'526567000868690',
'*54204378100013',
'LZPKIKAYMBLOUSQ',
'420429000208822',
'EUXC7LZIW2JPLCV',
'YK8KRPDGBWCHYND',
'4666C2CEQEN1IDC',
'46267512',
'0JAOPIA1EDSE4FP',
'013518749',
'Z8AVY57KPUIN1Q5',
'268128000209351',
'088011245821',
'QPMA6P4HFIWYGFN',
'SDEGYZILU1KB7UD',
'7017969',
'4556716632',
'248750000103177',
'4556728604',
'YNWFXKRUPAGKYUR',
'OXTUNKEKCK0U9P8',
'112127000108778',
'001170827000',
'32937255',
'UFLWE2216RINYGE',
'60TLHEG0D7YBR3B',
'HYRXVP2OURAIFR2',
'SGJHYVKU3MWULRB',
'088007968800',
'27970014',
'RYHB5IHLRORAQYH',
'G7OTELJTGC6SBWZ',
'WKF0RR2LR8GIYQH',
'420429000211321',
'A0000000000HI55',
'000027007704570',
'340201083624000',
'145863438',
'001918500661',
'298800300581873',
'181930000067204',
'2516340000',
'000445191569992',
'057181000156182',
'477715000203793',
'000001190700203',
'15877800600',
'027007705023',
'048800040008013',
'000311021521886',
'185119000762203',
'000980200425992',
'210758000200925',
'000102920051742',
'186516000067625',
'498750000022153',
'230120000200915',
'97515971',
'000001650044950',
'001111013264',
'001111651725',
'409986000201250',
'020395751',
'420429000211313',
'ACUMQGUWSXVY0OZ',
'001118969732',
'QZNHSMUYHCJS15U',
'000005444196000',
'859884000',
'212963000200925',
'464400000000854',
'001110253143',
'001117915496',
'480464000',
'000496219885886',
'000008010720',
'242865',
'J.W  MARRIOTT',
'200600027015',
'000532001',
'4556707188'
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

# View(all_data)
export(all_data,"//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Saravana/Dispute Merchant Analysis/mernm.xlsx") 
