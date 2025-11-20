			#**************************************************************************************
			
										##### R Vs FALCON LOGIC SIMULATION ####
			
			#**************************************************************************************
											
											
# .libPaths("C:/FRSC/R_Packages1")
# rm(list=ls())
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
library(RDCOMClient)
library(XLConnect)
# Library for fetching Data
library(RSQLite)

# Fetching Data

# library(readxl)
# mydata <- read_excel("C:/Users/o.pss.1510806/Desktop/R_Sim/vba_test.xlsm")
mydata <- read_excel("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/vba_test_UDV.xlsm")
# Enclosing rule name in quotes and converting it to upper case for Rule data comparison
for (i in 1:nrow(mydata))
{
# Used to print original Rule name in output instead of UPPER case rule name
mydata$Original_Rule_Name[i] <- mydata[i,4]
mydata$Original_Tenant[i] <- mydata[i,5]
mydata$Original_Project[i] <- mydata[i,6]
# Converting to UPPER for rule comparison
mydata[i,4] <- toupper(mydata[i,4])
mydata[i,4] <- paste0("'", mydata[i,4], "'")
}

			#**************************************************************************************
			
												#### R Vs FALCON HITS ####
			
			#**************************************************************************************
			
#### Transaction Data ####

# Rule counter for the day
Rule_counter = 0

# Creating the sample output template from the file
output <- data.frame(matrix(nrow = 0, ncol = 10))

# Getting today's date to create forlder and write to shared path the output
Today <- Sys.Date()

# Declaring the header details for the output data frame
a <- "Rule Name"
b <- paste0("R~", (Sys.Date() - 1))
c <- paste0("R~", (Sys.Date() - 2))
d <- paste0("Falcon~", (Sys.Date() - 1))
e <- paste0("Falcon~", (Sys.Date() - 2))
f <- "Result"
g <- "Type"
h <- "Today"
i <- "Tenant"
j <- "Project"
colnames(output) <- c(a,b,c,d,e,f,g,h,i,j)
# Converting output to a dataframe
output <- data.frame(output)
# Printing output before including data
print(output)

			#**************************************************************************************
			
										 #### FALT002 - R HITS ####
			
			#**************************************************************************************
						
# convert datetimediff to numeric for SQL purposes:
Country_Txn_Data_1$datetimediff <- as.numeric(Country_Txn_Data_1$datetimediff, units="secs")
# Country_Txn_Data_1$datetimediff <- NULL

						
# Framing SQL syntax for extracting Txn data from "Country_Txn_Data_1"
for (i in 1 : nrow(mydata))
{

# Segregating Debit and Credit to fetch data
if(tolower(mydata$Project[i]) == 'credit'){
proj <- 'cr'
} else {
proj <- 'db'
}

# Segregating Type I and II to fetch data
if(tolower(mydata$Type[i]) == "i"){
type_dt <- format(as.Date(Sys.Date() -1), format = "%Y-%m-%d")
type_dt <- format(as.Date(type_dt), "%d-%m-%Y")
# type_dt <- dmy(type_dt)
} else {
type_dt <- format(as.Date(Sys.Date() -2), format = "%Y-%m-%d")
type_dt <- format(as.Date(type_dt), "%d-%m-%Y")
# type_dt <- dmy(type_dt)
}

# Subsetting the data dump Country_Txn_Data_1 for needed country and project data
# IMPORTANT - trn_dt1 in Country_Txn_Data_1 has a space in the end - ex: "08-Feb-21 "
Txn_Data_Fromat <- paste0("Select * from Country_Txn_Data_1 WHERE ", mydata[i,2], " and " , 
" UPPER(cnty) in ( ", mydata$Tenant[i] ," ) ", " and LOWER(Dbname) like '",proj,"%'", " and trn_dt1 >= '" , 
type_dt, " '")

# Final Transaction data for simulation
Txn_Data <- sqldf(Txn_Data_Fromat)
FALT002 <- Txn_Data

# Sorting and removing duplicates for FI_TRANSACTION_ID & filtering for the USR_DAT_1
FALT002 = sqldf('SELECT DISTINCT FI_TRANSACTION_ID, * FROM FALT002 ORDER BY FI_TRANSACTION_ID ASC;')
# To remove }
#}


			#**************************************************************************************
			
										#### FALT003 - RULE HITS ####

			#**************************************************************************************

# Subsetting the data dump Country_Rule_Data for needed country and project data
Rule_Data_Fromat <- paste0("Select * from Country_Rule_Data WHERE " , 
" UPPER(Rule_cnty) in ( ", mydata$Tenant[i] ," ) ", " and LOWER(Rule_Dbname) like '%",proj,"%'",
" and UPPER(RULE_NAME_STRG) = ", mydata[i,4]," and trn_dt1 >= '", type_dt, " '")

# Final Rule data for simulation
Rule_Data <- sqldf(Rule_Data_Fromat)

FALT003 <- Rule_Data

# Sorting and removing duplicates for FI_TRANSACTION_ID & filtering for the USR_DAT_1
FALT003 = sqldf('SELECT DISTINCT FI_TRANSACTION_ID, * FROM FALT003 ORDER BY FI_TRANSACTION_ID ASC;')
# To remove }
#}

			#**************************************************************************************
			
										   #### COUNT COMPARISON ####
			
			#**************************************************************************************

Rule_counter = Rule_counter + 1
# printing "Rule Name"
# USed to print original Rule name in output instead of UPPER case rule name
print(mydata$Original_Rule_Name[i])
print("Txn Data Count: ")
print (table(FALT002$CRD_CLNT_ID, FALT002$trn_dt1))
print("Rule Data Count: ")
print(table(FALT003$CLIENT_XID, FALT003$trn_dt1))
# Total number of Rules executed
print("Total number of Rules executed: ")
print (Rule_counter)

			#**************************************************************************************
			
										  #### OUTPUT FILE GENERATION ####
			
			#**************************************************************************************

# Writing to the output data frame

dt <- format(as.Date(Sys.Date() -1), format = "%Y-%m-%d")
dt <- format(as.Date(dt), "%d-%m-%Y")
# dt2 <- format(as.Date(Sys.Date() -2), format = "%Y-%m-%d")
# dt2 <- format(as.Date(dt2), "%d-%m-%Y")

output[i,1] = mydata$Original_Rule_Name[i]
FALT002_count1 = sqldf(paste0("Select count(DISTINCT FI_TRANSACTION_ID) from FALT002 WHERE ", "trn_dt1 >= '" , dt, " '"))
output[i,2] = FALT002_count1
FALT002_count2 = sqldf(paste0("Select count(DISTINCT FI_TRANSACTION_ID) from FALT002 WHERE ", "trn_dt1 <= '" , dt, " '"))
output[i,3] = FALT002_count2
FALT003_count1 = sqldf(paste0("Select count(DISTINCT FI_TRANSACTION_ID) from FALT003 WHERE ", "trn_dt1 >= '" , dt, " '"))
output[i,4] = FALT003_count1
FALT003_count2 = sqldf(paste0("Select count(DISTINCT FI_TRANSACTION_ID) from FALT003 WHERE ", "trn_dt1 <= '" , dt, " '"))
output[i,5] = FALT003_count2
output[i,7] = mydata[i,8]
output[i,8] = format(as.Date(Sys.Date()), format = "%Y-%m-%d")

if(sum(FALT002_count1 + FALT002_count2) == sum(FALT003_count1 + FALT003_count2))
{
output[i,6] = "Matched"
} else {
output[i,6] = "Unmatched"
}

output[i,9] = mydata$Original_Tenant[i]
output[i,10] = mydata$Original_Project[i]
}

# Exporting output results to local for reference
export(output,paste0("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/","R_Simulation_Output","_",Today,".xlsx",collapse=""),which="R_Simulation")

# Exporting Txn_Data results to local for reference
export(Txn_Data,paste0("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/","Txn_Data","_",Today,".xlsx",collapse=""),which="Txn_Data")

# Exporting Rule_Data results to local for reference
export(Rule_Data,paste0("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/","Rule_Data","_",Today,".xlsx",collapse=""),which="Rule_Data")