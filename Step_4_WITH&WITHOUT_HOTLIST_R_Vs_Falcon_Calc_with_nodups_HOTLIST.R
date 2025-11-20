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
library(xlsx)
# Library for fetching Data
library(RSQLite)

# Fetching Data

# Replace the numeric columns with value NA to 0
Country_Txn_Data_1 <- mutate_if(Country_Txn_Data_1, is.numeric, ~replace(., is.na(.), 0))
Country_Txn_Data_1 <- data.table(Country_Txn_Data_1)

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
a <- "Rule_Name"
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

# Backup for future use
Country_Txn_Data_1_bckup <- Country_Txn_Data_1
# Converting to data table for faster processing
Country_Txn_Data_1 <- data.table(Country_Txn_Data_1)

# Backup for future use
Country_Rule_Data_bckup <- Country_Rule_Data
# Converting to data table for faster processing
Country_Rule_Data <- data.table(Country_Rule_Data)


# Function to write data frame to excel - This is to write the column names of FALT002 & FALT003 in case it is empty
write.xlsx.custom <- function(x, file, sheetName="Sheet1",
                       col.names=TRUE, row.names=TRUE, append=FALSE, showNA=TRUE)
{
    if (!is.data.frame(x))
        x <- data.frame(x)    # just because the error message is too ugly

    iOffset <- jOffset <- 0
    if (col.names)
        iOffset <- 1
    if (row.names)
        jOffset <- 1

    if (append && file.exists(file)){
        wb <- loadWorkbook(file)
    } else {
        ext <- gsub(".*\\.(.*)$", "\\1", basename(file))
        wb  <- createWorkbook(type=ext)
    }  
    sheet <- createSheet(wb, sheetName)

    noRows <- nrow(x) + iOffset
    noCols <- ncol(x) + jOffset
    if (col.names){
        rows  <- createRow(sheet, 1)                  # create top row
        cells <- createCell(rows, colIndex=1:noCols)  # create cells
        mapply(setCellValue, cells[1,(1+jOffset):noCols], colnames(x))
    }
    if (row.names)             # add rownames to data x                   
        x <- cbind(rownames=rownames(x), x)

    if(nrow(x) > 0) {
        colIndex <- seq_len(ncol(x))
        rowIndex <- seq_len(nrow(x)) + iOffset

        .write_block(wb, sheet, x, rowIndex, colIndex, showNA)
    }
    saveWorkbook(wb, file)

    invisible()
}


	#**************************************************************************************
			
										 #### Load HOTLISTS ####
			
    #**************************************************************************************

# REGULATORY_MID <- # read_excel("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/Hotlist/REGULATORY_MID.xls")


# Framing SQL syntax for extracting Txn data from "Country_Txn_Data_1"
for (i in 1 : nrow(mydata))
{

# Segregating Debit and Credit to fetch data
if(tolower(mydata$Project[i]) == 'credit'){
proj <- 'cr'
} else {
proj <- 'db'
}

print(proj)

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

print(type_dt)

# Subsetting the data dump Country_Txn_Data_1 for needed country and project data
# IMPORTANT - trn_dt1 in Country_Txn_Data_1 has a space in the end - ex: "08-Feb-21 "
# Txn_Data_Fromat <- paste0("Select * from Country_Txn_Data_1 WHERE ", mydata[i,2], " and " , 
# " UPPER(cnty) in ( ", mydata$Tenant[i] ," ) ", " and LOWER(Dbname) like '",proj,"%'", " and trn_dt1 >= '" , type_dt, " '")

print("Rule Name: ")
print(mydata[i,4])

RULE_NAME_CR = paste0(toupper(mydata[i,4])," _CR ")
RULE_NAME_DR = paste0(toupper(mydata[i,4])," _DR ")

sub_cr <- subset(Country_Txn_Data_1, toupper(Hotlist_Rule_Name) == paste0(toupper(mydata[i,4])," _CR "))
sub_dr <- subset(Country_Txn_Data_1, toupper(Hotlist_Rule_Name) == paste0(toupper(mydata[i,4])," _DR "))


if((tolower(mydata$Project[i]) == 'credit') && dim(sub_cr)[1] != 0){
Txn_Data_Fromat <- paste0("Select * from Country_Txn_Data_1 WHERE ", mydata[i,2], " and " , 
" UPPER(cnty) in ( ", mydata$Tenant[i] ," ) ", " and LOWER(Dbname) like '",proj,"%'",
" and UPPER(Hotlist_Rule_Name) = ", mydata[i,4], " _CR " ," and trn_dt1 >= '" , type_dt, " '")
} else if((tolower(mydata$Project[i]) == 'debit') && dim(sub_dr)[1] != 0) {
Txn_Data_Fromat <- paste0("Select * from Country_Txn_Data_1 WHERE ", mydata[i,2], " and " , 
" UPPER(cnty) in ( ", mydata$Tenant[i] ," ) ", " and LOWER(Dbname) like '",proj,"%'",
" and UPPER(Hotlist_Rule_Name) = ", mydata[i,4], " _DR " ," and trn_dt1 >= '" , type_dt, " '")
} else {
Txn_Data_Fromat <- paste0("Select * from Country_Txn_Data_1 WHERE ", mydata[i,2], " and " , 
" UPPER(cnty) in ( ", mydata$Tenant[i] ," ) ", " and LOWER(Dbname) like '",proj,"%'", " and trn_dt1 >= '" , type_dt, " '")
}



# Final Transaction data for simulation
Txn_Data <- sqldf(Txn_Data_Fromat)

# print("Dim of Txn_Data:")
# dim(Txn_Data)

# Removing duplicates like in SAS
# Removing duplicates => PROC SORT DATA=Final_Data OUT=Final_Data_1 NODUPKEY; BY FI_TRANSACTION_ID; RUN;
Txn_Data <- subset(Txn_Data, !duplicated(subset(Txn_Data, select=c(FI_TRANSACTION_ID))))
#Sorting => PROC SORT DATA=Final_Data_1 OUT=Txn_Data; BY ACCT_NBR AUTH_DTTM; RUN;
Txn_Data <- Txn_Data[order(Txn_Data$ACCT_NBR,Txn_Data$AUTH_DTTM_2), ]

FALT002 <- Txn_Data

# Sorting and removing duplicates for FI_TRANSACTION_ID & filtering for the USR_DAT_1
FALT002 = sqldf('SELECT DISTINCT FI_TRANSACTION_ID, * FROM FALT002 ORDER BY FI_TRANSACTION_ID ASC;')
# To remove }
#}

# print("Dim of FALT002:")
# dim(FALT002)


			#**************************************************************************************
			
										#### FALT003 - RULE HITS ####

			#**************************************************************************************

# Subsetting the data dump Country_Rule_Data for needed country and project data
Rule_Data_Fromat <- paste0("Select * from Country_Rule_Data WHERE " , 
" UPPER(Rule_cnty) in ( ", mydata$Tenant[i] ," ) ", " and LOWER(Rule_Dbname) like '%",proj,"%'",
" and UPPER(RULE_NAME_STRG) = ", mydata[i,4]," and trn_dt1 >= '", type_dt, " '")

# Final Rule data for simulation
Rule_Data <- sqldf(Rule_Data_Fromat)

# print("Dim of Rule_Data:")
# dim(Rule_Data)

# Removing duplicates like in SAS
# PROC SORT DATA=Rule_Data NODUPKEY; BY RULE_NAME_STRG FI_TRANSACTION_ID; RUN;
# Additional commands
# subset(Rule_Data, !duplicated(Rule_Data[,"FI_TRANSACTION_ID"]))
# Rule_Data[!duplicated(Rule_Data[ , c("RULE_NAME_STRG","FI_TRANSACTION_ID")]),]
# Removing duplicates like in SAS
# Removing duplicates & Sorting => PROC SORT DATA=Rule_Data NODUPKEY; BY RULE_NAME_STRG FI_TRANSACTION_ID; RUN;
Rule_Data <- subset(Rule_Data, !duplicated(subset(Rule_Data, select=c(RULE_NAME_STRG, FI_TRANSACTION_ID))))
Rule_Data <- Rule_Data[order(Rule_Data$RULE_NAME_STRG,Rule_Data$FI_TRANSACTION_ID), ]

FALT003 <- Rule_Data

# Sorting and removing duplicates for FI_TRANSACTION_ID & filtering for the USR_DAT_1
FALT003 = sqldf('SELECT DISTINCT FI_TRANSACTION_ID, * FROM FALT003 ORDER BY FI_TRANSACTION_ID ASC;')
# To remove }
#}

# print("Dim of FALT003:")
# dim(FALT003)

			#**************************************************************************************
			
										   #### COUNT COMPARISON ####
			
			#**************************************************************************************

Rule_counter = Rule_counter + 1
# printing "Rule_Name"
# USed to print original Rule_Name in output instead of UPPER case rule name
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

# dt1 <- format(as.Date(Sys.Date() -1), format = "%Y-%m-%d")
dt1 <- format(as.Date(Sys.Date() -1), "%d-%b-%y")

# dt2 <- format(as.Date(Sys.Date() -2), format = "%Y-%m-%d")
dt2 <- format(as.Date(Sys.Date() -2), "%d-%b-%y")

# dt2 <- format(as.Date(Sys.Date() -2), format = "%Y-%m-%d")
# dt2 <- format(as.Date(dt2), "%d-%m-%Y")

output[i,1] = mydata$Original_Rule_Name[i]
FALT002_count1 = sqldf(paste0("Select count(DISTINCT FI_TRANSACTION_ID) from FALT002 WHERE ", "trn_dt1 = '" , dt1, " '"))
FALT002_count1_data = sqldf(paste0("Select * from FALT002 WHERE ", "trn_dt1 = '" , dt1, " '"))
output[i,2] = FALT002_count1
FALT002_count2 = sqldf(paste0("Select count(DISTINCT FI_TRANSACTION_ID) from FALT002 WHERE ", "trn_dt1 = '" , dt2, " '"))
FALT002_count2_data = sqldf(paste0("Select * from FALT002 WHERE ", "trn_dt1 = '" , dt2, " '"))
output[i,3] = FALT002_count2
FALT003_count1 = sqldf(paste0("Select count(DISTINCT FI_TRANSACTION_ID) from FALT003 WHERE ", "trn_dt1 = '" , dt1, " '"))
FALT003_count1_data = sqldf(paste0("Select * from FALT003 WHERE ", "trn_dt1 = '" , dt1, " '"))
output[i,4] = FALT003_count1
FALT003_count2 = sqldf(paste0("Select count(DISTINCT FI_TRANSACTION_ID) from FALT003 WHERE ", "trn_dt1 = '" , dt2, " '"))
FALT003_count2_data = sqldf(paste0("Select * from FALT003 WHERE ", "trn_dt1 = '" , dt2, " '"))
output[i,5] = FALT003_count2
output[i,7] = mydata[i,8]
output[i,8] = format(as.Date(Sys.Date()), format = "%Y-%m-%d")

FALT002_temp = rbind(FALT002_count1_data, FALT002_count2_data)
FALT003_temp = rbind(FALT003_count1_data, FALT003_count2_data)

if(sum(FALT002_count1 + FALT002_count2) == sum(FALT003_count1 + FALT003_count2))
{
output[i,6] = "Matched"
} else {
output[i,6] = "Unmatched"
}

output[i,9] = mydata$Original_Tenant[i]
output[i,10] = mydata$Original_Project[i]

Original_Rule_Name = mydata$Original_Rule_Name[i]
Tnt = mydata$Tenant[i]
Tnt_cntry = mydata$Tenant[i]
proj_tmp = toupper(proj)

# To frame file name in case of many countires for Global
if (nchar(Tnt) > 10){
Tnt = "Global"
}

output_file_name = paste0("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/Results/",Original_Rule_Name,"_",Tnt,"_",proj_tmp,"_",Today,".xlsx",collapse="")
# print(output_file_name)

# To write oputput Nos. only for the specific rule
if (nchar(Tnt) > 1){
Rule_Output = sqldf(paste0("Select * from output WHERE ", "Rule_Name = ","'", output[i,1],"'", " and Project = ", "'", mydata$Project[i],"'"))
} else
{
Rule_Output = sqldf(paste0("Select * from output WHERE ", "Rule_Name = ","'", output[i,1],"'", " and Tenant in ", "(", Tnt_cntry, ")", " and Project = ", "'", mydata$Project[i],"'"))
}

# print(Rule_Output)

# Write the Output nos. R Vs Falcon to Excel
write.xlsx2(Rule_Output, file = output_file_name, sheetName="output", row.names=FALSE, append=TRUE)

# Write the FALT002 of R to Excel
if(dim(FALT002_temp)[1] == 0){
write.xlsx.custom(FALT002_temp, file = output_file_name, sheetName="FALT002", append=TRUE, row.names=FALSE)
}else{
write.xlsx2(FALT002_temp, file = output_file_name, sheetName="FALT002", row.names=FALSE, append=TRUE)
}

# Write the FALT003 of Falcon to Excel
if(dim(FALT003_temp)[1] == 0){
write.xlsx.custom(FALT003_temp, file = output_file_name, sheetName="FALT003", append=TRUE, row.names=FALSE)
}else{
write.xlsx2(FALT003_temp, file = output_file_name, sheetName="FALT003", row.names=FALSE, append=TRUE)
}

# Log file
sink("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/Results/Log_file.log", append=TRUE, split=TRUE)
}

# SINK RESET
# sink()
# sink("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/Results/Log_file.log")

# Exporting output results to local for reference
#export(output,paste0("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/","R_Simulation_Output","_",Original_Rule_Name,Today,".xlsx",collapse=""),which="R_Simulation")

# Exporting Txn_Data results to local for reference
#export(FALT002,paste0("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/","FALT002","_",Original_Rule_Name,Today,".xlsx",collapse=""),which="Txn_Data")

# Exporting Rule_Data results to local for reference
#export(FALT003,paste0("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/","FALT003","_",Original_Rule_Name,Today,".xlsx",collapse=""),which="Rule_Data")

# Log for output
# sink("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/Results/Log_file.log", append=TRUE, split=TRUE)
