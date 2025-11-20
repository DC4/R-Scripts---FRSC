# Head of Country_Txn_Data_1
head(Country_Txn_Data_1)
dim(Country_Txn_Data_1)
dim(Country_Txn_Data_CR)
dim(Country_Txn_Data_DB)

# Loading the CR and DB data commonly

Country_Txn_Data_Hot_CR <- data.table(Country_Txn_Data_CR)
Country_Txn_Data_Hot_DB <- data.table(Country_Txn_Data_DB)


##########################################################################################

# HR_COUNTRY_COMPROMISE_NCCC	TW	Credit

# Common = COMPROMISE_NCCC

COMPROMISE_NCCC <- read_excel("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/Hotlist/COMPROMISE_NCCC.xls")

COMPROMISE_NCCC <- data.table(COMPROMISE_NCCC)

Name_list_TW_CR <- sqldf("Select Name from COMPROMISE_NCCC")

Country_Txn_Data_1_sub <- sqldf("Select * from Country_Txn_Data_Hot_CR where Trim(mer_id) in Name_list_TW_CR ")

if(nrow(Country_Txn_Data_1_sub) == 0){
print("nomatch HR_COUNTRY_COMPROMISE_NCCC_CR")
}else{
Country_Txn_Data_11 <- anti_join(Country_Txn_Data_1, Country_Txn_Data_1_sub, by = "FI_TRANSACTION_ID")

Country_Txn_Data_1_sub$Hotlist_Rule_Name = 'HR_COUNTRY_COMPROMISE_NCCC_CR'

Country_Txn_Data_1 <- bind_rows(Country_Txn_Data_11, Country_Txn_Data_1_sub)
}


# Head of Country_Txn_Data_1
# Dimension of Country_Txn_Data_1 and Country_Txn_Data_11 must match
head(Country_Txn_Data_1)
dim(Country_Txn_Data_1)

##########################################################################################

# HR_COUNTRY_COMPROMISE_NCCC	TW	Debit

# Common = COMPROMISE_NCCC

COMPROMISE_NCCC <- read_excel("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/Hotlist/COMPROMISE_NCCC.xls")

COMPROMISE_NCCC <- data.table(COMPROMISE_NCCC)

Name_list_TW_DB <- sqldf("Select Name from COMPROMISE_NCCC")

Country_Txn_Data_1_sub_1 <- sqldf("Select * from Country_Txn_Data_Hot_DB where Trim(mer_id) in Name_list_TW_DB ")

if(nrow(Country_Txn_Data_1_sub_1) == 0){
print("nomatch HR_COUNTRY_COMPROMISE_NCCC_DR")
}else{
Country_Txn_Data_22 <- anti_join(Country_Txn_Data_1, Country_Txn_Data_1_sub_1, by = "FI_TRANSACTION_ID")

Country_Txn_Data_1_sub_1$Hotlist_Rule_Name = 'HR_COUNTRY_COMPROMISE_NCCC_DR'

Country_Txn_Data_1 <- bind_rows(Country_Txn_Data_22, Country_Txn_Data_1_sub_1)
}


# Head of Country_Txn_Data_1
# Dimension of Country_Txn_Data_1 and Country_Txn_Data_22 must match
head(Country_Txn_Data_1)
dim(Country_Txn_Data_1)

##########################################################################################


# Debit and Credit Data segregating
Country_Txn_Data_DB <- Country_Txn_Data_1[which(str_sub(Country_Txn_Data_1$CRD_CLNT_ID,-3) == '_DB'), ]
Country_Txn_Data_CR <- Country_Txn_Data_1[which(str_sub(Country_Txn_Data_1$CRD_CLNT_ID,-3) == '_CR'), ]
