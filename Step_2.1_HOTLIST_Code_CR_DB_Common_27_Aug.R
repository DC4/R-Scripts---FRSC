# Head of Country_Txn_Data_1
head(Country_Txn_Data_1)
dim(Country_Txn_Data_1)
dim(Country_Txn_Data_CR)
dim(Country_Txn_Data_DB)

# Loading the CR and DB data commonly

Country_Txn_Data_Hot_CR <- data.table(Country_Txn_Data_CR)
Country_Txn_Data_Hot_DB <- data.table(Country_Txn_Data_DB)

############################################################################################


# DR = BLACKLISTED_MERCHANTS_DR | XCNI_MR_Merchant_ACQID_Loc_CP_Test CR

BLACKLISTED_MERCHANTS_DR <- read_excel("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/Hotlist/BLACKLISTED_MERCHANTS_DR.xls")

BLACKLISTED_MERCHANTS_DR <- data.table(BLACKLISTED_MERCHANTS_DR)

Name_list_DR <- sqldf("Select Name from BLACKLISTED_MERCHANTS_DR")

Country_Txn_Data_1_sub_1 <- sqldf("Select * from Country_Txn_Data_Hot_CR where trim(MER_ID) not in Name_list_DR")

if(nrow(Country_Txn_Data_1_sub_1) == 0){
print("no match XCNI_MR_MERCHANT_ACQID_LOC_CP_TEST")
}else{
Country_Txn_Data_11 <- anti_join(Country_Txn_Data_1, Country_Txn_Data_1_sub_1, by = "FI_TRANSACTION_ID")

Country_Txn_Data_1_sub_1$Hotlist_Rule_Name = 'XCNI_MR_MERCHANT_ACQID_LOC_CP_TEST'

Country_Txn_Data_1 <- bind_rows(Country_Txn_Data_11, Country_Txn_Data_1_sub_1)
}

# Dimension of Country_Txn_Data_1 and Country_Txn_Data_11 must match
head(Country_Txn_Data_1)
dim(Country_Txn_Data_1)
dim(Country_Txn_Data_CR)
dim(Country_Txn_Data_DB)

############################################################################################

# TW = BLACKLISTED_MERCHANTS_TW  & MID_WHITELIST_GLOBAL_TW| HR_MERCHANT_ACQID_INT_CNP_TEST TW

BLACKLISTED_MERCHANTS_TW <- read_excel("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/Hotlist/BLACKLISTED_MERCHANTS_TW.xls")

BLACKLISTED_MERCHANTS_TW <- data.table(BLACKLISTED_MERCHANTS_TW)

Name_list_TW <- sqldf("Select Name from BLACKLISTED_MERCHANTS_TW")

Country_Txn_Data_1_sub_2 <- sqldf("Select * from Country_Txn_Data_DB where trim(MER_ID) not in Name_list_TW")

if(nrow(Country_Txn_Data_1_sub_2) == 0){
print("no match HR_MERCHANT_ACQID_INT_CNP_TEST")
}else{
Country_Txn_Data_22 <- anti_join(Country_Txn_Data_1, Country_Txn_Data_1_sub_2, by = "FI_TRANSACTION_ID")

Country_Txn_Data_1_sub_2$Hotlist_Rule_Name = 'HR_MERCHANT_ACQID_INT_CNP_TEST'

Country_Txn_Data_1 <- bind_rows(Country_Txn_Data_22, Country_Txn_Data_1_sub_2)
}

# Dimension of Country_Txn_Data_1 and Country_Txn_Data_22 must match
head(Country_Txn_Data_1)
dim(Country_Txn_Data_1)
dim(Country_Txn_Data_CR)
dim(Country_Txn_Data_DB)

############################################################################################

# Debit and Credit Data segregating
Country_Txn_Data_DB <- Country_Txn_Data_1[which(str_sub(Country_Txn_Data_1$CRD_CLNT_ID,-3) == '_DB'), ]
Country_Txn_Data_CR <- Country_Txn_Data_1[which(str_sub(Country_Txn_Data_1$CRD_CLNT_ID,-3) == '_CR'), ]
