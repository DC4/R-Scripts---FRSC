# Head of Country_Txn_Data_1
head(Country_Txn_Data_1)
dim(Country_Txn_Data_1)
dim(Country_Txn_Data_CR)
dim(Country_Txn_Data_DB)

# Loading the CR and DB data commonly

Country_Txn_Data_Hot_CR <- data.table(Country_Txn_Data_CR)
Country_Txn_Data_Hot_DB <- data.table(Country_Txn_Data_DB)

############################################################################################

# TW = BLACKLISTED MERCHANTS  & MID_WHITELIST_GLOBAL_TW| MR_Merchant_ACQID_Loc_CP_Test TW

TW_COMBINED_HOT <- read_excel("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/Hotlist/TW_COMBINED_HOT.xls")

TW_COMBINED_HOT <- data.table(TW_COMBINED_HOT)

Name_list_TW <- sqldf("Select Name from TW_COMBINED_HOT")

Country_Txn_Data_1_sub_1 <- sqldf("Select * from Country_Txn_Data_Hot_DB where trim(MER_ID) not in Name_list_TW")

if(nrow(Country_Txn_Data_1_sub_1) == 0){
print("no match MR_Merchant_ACQID_Loc_CP_Test")
}else{
Country_Txn_Data_11 <- anti_join(Country_Txn_Data_1, Country_Txn_Data_1_sub_1, by = "FI_TRANSACTION_ID")

Country_Txn_Data_1_sub_1$Hotlist_Rule_Name = 'MR_MERCHANT_ACQID_LOC_CP_TEST'

Country_Txn_Data_1 <- bind_rows(Country_Txn_Data_11, Country_Txn_Data_1_sub_1)
}

# Dimension of Country_Txn_Data_1 and Country_Txn_Data_11 must match
head(Country_Txn_Data_1)
dim(Country_Txn_Data_1)
dim(Country_Txn_Data_CR)
dim(Country_Txn_Data_DB)

############################################################################################

# Debit and Credit Data segregating
Country_Txn_Data_DB <- Country_Txn_Data_1[which(str_sub(Country_Txn_Data_1$CRD_CLNT_ID,-3) == '_DB'), ]
Country_Txn_Data_CR <- Country_Txn_Data_1[which(str_sub(Country_Txn_Data_1$CRD_CLNT_ID,-3) == '_CR'), ]
