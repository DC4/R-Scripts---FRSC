# Head of Country_Txn_Data_1
head(Country_Txn_Data_1)
dim(Country_Txn_Data_1)
dim(Country_Txn_Data_CR)
dim(Country_Txn_Data_DB)

# Loading the CR and DB data commonly

Country_Txn_Data_Hot_CR <- data.table(Country_Txn_Data_CR)
Country_Txn_Data_Hot_DB <- data.table(Country_Txn_Data_DB)

############################################################################################


# HK = LIMIT_BASED_WHITELIST_HK | MR_Country_VIP_Decline_HK CR

LIMIT_BASED_WHITELIST_HK <- read_excel("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/Hotlist/LIMIT_BASED_WHITELIST_HK.xls")

LIMIT_BASED_WHITELIST_HK <- data.table(LIMIT_BASED_WHITELIST_HK)

Name_list_HK <- sqldf("Select Name from LIMIT_BASED_WHITELIST_HK")

Country_Txn_Data_1_sub_1 <- sqldf("Select * from Country_Txn_Data_Hot_CR where trim(ACCT_NBR) in Name_list_HK")

if(nrow(Country_Txn_Data_1_sub_1) == 0){
print("no match MR_COUNTRY_VIP_DECLINE_HK")
}else{
Country_Txn_Data_11 <- anti_join(Country_Txn_Data_1, Country_Txn_Data_1_sub_1, by = "FI_TRANSACTION_ID")

Country_Txn_Data_1_sub_1$Hotlist_Rule_Name = 'MR_COUNTRY_VIP_DECLINE_HK'

Country_Txn_Data_1 <- bind_rows(Country_Txn_Data_11, Country_Txn_Data_1_sub_1)
}

# Dimension of Country_Txn_Data_1 and Country_Txn_Data_44 must match
head(Country_Txn_Data_1)
dim(Country_Txn_Data_1)
dim(Country_Txn_Data_CR)
dim(Country_Txn_Data_DB)


############################################################################################


# HK = LIMIT_BASED_WHITELIST_HK | MR_Country_VIP_Decline_HK DR

LIMIT_BASED_WHITELIST_HK <- read_excel("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/Hotlist/LIMIT_BASED_WHITELIST_HK.xls")

LIMIT_BASED_WHITELIST_HK <- data.table(LIMIT_BASED_WHITELIST_HK)

Name_list_HK <- sqldf("Select Name from LIMIT_BASED_WHITELIST_HK")

Country_Txn_Data_1_sub_2 <- sqldf("Select * from Country_Txn_Data_DB where trim(ACCT_NBR) in Name_list_HK")

if(nrow(Country_Txn_Data_1_sub_2) == 0){
print("no match MR_COUNTRY_VIP_DECLINE_HK")
}else{
Country_Txn_Data_22 <- anti_join(Country_Txn_Data_1, Country_Txn_Data_1_sub_2, by = "FI_TRANSACTION_ID")

Country_Txn_Data_1_sub_2$Hotlist_Rule_Name = 'MR_COUNTRY_VIP_DECLINE_HK'

Country_Txn_Data_1 <- bind_rows(Country_Txn_Data_22, Country_Txn_Data_1_sub_2)
}

# Dimension of Country_Txn_Data_1 and Country_Txn_Data_44 must match
head(Country_Txn_Data_1)
dim(Country_Txn_Data_1)
dim(Country_Txn_Data_CR)
dim(Country_Txn_Data_DB)


############################################################################################


# TW = BLACKLISTED_MERCHANTS_SMS_ONLY | LR_CC_Blacklist_Only_SMS_Txns_Test

BLACKLISTED_MERCHANTS_SMS_ONLY <- read_excel("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/Hotlist/BLACKLISTED_MERCHANTS_SMS_ONLY.xls")

BLACKLISTED_MERCHANTS_SMS_ONLY <- data.table(BLACKLISTED_MERCHANTS_SMS_ONLY)

Name_list_TW <- sqldf("Select Name from BLACKLISTED_MERCHANTS_SMS_ONLY")

Country_Txn_Data_1_sub_3 <- sqldf("Select * from Country_Txn_Data_CR where trim(mer_id) in Name_list_TW")

if(nrow(Country_Txn_Data_1_sub_3) == 0){
print("nomatch LR_CC_BLACKLIST_ONLY_SMS_TXNS_TEST")
}else{
Country_Txn_Data_1_sub_3$Hotlist_Rule_Name = 'LR_CC_BLACKLIST_ONLY_SMS_TXNS_TEST'

Country_Txn_Data_33 <- anti_join(Country_Txn_Data_1, Country_Txn_Data_1_sub_3, by = "FI_TRANSACTION_ID")

Country_Txn_Data_1 <- bind_rows(Country_Txn_Data_33, Country_Txn_Data_1_sub_3)
}

# Dimension of Country_Txn_Data_1 and Country_Txn_Data_44 must match
head(Country_Txn_Data_1)
dim(Country_Txn_Data_1)
dim(Country_Txn_Data_CR)
dim(Country_Txn_Data_DB)


############################################################################################

# Debit and Credit Data segregating
Country_Txn_Data_DB <- Country_Txn_Data_1[which(str_sub(Country_Txn_Data_1$CRD_CLNT_ID,-3) == '_DB'), ]
Country_Txn_Data_CR <- Country_Txn_Data_1[which(str_sub(Country_Txn_Data_1$CRD_CLNT_ID,-3) == '_CR'), ]
