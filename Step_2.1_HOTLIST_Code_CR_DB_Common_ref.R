# Head of Country_Txn_Data_1
head(Country_Txn_Data_1)
dim(Country_Txn_Data_1)
dim(Country_Txn_Data_CR)
dim(Country_Txn_Data_DB)

# Loading the CR and DB data commonly

Country_Txn_Data_Hot_CR <- data.table(Country_Txn_Data_CR)
Country_Txn_Data_Hot_DB <- data.table(Country_Txn_Data_DB)

##########################################################################################

# Common = COLLUSIVE_MERCHANTS_BASE

COLLUSIVE_MERCHANTS_BASE <- read_excel("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/Hotlist/COLLUSIVE_MERCHANTS_BASE.xls")

COLLUSIVE_MERCHANTS_BASE <- data.table(COLLUSIVE_MERCHANTS_BASE)

Name_list_COLL <- sqldf("Select Name from COLLUSIVE_MERCHANTS_BASE")

Country_Txn_Data_1_sub <- sqldf("Select * from Country_Txn_Data_DB where trim(mer_id) in Name_list_COLL")

if(nrow(Country_Txn_Data_1_sub) == 0){
print("nomatch HR_GLOBAL_COLLUSIVE_MERCHANT_TEST")
}else{
Country_Txn_Data_11 <- anti_join(Country_Txn_Data_1, Country_Txn_Data_1_sub, by = "FI_TRANSACTION_ID")

Country_Txn_Data_1_sub$Hotlist_Rule_Name = 'HR_GLOBAL_COLLUSIVE_MERCHANT_TEST'

Country_Txn_Data_1 <- bind_rows(Country_Txn_Data_11, Country_Txn_Data_1_sub, fill = NA)
}


# Head of Country_Txn_Data_1
# Dimension of Country_Txn_Data_1 and Country_Txn_Data_22 must match
head(Country_Txn_Data_1)
dim(Country_Txn_Data_1)
dim(Country_Txn_Data_CR)
dim(Country_Txn_Data_DB)

############################################################################################


# HK = MID_WHITELIST_GLOBAL_HK

MID_WHITELIST_GLOBAL_HK <- read_excel("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/Hotlist/MID_WHITELIST_GLOBAL_HK.xls")

MID_WHITELIST_GLOBAL_HK <- data.table(MID_WHITELIST_GLOBAL_HK)

Name_list_HK <- sqldf("Select Name from MID_WHITELIST_GLOBAL_HK")

Country_Txn_Data_1_sub_1 <- sqldf("Select * from Country_Txn_Data_Hot_CR where trim(mer_id) not in Name_list_HK")


if(nrow(Country_Txn_Data_1_sub_1) == 0){
print("nomatch MR_MERCHANT_ACQID_LOC_CP_TEST")
}else{
Country_Txn_Data_22 <- anti_join(Country_Txn_Data_1, Country_Txn_Data_1_sub_1, by = "FI_TRANSACTION_ID")

Country_Txn_Data_1_sub_1$Hotlist_Rule_Name = 'MR_MERCHANT_ACQID_LOC_CP_TEST'

Country_Txn_Data_1 <- bind_rows(Country_Txn_Data_22, Country_Txn_Data_1_sub_1)
}

# Dimension of Country_Txn_Data_1 and Country_Txn_Data_44 must match
head(Country_Txn_Data_1)
dim(Country_Txn_Data_1)
dim(Country_Txn_Data_CR)
dim(Country_Txn_Data_DB)


############################################################################################


# HK = BLACKLISTED_MERCHANTS_HK

BLACKLISTED_MERCHANTS_HK <- read_excel("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/Hotlist/BLACKLISTED_MERCHANTS_HK.xls")

BLACKLISTED_MERCHANTS_HK <- data.table(BLACKLISTED_MERCHANTS_HK)

Name_list_HK_1 <- sqldf("Select Name from BLACKLISTED_MERCHANTS_HK")

Country_Txn_Data_1_sub_2 <- sqldf("Select * from Country_Txn_Data_CR where trim(mer_id) not in Name_list_HK_1")

if(nrow(Country_Txn_Data_1_sub_2) == 0){
print("nomatch MR_MERCHANT_ACQID_LOC_CP_TEST")
}else{
Country_Txn_Data_1_sub_2$Hotlist_Rule_Name = 'MR_MERCHANT_ACQID_LOC_CP_TEST'

Country_Txn_Data_33 <- anti_join(Country_Txn_Data_1, Country_Txn_Data_1_sub_2, by = "FI_TRANSACTION_ID")

Country_Txn_Data_1 <- bind_rows(Country_Txn_Data_33, Country_Txn_Data_1_sub_2)
}

# Dimension of Country_Txn_Data_1 and Country_Txn_Data_44 must match
head(Country_Txn_Data_1)
dim(Country_Txn_Data_1)
dim(Country_Txn_Data_CR)
dim(Country_Txn_Data_DB)

############################################################################################

# DR = BLACKLISTED_MERCHANTS_DR

BLACKLISTED_MERCHANTS_DR <- read_excel("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/Hotlist/BLACKLISTED_MERCHANTS_DR.xls")

BLACKLISTED_MERCHANTS_DR <- data.table(BLACKLISTED_MERCHANTS_DR)

Name_list_DR <- sqldf("Select Name from BLACKLISTED_MERCHANTS_DR")

Country_Txn_Data_1_sub_3 <- sqldf("Select * from Country_Txn_Data_CR where trim(mer_id) in Name_list_DR")

if(nrow(Country_Txn_Data_1_sub_3) == 0){
print("nomatch MR_MERCHANT_ACQID_LOC_CP_TEST")
}else{
Country_Txn_Data_1_sub_3$Hotlist_Rule_Name = 'HR_COUNTRY_MID_BLACKLIST_HK_TEST'

Country_Txn_Data_44 <- anti_join(Country_Txn_Data_1, Country_Txn_Data_1_sub_3, by = "FI_TRANSACTION_ID")

Country_Txn_Data_1 <- bind_rows(Country_Txn_Data_44, Country_Txn_Data_1_sub_3)

}

# Dimension of Country_Txn_Data_1 and Country_Txn_Data_44 must match
head(Country_Txn_Data_1)
dim(Country_Txn_Data_1)
dim(Country_Txn_Data_CR)
dim(Country_Txn_Data_DB)

# Debit and Credit Data segregating
Country_Txn_Data_DB <- Country_Txn_Data_1[which(str_sub(Country_Txn_Data_1$CRD_CLNT_ID,-3) == '_DB'), ]
Country_Txn_Data_CR <- Country_Txn_Data_1[which(str_sub(Country_Txn_Data_1$CRD_CLNT_ID,-3) == '_CR'), ]
