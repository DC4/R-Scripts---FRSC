# Head of Country_Txn_Data_1
head(Country_Txn_Data_1)
dim(Country_Txn_Data_1)
dim(Country_Txn_Data_CR)
dim(Country_Txn_Data_DB)

# Loading the CR and DB data commonly

Country_Txn_Data_Hot_CR <- data.table(Country_Txn_Data_CR)
Country_Txn_Data_Hot_DB <- data.table(Country_Txn_Data_DB)


##########################################################################################

# HR_Country_BLACKLIST_Mer_BD	BD	Credit

# Common = BD_BLACKLISTED_MERCHANTS

BD_BLACKLISTED_MERCHANTS <- read_excel("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/Hotlist/BD_BLACKLISTED_MERCHANTS.xls")

BD_BLACKLISTED_MERCHANTS <- data.table(BD_BLACKLISTED_MERCHANTS)

Name_list_BD_CR <- sqldf("Select Name from BD_BLACKLISTED_MERCHANTS")

Country_Txn_Data_1_sub <- sqldf("Select * from Country_Txn_Data_Hot_CR where Trim(mer_id) in Name_list_BD_CR ")

if(nrow(Country_Txn_Data_1_sub) == 0){
print("nomatch HR_COUNTRY_BLACKLIST_MER_BD")
}else{
Country_Txn_Data_11 <- anti_join(Country_Txn_Data_1, Country_Txn_Data_1_sub, by = "FI_TRANSACTION_ID")

Country_Txn_Data_1_sub$Hotlist_Rule_Name = 'HR_COUNTRY_BLACKLIST_MER_BD'

Country_Txn_Data_1 <- bind_rows(Country_Txn_Data_11, Country_Txn_Data_1_sub)
}


# Head of Country_Txn_Data_1
# Dimension of Country_Txn_Data_1 and Country_Txn_Data_11 must match
head(Country_Txn_Data_1)
dim(Country_Txn_Data_1)
dim(Country_Txn_Data_CR)
dim(Country_Txn_Data_DB)


##########################################################################################

# HR_Country_BLACKLIST_Mer_BD	BD	Debit

# Common = BD_BLACKLISTED_MERCHANTS

BD_BLACKLISTED_MERCHANTS <- read_excel("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/Hotlist/BD_BLACKLISTED_MERCHANTS.xls")

BD_BLACKLISTED_MERCHANTS <- data.table(BD_BLACKLISTED_MERCHANTS)

Name_list_BD_DB <- sqldf("Select Name from BD_BLACKLISTED_MERCHANTS")

Country_Txn_Data_1_sub_1 <- sqldf("Select * from Country_Txn_Data_Hot_DB where Trim(mer_id) in Name_list_BD_DB ")

if(nrow(Country_Txn_Data_1_sub_1) == 0){
print("nomatch HR_COUNTRY_BLACKLIST_MER_BD")
}else{
Country_Txn_Data_22 <- anti_join(Country_Txn_Data_1, Country_Txn_Data_1_sub_1, by = "FI_TRANSACTION_ID")

Country_Txn_Data_1_sub_1$Hotlist_Rule_Name = 'HR_COUNTRY_BLACKLIST_MER_BD'

Country_Txn_Data_1 <- bind_rows(Country_Txn_Data_22, Country_Txn_Data_1_sub_1)
}


# Head of Country_Txn_Data_1
# Dimension of Country_Txn_Data_1 and Country_Txn_Data_22 must match
head(Country_Txn_Data_1)
dim(Country_Txn_Data_1)
dim(Country_Txn_Data_CR)
dim(Country_Txn_Data_DB)


##########################################################################################


# HR_Global_Blacklistingn1_TW	 'TW'	Credit

TW_BLACKLISTED_MERCHANTS <- read_excel("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/Hotlist/Hotlist_160921_TW.xls")

TW_BLACKLISTED_MERCHANTS <- data.table(TW_BLACKLISTED_MERCHANTS)

Name_list_TW_CR <- sqldf("Select Name from TW_BLACKLISTED_MERCHANTS")

Country_Txn_Data_1_sub_2 <- sqldf("Select * from Country_Txn_Data_Hot_CR where Trim(mer_id) in Name_list_TW_CR ")

if(nrow(Country_Txn_Data_1_sub_2) == 0){
print("nomatch HR_GLOBAL_BLACKLISTINGN1_TW")
}else{
Country_Txn_Data_1_sub_2$Hotlist_Rule_Name = 'HR_GLOBAL_BLACKLISTINGN1_TW'

Country_Txn_Data_33 <- anti_join(Country_Txn_Data_1, Country_Txn_Data_1_sub_2, by = "FI_TRANSACTION_ID")

Country_Txn_Data_1 <- bind_rows(Country_Txn_Data_33, Country_Txn_Data_1_sub_2)
}

# Dimension of Country_Txn_Data_1 and Country_Txn_Data_44 must match
head(Country_Txn_Data_1)
dim(Country_Txn_Data_1)
dim(Country_Txn_Data_CR)
dim(Country_Txn_Data_DB)


##########################################################################################


# HR_Global_Blacklistingn1_TW	 'TW'	Debit

TW_BLACKLISTED_MERCHANTS <- read_excel("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/Hotlist/Hotlist_160921_TW.xls")

TW_BLACKLISTED_MERCHANTS <- data.table(TW_BLACKLISTED_MERCHANTS)

Name_list_TW_DB <- sqldf("Select Name from TW_BLACKLISTED_MERCHANTS")

Country_Txn_Data_1_sub_3 <- sqldf("Select * from Country_Txn_Data_Hot_DB where (Trim(mer_id) in Name_list_TW_DB) or (Trim(mer_id) in ('4445091120618', '4445090874552')) or (TRIM(MER_ID) IN ('000812770010915', '000812770010917', '000812770010918', '000812770010919', '000812770010920') AND TRN_AMT >= 5000)")

if(nrow(Country_Txn_Data_1_sub_3) == 0){
print("nomatch HR_GLOBAL_BLACKLISTINGN1_TW")
}else{
Country_Txn_Data_1_sub_3$Hotlist_Rule_Name = 'HR_GLOBAL_BLACKLISTINGN1_TW'

Country_Txn_Data_44 <- anti_join(Country_Txn_Data_1, Country_Txn_Data_1_sub_3, by = "FI_TRANSACTION_ID")

Country_Txn_Data_1 <- bind_rows(Country_Txn_Data_44, Country_Txn_Data_1_sub_3)
}

# Dimension of Country_Txn_Data_1 and Country_Txn_Data_44 must match
head(Country_Txn_Data_1)
dim(Country_Txn_Data_1)
dim(Country_Txn_Data_CR)
dim(Country_Txn_Data_DB)


##########################################################################################


# HR_Global_Blacklisting_n1_1	 'AE', 'BH', 'BN', 'BW', 'GH', 'ID', 'IN', 'JE', 'JO', 'KE', 'LK', 'MY', 'NG', 'NP', 'SG', # 'VN', 'ZM'	credit


GBL_BLACKLISTED_MERCHANTS <- read_excel("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/Hotlist/GBL_BLACKLISTED_MERCHANTS.xls")

GBL_BLACKLISTED_MERCHANTS <- data.table(GBL_BLACKLISTED_MERCHANTS)

Name_list_GBL <- sqldf("Select Name from GBL_BLACKLISTED_MERCHANTS")

Country_Txn_Data_1_sub_4 <- sqldf("Select * from Country_Txn_Data_Hot_CR where Trim(mer_id) in Name_list_GBL")

if(nrow(Country_Txn_Data_1_sub_4) == 0){
print("nomatch HR_GLOBAL_BLACKLISTING_N1_1")
}else{
Country_Txn_Data_1_sub_4$Hotlist_Rule_Name = 'HR_GLOBAL_BLACKLISTING_N1_1'

Country_Txn_Data_55 <- anti_join(Country_Txn_Data_1, Country_Txn_Data_1_sub_4, by = "FI_TRANSACTION_ID")

Country_Txn_Data_1 <- bind_rows(Country_Txn_Data_55, Country_Txn_Data_1_sub_4)
}

# Dimension of Country_Txn_Data_1 and Country_Txn_Data_55 must match
head(Country_Txn_Data_1)
dim(Country_Txn_Data_1)
dim(Country_Txn_Data_CR)
dim(Country_Txn_Data_DB)


##########################################################################################


# Debit and Credit Data segregating
Country_Txn_Data_DB <- Country_Txn_Data_1[which(str_sub(Country_Txn_Data_1$CRD_CLNT_ID,-3) == '_DB'), ]
Country_Txn_Data_CR <- Country_Txn_Data_1[which(str_sub(Country_Txn_Data_1$CRD_CLNT_ID,-3) == '_CR'), ]
