# Head of Country_Txn_Data_1
head(Country_Txn_Data_1)
dim(Country_Txn_Data_1)
dim(Country_Txn_Data_CR)
dim(Country_Txn_Data_DB)

# Loading the CR and DB data commonly

Country_Txn_Data_Hot_CR <- data.table(Country_Txn_Data_CR)
Country_Txn_Data_Hot_DB <- data.table(Country_Txn_Data_DB)


##########################################################################################

# HR_Country_GH_Missinng_CVV	GBL	 Debit

# Common = GHANA_CARDS

GHANA_CARDS <- read_excel("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/Hotlist/GHANA_CARDS.xls")

GHANA_CARDS <- data.table(GHANA_CARDS)

Name_list_GBL_DR <- sqldf("Select Name from GHANA_CARDS")

Country_Txn_Data_1_sub <- sqldf("Select * from Country_Txn_Data_Hot_DB where Trim(ACCT_NBR) in Name_list_GBL_DR ")

if(nrow(Country_Txn_Data_1_sub) == 0){
print("nomatch HR_COUNTRY_GH_MISSINNG_CVV_DR")
Country_Txn_Data_1_sub$Hotlist_Rule_Name = ''
}else{
Country_Txn_Data_11 <- anti_join(Country_Txn_Data_1, Country_Txn_Data_1_sub, by = "FI_TRANSACTION_ID")

Country_Txn_Data_1_sub$Hotlist_Rule_Name = 'HR_COUNTRY_GH_MISSINNG_CVV_DR'

Country_Txn_Data_1 <- bind_rows(Country_Txn_Data_11, Country_Txn_Data_1_sub)
}


# Head of Country_Txn_Data_1
# Dimension of Country_Txn_Data_1 and Country_Txn_Data_11 must match
head(Country_Txn_Data_1)
dim(Country_Txn_Data_1)

##########################################################################################


# Debit and Credit Data segregating
Country_Txn_Data_DB <- Country_Txn_Data_1[which(str_sub(Country_Txn_Data_1$CRD_CLNT_ID,-3) == '_DB'), ]
Country_Txn_Data_CR <- Country_Txn_Data_1[which(str_sub(Country_Txn_Data_1$CRD_CLNT_ID,-3) == '_CR'), ]
