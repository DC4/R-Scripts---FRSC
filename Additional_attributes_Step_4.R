			#**************************************************************************************
			
									       #### Additional attributes ####
			
			#**************************************************************************************

Country_Txn_Data_1$USR_IND_4_New <- str_sub(Country_Txn_Data_1$USR_IND_4,1,2)
Country_Txn_Data_1$USR_IND_4_New_1 <- str_sub(Country_Txn_Data_1$USR_IND_4,3,4)
Country_Txn_Data_1$ACCT_NBR_New <- str_sub(Country_Txn_Data_1$ACCT_NBR,1,1)
Country_Txn_Data_1$USER_DATA_4_STRG_New <- str_sub(Country_Txn_Data_1$USER_DATA_4_STRG, 1, 11)
Country_Txn_Data_1$USR_DAT_2_New <- str_sub(Country_Txn_Data_1$USR_DAT_2, 9, 1)
Country_Txn_Data_1$USR_DAT_1_New <- str_sub(Country_Txn_Data_1$USR_DAT_1, 9, 2)
# Transaction data
# head(Country_Txn_Data_1)