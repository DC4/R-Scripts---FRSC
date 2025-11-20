Country_Txn_Data_1 <- Country_Txn_Data_DB

BLACKLISTED_MERCHANTS <- read_excel("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/Hotlist/BLACKLISTED_MERCHANTS_HK.xls")

Country_Txn_Data_1_Backup <- Country_Txn_Data_1
BLACKLISTED_MERCHANTS <- data.table(BLACKLISTED_MERCHANTS)
Country_Txn_Data_2 <- data.table(Country_Txn_Data_1)

Name_list <- sqldf("Select Name from BLACKLISTED_MERCHANTS")

Country_Txn_Data_3 <- sqldf("Select * from Country_Txn_Data_2 where trim(mer_id) in Name_list")

Country_Txn_Data_1 <- Country_Txn_Data_3