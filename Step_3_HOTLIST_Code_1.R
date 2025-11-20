BLACKLISTED_MERCHANTS_DEBIT <- read_excel("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/R_Simulation/Hotlist/BLACKLISTED_MERCHANTS_DEBIT.xls")

BLACKLISTED_MERCHANTS_DEBIT <- data.table(BLACKLISTED_MERCHANTS_DEBIT)
Country_Txn_Data_2 <- data.table(Country_Txn_Data_1)

CNTY_BLACKLISTED_MERCHANTS_DEBIT <- BLACKLISTED_MERCHANTS_DEBIT
CNTY_BLACKLISTED_MERCHANTS_DEBIT <- NULL
Country_Txn_Data_3 <- BLACKLISTED_MERCHANTS_DEBIT
Country_Txn_Data_3 <- NULL

Country_list <- list('AE', 'BH', 'BN', 'BW', 'CI', 'CM', 'GH', 'GM', 'IN', 'JO', 'KE', 'LK', 'MY', 'NG', 'NP', 'QA', 'SG', 'SL', 'TZ', 'UG', 'VN', 'ZM', 'ZW')

Country_list <- list('AE')

Unique_Country_Data_fnc <- function(x) {
for (i in 1:length(Country_list)) {

		TEMP_BLACKLISTED_MERCHANTS_DEBIT_1 <- sqldf(paste0("Select * from BLACKLISTED_MERCHANTS_DEBIT where UPPER(Value) like '%", noquote(Country_list[[i]]), "%'"))
		
		TEMP_BLACKLISTED_MERCHANTS_DEBIT_2 <- sqldf(paste0("Select * from TEMP_BLACKLISTED_MERCHANTS_DEBIT_1 B Left join Country_Txn_Data_2 A on trim(A.mer_id) = B.Name where UPPER(A.cnty) = ", "'",(Country_list[[i]]), "'"))
		
		Country_Txn_Data_3 <- rbind(Country_Txn_Data_3, TEMP_BLACKLISTED_MERCHANTS_DEBIT_2)

	}
}

require(compiler)
# enableJIT(3)
# help(cmpfun)
Unique_Country_Data_fnc <- cmpfun(Unique_Country_Data_fnc)
Unique_Country_Data_fnc(Country_list)