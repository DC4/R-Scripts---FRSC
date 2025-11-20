.libPaths("C:/FRSC/R_Packages1")
rm(list=ls())
library(mailR)
library(xml2)
library(data.table)
library(sqldf)
library(DBI)
library(dplyr)
library(RCurl)
library(rio)
library(xtable)
library(openxlsx)
library(RDCOMClient)
library(XLConnect)

# Fetching Data
wb <- loadWorkbook("//inhadfil101.in.standardchartered.com/wkgrps5/RAC_IN_MIS/Sumeet/Rules_Coding_tracker_for_SAS_Simulation_2021.xlsx", password="Bang@Rac")
df <- readWorksheet(wb, "2022")

# Data analysis
#head(df)

# Dropping the 1st column
df$S.No.<- NULL

# Date change
#Character format
df$Deploy.date <- substr(df$Deploy.date, 1, 10)

#Date format
df$Deploy.date <- as.Date(df$Deploy.date)

# Declaring compare date
Date_Compare <- Sys.Date()-3

# Obtaining rules coded GE 3days before:
df1 <- df[df$Deploy.date >= Date_Compare, ]

# Change Date to character format for display
df1$Deploy.date <- as.character(df1$Deploy.date)
df1$Completed..SAS.Simulation <- as.character(df1$Completed..SAS.Simulation)

# Type 2 rules deployed on Wednesdays
Date_Compare_1 <- Sys.Date()- 5
df2 <- df[df$Deploy.date >= Date_Compare_1, ]
df2$Deploy.date <- as.character(df2$Deploy.date)
df2$Completed..SAS.Simulation <- as.character(df2$Completed..SAS.Simulation)
df3 = subset(df2, is.na(df2$Completed.By))

# Subsetting for "Completed..SAS.Simulation" columns as Blank for the past 5 days
df4 = subset(df2, is.na(df2$Completed..SAS.Simulation))
df4$Deploy.date <- as.character(df4$Deploy.date)
df4$Completed..SAS.Simulation <- as.character(df4$Completed..SAS.Simulation)

# Fetch if "Completed..SAS.Simulation" is empty for final data
df4_1 <- subset(df, is.na(df$Completed..SAS.Simulation))
df4_1$Deploy.date <- as.character(df4_1$Deploy.date)
df4_1$Completed..SAS.Simulation <- as.character(df4_1$Completed..SAS.Simulation)

# Combining Rule simulations pending and not completed
df5 = rbind(df1,df3,df4,df4_1)

# Dropping not needed columns
df6 = subset(df5, select = -c(Ageing,Description))

# Eliminating duplicates
df6 = sqldf('SELECT DISTINCT "Rule.Name",Tenant,Project,Type,"Deploy.date",Jack,Author,"Completed.By","Completed..SAS.Simulation","Modification.Deletion.Deactivation","Comments" FROM df6;')

# Fetch results only if "Completed..SAS.Simulation" is empty for final data
df6 <- subset(df6, is.na(df6$Completed..SAS.Simulation))

# Drop row if Rule Name is #NA
df6 <- df6[!is.na(df6$Rule.Name), ]

# Identify if data dataframe is empty or not
if(nrow(df6) == 0)
{
df8 <- paste0("Dear All,<br>", "<br>No pending SAS simulations for today.<br>", 
"<br>Thanks & Regards","<br>FRSC_MIS")
}else
{
df6$No.<-1:nrow(df6)

# Rearranging columns
df6 <- df6[, c(12, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)]

# html format conversion
df7 <- print(xtable(df6, align="ccccccccccccc"), # align columns
   type = "html", include.rownames = F, print.results=FALSE)
   
df8 <- paste0("Dear All,<br>", "<br>Below rules are pending for SAS simulation.<br><br>", "<html>", df7, "</html>", 
"<br>Thanks & Regards","<br>FRSC_MIS")
}

# Mailing to team
dt <- Sys.Date()
send.mail(from="mis.frsc@sc.com",
	   #to=c("Dinesh.Charan@sc.com"),
	   #cc=c("Dinesh.Charan@sc.com"),
           to=c("Subramanian.Paramasivam@sc.com","Prakash.C@sc.com","Parthasarathi.KP@sc.com","Manjunath.Km@sc.com","HarishB.Darshankar@sc.com","Srinadhareddy.Tatiparthi@sc.com","Abhishek.Bhatia1@sc.com","JanakDinesh.Mundada@sc.com","BonuVidhya.Sahiti@sc.com","Dinesh.Charan@sc.com"),
           cc = c("Saravanakumar.K@sc.com","Anishramkrishnan.Iyer@sc.com","Ajanta.Dhilipkumarbehera@sc.com","JohnPaulRaja.A@sc.com"),
		  subject=paste0("SAS Simulation Pending rules as on ",dt),
          body=df8,
          html=TRUE,
          smtp=list(host.name = "inmail01apps.zone1.scb.net",
                    port = 25,
                    user.name = "mis.frsc@sc.com",
                    ssl = F),
          authenticate=F,
          send = TRUE
)
