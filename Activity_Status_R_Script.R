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
library(readxl)

df = read.xlsx("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Ajanta/Daily_Activity_Check_Output.xlsx")

df1 <- print(xtable(df, align="ccccccccccccccccccccccccccccccccccc"), # align columns
   type = "html", include.rownames = T, print.results=FALSE)
   
Incident = read_xlsx("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Ajanta/Daily_Activity_Check_Output.xlsx", sheet = "Incident", col_types=c(Application='text', Incident_Details='text', Date='date', SPOC='text'))

Incident$Date = as.character(Incident$Date)

Incident1 = print(xtable(Incident, align="ccccc"), # align columns
   type = "html", include.rownames = T, print.results=FALSE)
   
Ticket = read_xlsx("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Ajanta/Daily_Activity_Check_Output.xlsx", sheet = "Tickets", col_types=c(Application='text', Ticket_Details='text', Date='date', SPOC='text'))

Ticket$Date = as.character(Ticket$Date)

Ticket1 = print(xtable(Ticket, align="ccccc"), # align columns
   type = "html", include.rownames = T, print.results=FALSE)

df2 <- paste0("Hi All,<br>", "<br>Please find below the Daily Activity Tracker Details along with the Crucial Monthly Incident and Ticket Details : <br><br>", "<html>", df1, "<br><br>" , Incident1, "<br><br>" , Ticket1, "</html>", "<br><br>SPOC Names:<br>",

"<br>DC : Dinesh Charan
<br>ST : Srinadh
<br>SK : Saravanan
<br>AD : Ajanta
<br>KM : Manjunath
<br>AI : Anish
<br>HD : Harish
<br>AB : Abhishek
<br>JK : Janak
<br>VS : Vidhya<br>","<br>Thanks & Regards","<br>FRSC_MIS")

dt <- Sys.Date()
send.mail(from="mis.frsc@sc.com",
	  #to=c("Dinesh.Charan@sc.com"),
          to=c("Saravanakumar.K@sc.com","Manjunath.Km@sc.com","HarishB.Darshankar@sc.com","Srinadhareddy.Tatiparthi@sc.com","Abhishek.Bhatia1@sc.com","Anishramkrishnan.Iyer@sc.com","Ajanta.Dhilipkumarbehera@sc.com","JanakDinesh.Mundada@sc.com","BonuVidhya.Sahiti@sc.com","Dinesh.Charan@sc.com"),
          subject=paste0("Daily Activities Status tracker as on ",dt),
          body=df2,
          html=TRUE,
          smtp=list(host.name = "inmail01apps.zone1.scb.net",
                    port = 25,
                    user.name = "mis.frsc@sc.com",
                    ssl = F),
          authenticate=F,
          send = TRUE
)
