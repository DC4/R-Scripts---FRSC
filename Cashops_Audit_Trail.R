
.libPaths("C:/FRSC/R_Packages1")

rm(list=ls())
library(xml2)
library(data.table)
library(sqldf)
library(DBI)
library(dplyr)
library(RCurl)
library(rio)

rundt2 <- format(as.Date(Sys.Date() -0), format = "%d%b%Y")

xmllist <- c(list.files(paste0("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Cashops/",rundt2,collapse = ""),pattern=".xml",recursive=T,full.names=T))

xmllist2 <- c(list.files(paste0("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Cashops/",rundt2,collapse = ""),pattern="_attbs",recursive=T,all.files = F,full.names=T))


xmllist_new <- data.frame(unlist(xmllist))

names(xmllist_new)[1] <- "Filename"


xmllist2_new <- data.frame(unlist(xmllist2))

names(xmllist2_new)[1] <- "Filename"

Final_list <-  sqldf("select * from xmllist_new where Filename not in (select Filename from xmllist2_new) and filename like '%rbtran21%'")


class(Final_list)

f_n1 <- unlist(Final_list,use.names = F)
class(f_n1)
for ( f in f_n1)
{

  
  filen <- paste0(f)
  print(filen)

  chk1 <- match(f,f_n1)
  

  xml_file <- read_xml(filen,"text")

 
 VersionElement <- xml_find_all(xml_file, "//VersionElement")
 VersionId <- xml_attr(VersionElement, "VersionId")
 
 ItemName <- xml_text(xml_find_all(xml_file, "//ItemName"))
 IsEntryDeleted <- xml_text(xml_find_all(xml_file, "//IsEntryDeleted"))
 User <- xml_text(xml_find_all(xml_file, "//User"))
 Comment <- xml_text(xml_find_all(xml_file, "//Comment"))
 Timestamp <- xml_text(xml_find_all(xml_file, "//Timestamp"))
 
 
 xml_data_frame = data.frame(ItemName = ItemName,
                             IsEntryDeleted = IsEntryDeleted,
                             VersionId = VersionId,
                             User = User,
                             Comment = Comment,
                             Timestamp = Timestamp)
 
 
 
 xml_data_frame$Filename <- paste0(f,collapse="")
    
    if (chk1 ==1) 
    { 
      all_data <- subset(xml_data_frame, FALSE)
    }
    
    all_data <- rbind(all_data,xml_data_frame)
    

}
all_data1 <- sqldf("select Timestamp,Comment,User,substr(Filename,93,100) as Rulename,substr(Timestamp,1,11) as date1 from all_data")



all_data1 <- sqldf("select Timestamp,Comment,User,substr(Filename,93,100) as Rulename,substr(Timestamp,1,11) as date1,
                   substr(Filename,77,6) as Tenant, substr(Filename,67,9) as running_dt from all_data")


#all_data_g <- all_data1[ which(Tenant=='Global'),]


all_data_gb_1 <- subset(all_data1, Tenant == 'Global')
all_data_gp1_1 <- subset(all_data1, Tenant == 'Group1')
all_data_gp2_1 <- subset(all_data1, Tenant == 'Group2')
all_data_rth_1 <- subset(all_data1, Tenant == 'Reauth')
all_data_bd_1 <- subset(all_data1, Tenant == 'BNGDSH')
all_data_id_1 <- subset(all_data1, Tenant == 'INDNSA')
all_data_ph_1 <- subset(all_data1, Tenant == 'PHLPNS')

all_data_gb <- all_data_gb_1[-7]
all_data_gp1 <- all_data_gp1_1[-7]
all_data_gp2 <- all_data_gp2_1[-7]
all_data_rth <- all_data_rth_1[-7]
all_data_bd <- all_data_bd_1[-7]
all_data_id <- all_data_id_1[-7]
all_data_ph <- all_data_ph_1[-7]

export(all_data_gb,paste0("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Cashops/Output/Cashops_Audit_gb_",rundt2,".xlsx",collapse=""))
export(all_data_gp1,paste0("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Cashops/Output/Cashops_Audit_gp1_",rundt2,".xlsx",collapse=""))
export(all_data_gp2,paste0("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Cashops/Output/Cashops_Audit_gp2_",rundt2,".xlsx",collapse=""))
export(all_data_rth,paste0("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Cashops/Output/Cashops_Audit_rth_",rundt2,".xlsx",collapse=""))
export(all_data_bd,paste0("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Cashops/Output/Cashops_Audit_bd_",rundt2,".xlsx",collapse=""))
export(all_data_id,paste0("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Cashops/Output/Cashops_Audit_id_",rundt2,".xlsx",collapse=""))
export(all_data_ph,paste0("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Cashops/Output/Cashops_Audit_ph_",rundt2,".xlsx",collapse=""))


library(mailR)

source("C:/FRSC/R_Codes_Prod/ZfileR.R")

compress7z("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Cashops/Output/",paste0("Cashops_Audit_",rundt2,".zip",collapse=""),paste0("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Cashops/Output/Cashops_Audit_gb_",rundt2,".xlsx",collapse=""),"scb123")
compress7z("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Cashops/Output/",paste0("Cashops_Audit_",rundt2,".zip",collapse=""),paste0("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Cashops/Output/Cashops_Audit_gp1_",rundt2,".xlsx",collapse=""),"scb123")
compress7z("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Cashops/Output/",paste0("Cashops_Audit_",rundt2,".zip",collapse=""),paste0("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Cashops/Output/Cashops_Audit_gp2_",rundt2,".xlsx",collapse=""),"scb123")
compress7z("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Cashops/Output/",paste0("Cashops_Audit_",rundt2,".zip",collapse=""),paste0("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Cashops/Output/Cashops_Audit_rth_",rundt2,".xlsx",collapse=""),"scb123")
compress7z("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Cashops/Output/",paste0("Cashops_Audit_",rundt2,".zip",collapse=""),paste0("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Cashops/Output/Cashops_Audit_bd_",rundt2,".xlsx",collapse=""),"scb123")
compress7z("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Cashops/Output/",paste0("Cashops_Audit_",rundt2,".zip",collapse=""),paste0("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Cashops/Output/Cashops_Audit_id_",rundt2,".xlsx",collapse=""),"scb123")
compress7z("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Cashops/Output/",paste0("Cashops_Audit_",rundt2,".zip",collapse=""),paste0("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Cashops/Output/Cashops_Audit_ph_",rundt2,".xlsx",collapse=""),"scb123")


send.mail(from="mis.frsc@sc.com",
          to=c("JanakDinesh.Mundada@sc.com","Saravanakumar.K@sc.com","Anishramkrishnan.Iyer@sc.com","Ajanta.Dhilipkumarbehera@sc.com","Srinadhareddy.Tatiparthi@sc.com","BonuVidhya.Sahiti@sc.com","Dinesh.Charan@sc.com"),
          subject=paste0("UAT_Cashops_Audit  - ",rundt2,collapse=""),
          body="Hi All,<br> <br> Please find attached status for today.<br> <br> Thanks <br> FRSC_MIS",
          html=T,
          smtp=list(host.name = "inmail01apps.zone1.scb.net",
                    port = 25,
                    user.name = "mis.frsc@sc.com",
                    ssl = F),
          authenticate=F,
	    attach.files=(paste0("//inhadfil101.in.standardchartered.com/wkgrps7/RAC_IN_MIS/Cashops/Output/Cashops_Audit_",rundt2,".zip",collapse=""))
)

#.libPaths("C:/FRSC/R-3.5.2/library")
#.libPaths("C:/FRSC/R_Packages1")

