			#**************************************************************************************
			
										     ### MAILING TO TEAM ####
			
			#**************************************************************************************

# html format conversion
df7 <- print(xtable(output, align="ccccccccccc"), # align columns
   type = "html", include.rownames = F, print.results=FALSE)
   
df8 <- paste0("Dear All,<br>", "<br>Please find below the R simulation results for today:<br><br>", "<html>", df7, "</html>", 
"<br>Thanks & Regards","<br>Vidhya")

# Mailing to Team
dt <- Sys.Date()
send.mail(from="mis.frsc@sc.com",
		    to=c("BonuVidhya.Sahiti@sc.com"),
		    cc=c("BonuVidhya.Sahiti@sc.com"),
            #cc = c("Srinadhareddy.Tatiparthi@sc.com","Abhishek.Bhatia1@sc.com","Ajanta.Dhilipkumarbehera@sc.com","JanakDinesh.Mundada@sc.com","BonuVidhya.Sahiti@sc.com","Dinesh.Charan@sc.com"),
            #to = c("Saravanakumar.K@sc.com","Anishramkrishnan.Iyer@sc.com"),
		  subject=paste0("R Simulation results as on ",dt),
          body=df8,
          html=TRUE,
          smtp=list(host.name = "inmail01apps.zone1.scb.net",
                    port = 25,
                    user.name = "mis.frsc@sc.com",
                    ssl = F),
          authenticate=F,
          send = TRUE
)