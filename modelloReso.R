### Connecting to MS Sql Server - Shared Tables
library(RODBC)
library(tidyverse)
library(caret)
library(h2o)
library(easyRFM) ### installare da comando: 
### install.packages("devtools")
### devtools::install_github("hoxo-m/easyRFM")

conn <- odbcDriverConnect(connection="Driver={SQL Server Native Client 11.0};server=YAPP14;database=SharedTables;trusted_connection=yes;")

### Query
queryResi <- c("SELECT
               SUBSTRING(Codice12, 1, 11) C10, 
               FlagReso,
               OrderValue,
               PotentialOrderValue,
               dph.Micro,
               dph.Macro,
               s.DescrizioneAbbreviata_EN as sessoItem,
               TipoSpedizione_ID_Cliente,
               TipoPagamento_ID,
               o.Nazioni_ID,
               PaymentType_ID,
               Freeshipping_ID,
               FlagRegalo,
               d.Descrizione,
               Utenti_ID,
               CASE WHEN utenti_sesso = 'F' THEN 'W'
               ELSE utenti_sesso END as sessoUtenti,
               utenti_mail,
               utenti_mail_sha256,
               ANNO,
               MESE,
               GIORNO,
               Data
               FROM [SharedTables].[ods].[RigheOrdini] ro 
               LEFT JOIN [SharedTables].[ods].[Divisione] d
               ON Divisione_Acquisto_ID = d.ID_Divisione
               LEFT JOIN [SharedTables].[ods].[Ordini] as o
               ON Ordini_ID = o.ID_Ordini
               LEFT JOIN [SharedTables].ods.Data
               ON Data_ID = ID_Data
               LEFT JOIN [SharedTables].ods.Utenti
               ON Utenti_ID = ID_Utenti
               LEFT JOIN SharedTables.ods.Dim_Product_Hier dph
               ON ro.Articolo_ID = dph.Articolo_ID
               LEFT JOIN SharedTables.ods.Sesso s
               ON Sesso_ID = s.ID_Sesso
               WHERE d.Descrizione LIKE '%ARMANI%' AND ANNO = '2017'")


queryRFM <- c("SELECT
	            Data,
              NumeroOrdine,
              utenti_mail_sha256,
              Gross_Sales
              
              FROM
              [SharedTables].[ods].[Ordini] as o
              LEFT JOIN 
              [SharedTables].[ods].[Utenti]
              ON Utenti_ID = ID_Utenti
              LEFT JOIN 
              [SharedTables].[ods].[Divisione] as d
              ON o.divisione_id = d.ID_Divisione
              LEFT JOIN
              [SharedTables].[ods].[Data]
              ON Data_ID = ID_Data
              WHERE CAST(Anno as INT) > 2014 AND Descrizione LIKE '%ARMANI%' AND Gross_Sales > 0
              ")

### Executing Query
resi <- sqlQuery(conn, queryResi)
acquisti <- sqlQuery(conn, queryRFM)

### Preprocessing Resi
resi <- resi %>% mutate_if(is.factor, as.character) %>% 
                 mutate(FlagReso = as.factor(FlagReso),
                      sessoItem = str_trim(sessoItem),
                      corrispSessi = ifelse(sessoUtenti == ' ' | sessoUtenti == 'N', 'unknown', 
                                            ifelse(sessoUtenti == sessoItem, 'yes', 'no')),
                      Sconto = 1 - OrderValue/PotentialOrderValue,
                      TipoSpedizione_ID_Cliente = as.factor(TipoSpedizione_ID_Cliente),
                      TipoPagamento_ID = as.factor(TipoPagamento_ID),
                      Nazioni_ID = as.factor(Nazioni_ID),
                      PaymentType_ID = as.factor(PaymentType_ID),
                      Freeshipping_ID = as.factor(Freeshipping_ID),
                      FlagRegalo = as.factor(FlagRegalo)
                      ) %>% mutate_if(is.character, as.factor)

### Preprocessing Acquisti
acquisti <- acquisti %>% mutate_if(is.factor, as.character)

### Building RFM features
rfmResult <- rfm_auto(acquisti, id = 'utenti_mail_sha256', payment = 'Gross_Sales', date = 'Data')

### Joining RFM features to Return DF
resi <- resi %>% left_join(rfmResult$rfm)

### Selecting Features
train <- resi %>% select(FlagReso, OrderValue, Macro, sessoItem, sessoUtenti, corrispSessi, TipoSpedizione_ID_Cliente, PaymentType_ID, Freeshipping_ID, FlagRegalo, Sconto)


### Starting h2o
h2o.init(nthreads = 3)

### Creating train and test frame
set.seed(3456)
trainIndex <- createDataPartition(train$FlagReso, p = .9, 
                                  list = FALSE, 
                                  times = 1)

train90 <- train[ trainIndex,]
test10  <- train[-trainIndex,]

train.h2o <- as.h2o(train90)
test.h2o <- as.h2o(test10)

y = 'FlagReso'
x = setdiff(names(train.h2o), y)
h2o.rf <- h2o.randomForest(x = x, y = y, training_frame = train.h2o, validation_frame = test.h2o, balance_classes = TRUE, seed = 1234, ntrees = 100, nfolds = 5, stopping_metric = 'RMSLE')
h2o.automl <- h2o.automl(x = x, y = y, training_frame = train.h2o, validation_frame = test.h2o, nfolds = 5, max_runtime_secs = 600)

###h2o.rf
### AUC 0.767 on test set

confusionMatrix(as.vector(as.factor(ifelse(predict(h2o.rf
                                                   , test.h2o)[,3] < 0.135070, 0, 1))), test10$FlagReso, positive = '1')
View(train90)

### Closing connection
odbcClose(conn)
