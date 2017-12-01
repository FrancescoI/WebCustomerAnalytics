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
    	     Ordini_ID,
           SUBSTRING(Codice12, 0, 11) as C10,
           DataReso_ID,
           FlagReso,
           OrderValue,
           PotentialOrderValue,
           ScontoPerc,
           MicroMotivo_ID,
           dph.Micro,
           dph.Macro,
           Sesso_ID,
           Divisione_Acquisto_ID,
           Descrizione,
           Data_ID,
           Utenti_ID,
           utenti_mail,
           utenti_mail_sha256,
           ANNO,
           MESE,
           GIORNO
           FROM [SharedTables].[ods].[RigheOrdini] ro 
           LEFT JOIN [SharedTables].[ods].[Divisione]
           ON Divisione_Acquisto_ID = ID_Divisione
           LEFT JOIN [SharedTables].[ods].[Ordini]
           ON Ordini_ID = ID_Ordini
           LEFT JOIN [SharedTables].ods.Data
           ON Data_ID = ID_Data
           LEFT JOIN [SharedTables].ods.Utenti
           ON Utenti_ID = ID_Utenti
           LEFT JOIN SharedTables.ods.Dim_Product_Hier dph
           ON ro.Articolo_ID = dph.Articolo_ID
           WHERE Descrizione = 'ARMANICOM' AND ANNO = '2017' AND CAST(MESE as INT) > 6")


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
              WHERE CAST(Anno as INT) > 2015 AND Descrizione LIKE '%ARMANI%' AND Gross_Sales > 0
              ")

### Executing Query
resi <- sqlQuery(conn, queryResi)
acquisti <- sqlQuery(conn, queryRFM)

### Preprocessing Resi
resi <- resi %>% mutate(FlagReso = as.factor(FlagReso), 
                      Sconto = 1 - OrderValue/PotentialOrderValue,
                      Sesso_ID = as.factor(Sesso_ID))

### Preprocessing Acquisti
acquisti <- acquisti %>% mutate_if(is.factor, as.character)

### Building RFM features
rfmResult <- rfm_auto(acquisti, id = 'utenti_mail_sha256', payment = 'Gross_Sales', date = 'Data')

### Joining RFM features to Return DF
resi <- resi %>% left_join(rfmResult$rfm)

### Selecting Features
train <- resi %>% select(FlagReso, OrderValue, Macro, Sconto, Frequency, Monetary, RecencyClass, FrequencyClass, MonetaryClass)


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
h2o.rf <- h2o.randomForest(x = x, y = y, training_frame = train.h2o, validation_frame = test.h2o, balance_classes = TRUE, seed = 1234, ntrees = 200)

h2o.rf
### AUC 0.87 on test set

### Closing connection
odbcClose(conn)
