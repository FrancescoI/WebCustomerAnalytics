### Connecting to MS Sql Server - Shared Tables
library(RODBC)
library(tidyverse)
library(caret)
library(h2o)
###library(easyRFM) ### installare da comando: 
### install.packages("devtools")
### devtools::install_github("hoxo-m/easyRFM")

conn <- odbcDriverConnect(connection="Driver={SQL Server Native Client 11.0};server=YAPP14;database=SharedTables;trusted_connection=yes;")

queryResi <- c("WITH temp as (          
        		           SELECT
                Codice12,
                SUBSTRING(Codice12, 1, 10) C10,
                SUBSTRING(Codice12, 1, 8) C8,
                SUBSTRING(Codice12, 11, 12) Variant,
                FlagReso,
                NumeroOrdine,
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
                TipoUtenti_ID,
                Flag_Vip,
                CASE WHEN utenti_sesso = 'F' THEN 'W'
                ELSE utenti_sesso END as sessoUtenti,
                utenti_mail,
                utenti_mail_sha256,
                ANNO,
                MESE,
                GIORNO,
                Data,
                ROW_NUMBER() OVER(PARTITION BY NumeroOrdine ORDER BY NumeroOrdine) as articoliInOrder
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
                WHERE d.Descrizione LIKE'%MARNI%' AND ANNO = '2017' AND MESE != '12' AND o.Stato != '7' 
),
                
                
                temp2 as (          
                SELECT
                SUBSTRING(Codice12, 1, 10) C10,
                NumeroOrdine,
                COUNT(SUBSTRING(Codice12, 1, 10)) as countC10
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
                WHERE d.Descrizione LIKE'%MARNI%' AND ANNO = '2017' AND MESE != '12' AND o.Stato != '7'
                GROUP BY SUBSTRING(Codice12, 1, 10), NumeroOrdine
                )

                SELECT 
                temp.*,
                MAX(temp.articoliInOrder) OVER(PARTITION BY temp.NumeroOrdine) as nrArticoli,
                CASE WHEN temp2.countC10 > 1 THEN '1' ELSE '0' END as hasMoreC10
                FROM temp
                LEFT JOIN temp2
                ON temp.NumeroOrdine = temp2.NumeroOrdine
                ")


queryAcquisti <- c("WITH temp AS(

                   SELECT
                   Data,
                   NumeroOrdine,
                   utenti_mail_sha256,
                   Promocodes_ID,
				           Device_ID,
                   ROW_NUMBER() OVER(PARTITION BY utenti_mail_sha256 ORDER BY Data) as tranNumber,
                   MAX(ro.FlagReso) as hasReturn
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
                   WHERE d.Descrizione = 'XY MARNI' AND o.Stato != '7'
                   GROUP BY NumeroOrdine, utenti_mail_sha256, Data, Promocodes_ID, Device_ID)
                   
                   SELECT 
                   t.Data,
                   t.NumeroOrdine,
                   t.utenti_mail_SHA256,
                   t.tranNumber,
                   t.Device_ID, 
                   t.Promocodes_ID,
                   t.cumReturn - t.hasReturn AS cumReturn FROM
                   (SELECT
                   t1.Data,
                   t1.utenti_mail_sha256,
                   t1.NumeroOrdine,
                   t1.Promocodes_ID,
                   t1.Device_ID,
                   t1.tranNumber,
                   t1.hasReturn,
                   SUM(t2.hasReturn) as cumReturn
                   FROM temp t1 INNER JOIN temp t2 on t1.tranNumber >= t2.tranNumber AND t1.utenti_mail_sha256 = t2.utenti_mail_sha256
                   GROUP BY t1.utenti_mail_sha256, t1.Data, t1.NumeroOrdine, t1.tranNumber, t1.hasReturn, t1.Promocodes_ID, t1.Device_ID
                   ) t
              ")

### Executing Query
resi <- sqlQuery(conn, queryResi)
acquisti <- sqlQuery(conn, queryAcquisti)

### Gathering daysSinceLastPurchase Feature
acquisti %>% arrange(utenti_mail_SHA256, Data) %>% 
  mutate(lastPurchase = case_when(tranNumber == 1 ~ Data, TRUE ~ lag(Data, order_by = utenti_mail_SHA256))) %>%
  mutate(daysSinceLastPurchase = (Data - lastPurchase)/(3600*24)) -> acquisti

acquisti %>% select(Data, utenti_mail_SHA256) %>% group_by(utenti_mail_SHA256) %>% 
  summarise(firstPurchase = min(Data)) %>% right_join(acquisti) %>% 
  mutate(daysSinceFirstPurchase = (Data - firstPurchase)/(3600*24)) %>% 
  mutate(daysSinceFirstPurchase = as.integer(daysSinceFirstPurchase)) -> acquisti

acquisti %>% rename(utenti_mail_sha256 = utenti_mail_SHA256) -> acquisti

### Preprocessing Resi
resi2 <- resi %>% left_join(acquisti, by = 'NumeroOrdine') %>% 
                 select(-Data.y, -utenti_mail_sha256.y) %>% 
                 mutate_if(is.factor, as.character) %>% 
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
                      FlagRegalo = as.factor(FlagRegalo),
                      isFirst = as.factor(ifelse(tranNumber == 1, 1, 0)),
                      TipoUtenti_ID = as.factor(TipoUtenti_ID)
                      ) %>% 
                  mutate_if(is.factor, as.character) %>% 
                  mutate(NumeroOrdine = str_trim(as.character(NumeroOrdine)),
                                                 hasDoneFirstReturn = ifelse(cumReturn > 0, 1, 0),
                         Device_ID = factor(Device_ID)
                         )


### If Behavioral data available
resi2 %>% left_join(query_res, by = c('utenti_mail_sha256.x' = 'emailHashed', 'NumeroOrdine' = 'transactionId')) -> resi2


### Selecting Features with Behavioral
### train <- resi2 %>% select(Data.x, NumeroOrdine, C10, FlagReso, Nazioni_ID, OrderValue, Macro, sessoItem, sessoUtenti, corrispSessi, TipoSpedizione_ID_Cliente, PaymentType_ID, Freeshipping_ID, FlagRegalo, Sconto, tranNumber, daysSinceLastPurchase, daysSinceFirstPurchase, isFirst, nrArticoli, TipoUtenti_ID, hasMoreC10, cumReturn, hasDoneFirstReturn, Device_ID, zoomOpenCount, returnPolicyCount) %>% mutate_if(is.character, as.factor) %>% mutate(daysSinceLastPurchase = as.integer(daysSinceLastPurchase))

### Selecting Features without Behavioral
train <- resi2 %>% select(Data.x, NumeroOrdine, C10, FlagReso, Nazioni_ID, OrderValue, Macro, sessoItem, sessoUtenti, corrispSessi, TipoSpedizione_ID_Cliente, PaymentType_ID, Freeshipping_ID, FlagRegalo, Sconto, tranNumber, daysSinceLastPurchase, daysSinceFirstPurchase, isFirst, nrArticoli, TipoUtenti_ID, hasMoreC10, cumReturn, hasDoneFirstReturn, Device_ID) %>% mutate_if(is.character, as.factor) %>% mutate(daysSinceLastPurchase = as.integer(daysSinceLastPurchase))

### Starting h2o
h2o.init(nthreads = 3)

### Creating train and test frame
set.seed(1234)
trainIndex <- createDataPartition(train$FlagReso, times = 1, p = 0.9, list = FALSE)

train90 <- train[trainIndex, ]
test10  <- train[-trainIndex, ]

train.h2o <- train90 %>% select(-Data.x, -NumeroOrdine, -C10) %>% as.h2o()
test.h2o <- test10 %>% select(-Data.x) %>% as.h2o()

y = 'FlagReso'
##z = c('NumeroOrdine.y', 'C10.y')
x = setdiff(names(train.h2o), y)
##x = setdiff(x, z)
h2o.rf <- h2o.randomForest(x = x, y = y, training_frame = train.h2o, validation_frame = test.h2o, balance_classes = TRUE, seed = 1234, ntrees = 50, stopping_metric = 'AUTO', binomial_double_trees = TRUE)
###h2o.automl <- h2o.automl(x = x, y = y, training_frame = train.h2o, validation_frame = test.h2o, max_runtime_secs = 600)

###h2o.rf
### AUC 0.767 on test set

trainRandomColumn <- function(data) {
  
  set.seed(1234)
  trainIndex <- createDataPartition(data$FlagReso, times = 1, p = 0.9, list = FALSE)
  
  train90 <- data[trainIndex, ]
  test10  <- data[-trainIndex, ]
  
  train.h2o <- train90 %>% select(-Data.x, -NumeroOrdine, -C10) %>% as.h2o()
  test.h2o <- test10 %>% select(-Data.x) %>% as.h2o()
  
  y = 'FlagReso'
  ##z = c('NumeroOrdine.y', 'C10.y')
  x = setdiff(names(train.h2o), y)
  ##x = setdiff(x, z)
  h2o.rf <- h2o.randomForest(x = x, y = y, training_frame = train.h2o, validation_frame = test.h2o, balance_classes = TRUE, seed = 1234, ntrees = 50, stopping_metric = 'AUTO', binomial_double_trees = TRUE)
  ###h2o.automl <- h2o.automl(x = x, y = y, training_frame = train.h2o, validation_frame = test.h2o, max_runtime_secs = 600)
  perf1 <- max(h2o.rf@model$validation_metrics@metrics$thresholds_and_metric_scores$f1)
  
  data2 <- data %>% mutate(returnPolicyCount = sample(returnPolicyCount, replace = FALSE),
                           zoomOpenCount = sample(zoomOpenCount, replace = FALSE))
  set.seed(1234)
  trainIndex <- createDataPartition(data2$FlagReso, times = 1, p = 0.9, list = FALSE)
  
  train90 <- data2[trainIndex, ]
  test10  <- data2[-trainIndex, ]
  
  train.h2o <- train90 %>% select(-Data.x, -NumeroOrdine, -C10) %>% as.h2o()
  test.h2o <- test10 %>% select(-Data.x) %>% as.h2o()
  
  y = 'FlagReso'
  ##z = c('NumeroOrdine.y', 'C10.y')
  x = setdiff(names(train.h2o), y)
  ##x = setdiff(x, z)
  h2o.rf2 <- h2o.randomForest(x = x, y = y, training_frame = train.h2o, validation_frame = test.h2o, balance_classes = TRUE, seed = 1234, ntrees = 50, stopping_metric = 'AUTO', binomial_double_trees = TRUE)
  ###h2o.automl <- h2o.automl(x = x, y = y, training_frame = train.h2o, validation_frame = test.h2o, max_runtime_secs = 600)
  perf2 <- max(h2o.rf2@model$validation_metrics@metrics$thresholds_and_metric_scores$f1)
  
  cat('F1 con variabile = ', perf1, ' || F1 con variabile randomizzata = ', perf2, ' || F1-loss con variabile randomizzata = ', round((perf2/perf1 - 1) * 100, 2), '%')
}

### Closing connection
odbcClose(conn)

