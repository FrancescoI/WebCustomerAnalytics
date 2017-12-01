### Connecting to MS Sql Server - Shared Tables
library(RODBC)
library(tidyverse)
library(caret)
library(h2o)
conn <- odbcDriverConnect(connection="Driver={SQL Server Native Client 11.0};server=YAPP14;database=SharedTables;trusted_connection=yes;")

### Query
query <- c("SELECT
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
           WHERE Descrizione = 'ARMANICOM' AND ANNO = '2017' AND MESE = '10'")

### Executing Query
ex1 <- sqlQuery(conn, query)

### Preprocessing
ex1 <- ex1 %>% mutate(FlagReso = as.factor(FlagReso), 
                      Sconto = 1 - OrderValue/PotentialOrderValue,
                      Sesso_ID = as.factor(Sesso_ID))

### Selecting Features
train <- ex1 %>% select(FlagReso, OrderValue, Macro, Sconto)


### Starting h2o
h2o.init(nthreads = 3)

train.h2o <- as.h2o(train)
y = 'FlagReso'
x = setdiff(names(train.h2o), y)
automl <- h2o.automl(x = x, y = y, training_frame = train.h2o)

automl


### Visualizing
ex1 %>% mutate(FlagReso = as.factor(FlagReso), Sconto = 1 - OrderValue/PotentialOrderValue) %>% ggplot(aes(OrderValue, fill = FlagReso)) + geom_density(alpha = 0.5) + facet_wrap( ~ as.factor(Macro), ncol = 4, scales = 'free') 

ex1 %>% mutate(FlagReso = as.factor(FlagReso), Sconto = 1 - OrderValue/PotentialOrderValue) %>% group_by(FlagReso) %>% summarise(number = n())

### Closing connection
odbcClose(conn)
