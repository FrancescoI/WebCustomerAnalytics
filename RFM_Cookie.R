### Importing data to BigQuery

### Loading libraries
library(bigrquery)
library(tidyverse)
library(caret)
library(easyRFM) ### installare da comando: 
                 ### install.packages("devtools")
                 ### devtools::install_github("hoxo-m/easyRFM")

### Faccio la Query
### | Cookie ID | Revenue | Date |
###
### Per le sole date in cui è avvenuta una transazione

query1 <- "SELECT 
          fullVisitorId as id, 
          IFNULL(totals.transactionRevenue,0) as payment, 
          date as date 
          FROM (TABLE_DATE_RANGE([yoox-bq-export:20853980.ga_sessions_], TIMESTAMP('2017-01-01'), TIMESTAMP('2017-08-31'))) 
          WHERE totals.transactionRevenue > 0"


### Faccio la Query
### | Hashed Email | Revenue | Date |
###
### Per le sole date in cui è avvenuta una transazione

query2 <- 'SELECT * FROM (
            SELECT
              MAX(IF(customDimensions.index=48, customDimensions.value, NULL)) WITHIN RECORD AS id,
              IFNULL(totals.transactionRevenue,0) AS payment,
              date AS date,
            FROM (TABLE_DATE_RANGE([yoox-bq-export:20853980.ga_sessions_], TIMESTAMP("2017-09-01"), TIMESTAMP("2017-09-07")))
            WHERE
              totals.transactionRevenue > 0 
            )
            WHERE id != ""'



query_res <- query_exec(query2, 'yoox-bq-export', max_pages = Inf)
query_final <- query_res

### Converto correttamente la transaction Revenue e le Date
query_final$payment <- query_final$payment / 1000000
query_final$date <- as.Date(query_final$date, "%Y%m%d")

### RFM
result <- rfm_auto(query_final)
head(result$rfm)
result$rfm %>% arrange(desc(FrequencyClass), desc(MonetaryClass)) %>% head
