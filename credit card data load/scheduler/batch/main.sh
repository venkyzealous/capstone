#!/bin/sh

for (( i=60; i>0; i--)); do
  sleep 14400 &
  
  echo "truncating lookup tables........"
  
  echo -e "truncate 'lookup_txn_master'" | hbase shell 
  echo -e "truncate 'lookup_card_member'" | hbase shell 
  
  echo "injesting incremental credit card data......."
  
  sqoop job --exec import_card_member
  sqoop job --exec import_member_score
  
  echo "processing.........."
  
  hive -f ./hive/analysis.sql
  
  wait
done