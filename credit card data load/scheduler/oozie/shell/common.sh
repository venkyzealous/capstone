#!/bin/sh

echo -e "truncate 'lookup_txn_master'" | hbase shell 
echo -e "truncate 'lookup_card_member'" | hbase shell 
