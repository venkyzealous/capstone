USE default;

--Loading top 10 genuine transactions 
INSERT INTO LOOKUP_TXN_MASTER
SELECT * FROM (
    SELECT  *, RANK() OVER(PARTITION BY card_id ORDER BY transaction_dt DESC) txrank
     FROM(
        SELECT *
        FROM card_transactions
        WHERE status = "GENUINE"
    ) genuine_transactions
) ranked_transactions 
WHERE txrank <= 10;

TRUNCATE TABLE TXN_STATS_STG;

INSERT INTO TXN_STATS_STG
SELECT card_id, AVG(amount),stddev_pop(amount) FROM LOOKUP_TXN_MASTER
GROUP BY card_id;


TRUNCATE TABLE TXN_LAST_DETAIL_STG;

INSERT INTO TXN_LAST_DETAIL_STG 
SELECT card_id,postcode,transaction_dt
FROM LOOKUP_TXN_MASTER
where txrank = 1;

TRUNCATE TABLE TXN_FINAL_STG;

INSERT INTO TXN_FINAL_STG(card_id,moving_average,std_dev,
postcode,transaction_dt,score,member_id,member_joining_dt,card_purchase_dt,country,city)
SELECT S.card_id, S.moving_average, S.std_dev,
L.postcode,L.transaction_dt,MS.score,CM.member_id,CM.member_joining_dt,CM.card_purchase_dt,CM.country,CM.city FROM
TXN_STATS_STG S LEFT JOIN TXN_LAST_DETAIL_STG L
ON S.card_id = L.card_id
LEFT JOIN card_member CM
ON S.card_id = CM.card_id
LEFT JOIN member_score MS ON
CM.member_id = MS.member_id;



INSERT INTO LOOKUP_CARD_MEMBER
SELECT card_id,(3*std_dev + moving_average), postcode,
transaction_dt,score,member_id,member_joining_dt, card_purchase_dt,country,city FROM
TXN_FINAL_STG;