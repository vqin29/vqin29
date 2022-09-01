WITH p AS 
          (SELECT "uid"
                 ,"ContactID"
                  ,CASE WHEN LEFT("phone"::NUMBER,1) IN (2,3,4,7,8) AND LENGTH("phone"::NUMBER) = 9 THEN 61 || "phone"::NUMBER
                          ELSE "phone"
                          END AS "phone_number"
           FROM "acc_contacts_phones"
           UNPIVOT ("phone" FOR "type" IN ("phone_number_default","phone_number_ddi","phone_number_mobile","phone_number_fax"))
           WHERE "phone" != ''
           GROUP BY 1,2,3
           )
    ,p_csv AS
          (SELECT c."uid"
                 ,c."er_id"
                 ,c."client_name"
                 ,c."sale_date"
                 ,c."revenue"
                 ,CASE WHEN LEFT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(c."phone",'\\s',''),'\\D+',','),'^,|,.*')::NUMBER,1) IN (2,3,4,7,8)
                       AND LENGTH(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(c."phone",'\\s',''),'\\D+',','),'^,|,.*')::NUMBER) 
                       THEN 61 || REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(c."phone",'\\s',''),'\\D+',','),'^,|,.*')::NUMBER
                       ELSE REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(c."phone",'\\s',''),'\\D+',','),'^,|,.*')
                       END AS "phone_number" -- takes first number if more than one
            FROM "er_csv_sales" c
            UNPIVOT ("phone" FOR "type" IN ("mobile_number","land_line","secondary_phone_number"))
            HAVING REGEXP_REPLACE(c."phone",'\\D+','') != ''
           )
      ,e AS 
          (SELECT c."uid"
                 ,c."er_id"
                 ,c."client_name"
                 ,c."sale_date"
                 ,c."revenue"
                 ,c."email_address"
           FROM "er_csv_sales" c
           UNPIVOT ("email_address" FOR "type" IN ("email","secondary_email"))
           WHERE "email_address" != ''
          )
   ,ff AS  
          (SELECT lf."er_id"
                 ,lf."submissionId" AS "lead_id"
                 ,CASE WHEN REGEXP_SUBSTR(lf."caller",'[1-9]') IN (2,3,4,7,8) AND LENGTH(REGEXP_REPLACE(lf."caller",'\\D','')::NUMBER) = 9 
                       THEN 61 || REGEXP_REPLACE(lf."caller",'\\D','')::NUMBER
                       ELSE REGEXP_REPLACE(lf."caller",'\\D','')
                       END AS "phone_number"
                 ,TRIM(lf."email") AS "email"
                 ,CASE WHEN lf."formName" = 'Podium' THEN 'Podium'
                       ELSE 'Form Fill'
                       END AS "attribution_method"
                 ,REGEXP_REPLACE(lf."googleClientId",'GA1\.[0-9]\.','') AS "googleClientId"
                 ,MIN(CONVERT_TIMEZONE('Australia/Melbourne',lf."leadDate"))  AS "lead_timestamp"
           FROM "er_leads_forms" lf
           WHERE REGEXP_REPLACE("caller",'\\D','') != ''
           GROUP BY 1,2,3,4,5,6
           )
    ,ff_csv AS
            (SELECT lf."er_id"
                   ,CASE WHEN REGEXP_SUBSTR(lf."caller",'[1-9]') IN (2,3,4,7,8)
                         AND LENGTH(REGEXP_REPLACE(lf."caller",'\\D','')::NUMBER) = 9
                         THEN 61 || REGEXP_REPLACE(lf."caller",'\\D','')::NUMBER
                         ELSE REGEXP_REPLACE(lf."caller",'\\D','')
                         END AS "phone_number"
                    ,TRIM(lf."email") AS "email"
                    ,CASE WHEN lf."formName" = 'Podium' THEN 'Podium'
                          ELSE 'Form Fill'
                          END AS "attribution_method"
                    ,regexp_replace("googleClientId",'GA1\.[0-9]\.','') AS "googleClientId"
                    ,MIN(CONVERT_TIMEZONE('Australia/Melbourne',lf."leadDate"))  AS "timestamp"
              FROM "er_leads_forms" lf
              WHERE REGEXP_REPLACE("caller",'\\D','') != ''   
              GROUP BY 1,2,3,4,5
             )
  ,f AS
          (SELECT "uid"
                 ,"ContactID"
                 ,MIN("Date"::DATE) AS "first_invoice_date"
           FROM "acc_invoices"
           WHERE "Source" = 'Invoices'AND ("Status" IS NULL OR LENGTH("Status" ) = 0 OR "Status" IN ('PAID', 'AUTHORISED'))
           GROUP BY 1,2
           )
  ,wj_calls AS 
          (SELECT fk."er_id"
                 ,fc."lead_id"
                 ,c."ContactID"
                 ,fc."lead_timestamp"
                 ,fc."googleClientId"
                 ,DATEDIFF(d,fc."lead_timestamp"::DATE,f."first_invoice_date") AS "days_lead_to_sale"
                 ,'Phone' AS "attribution_method"
           FROM "acc_contacts" c
           INNER JOIN "er_fk_table" fk
              ON CONTAINS(COALESCE(fk."xe_id",fk."my_id"),c."uid")
           INNER JOIN p
              ON c."ContactID" = p."ContactID"
              AND c."uid" = p."uid"
           INNER JOIN f
              ON c."ContactID" = f."ContactID"
              AND c."uid" = f."uid"
           INNER JOIN
                (
                  SELECT
                    fk."er_id"
                    ,"uid" AS "lead_id"
                    ,"caller"
                    ,"googleClientId"
                    ,MIN(CONVERT_TIMEZONE('Australia/Melbourne',"dateStartGMT"::STRING))  AS "lead_timestamp"
                  FROM "er_leads_calls" lc
                  INNER JOIN "er_fk_table" fk
                    ON CONTAINS(fk."wj_id",lc."account")
                  WHERE "caller" NOT IN ('anonymous','')
                  GROUP BY 1,2,3,4
                ) fc
              ON fk."er_id" = fc."er_id"
              AND fc."lead_timestamp"::DATE <= f."first_invoice_date"
              AND p."phone_number"::STRING = fc."caller"::STRING
            )
  ,wj_calls_csv as 
           (SELECT p_csv."uid"
                  ,p_csv."er_id"
                  ,p_csv."client_name"
                  ,p_csv."sale_date"
                  ,p_csv."revenue"
                  ,fc."timestamp"
                  ,fc."googleClientId"
                  ,DATEDIFF(d,fc."timestamp"::DATE,p_csv."sale_date") AS "days_lead_to_sale"
                  ,'Phone' AS "attribution_method"
            FROM p_csv
            INNER JOIN
                (
                  SELECT
                    fk."er_id"
                    ,lc."caller"
                    ,lc."googleClientId"
                    ,MIN(CONVERT_TIMEZONE('Australia/Melbourne',lc."dateStartGMT"::STRING))  AS "timestamp"
                  FROM "er_leads_calls" lc

                  INNER JOIN "er_fk_table" fk
                  ON CONTAINS(fk."wj_id",lc."account")

                  WHERE "caller" NOT IN ('anonymous','')

                  GROUP BY 1,2,3
                ) fc
              ON p_csv."er_id" = fc."er_id"
              AND p_csv."sale_date" >= fc."timestamp"::DATE
              AND p_csv."phone_number" = fc."caller"
            )
  ,ff_email AS 
            (SELECT fk."er_id"
                   ,ff."lead_id"
                   ,c."ContactID"
                   ,ff."lead_timestamp"
                   ,ff."googleClientId"
                   ,DATEDIFF(d,ff."lead_timestamp"::DATE,f."first_invoice_date") AS "days_lead_to_sale"
                   ,ff."attribution_method"
             FROM "acc_contacts" c
             INNER JOIN "er_fk_table" fk
                 ON CONTAINS(COALESCE(fk."xe_id",fk."my_id"),c."uid")
             INNER JOIN f
                 ON c."ContactID" = f."ContactID"
                 AND c."uid" = f."uid"
             INNER JOIN ff
                ON fk."er_id" = ff."er_id"
                AND ff."lead_timestamp"::DATE <= f."first_invoice_date"
                AND ff."email" = c."EmailAddress" 
           )
  ,ff_email_csv AS 
         (SELECT e."uid"
                ,e."er_id"
                ,e."client_name"
                ,e."sale_date"
                ,e."revenue"
                ,ff_csv."timestamp"
                ,ff_csv."googleClientId"
                ,DATEDIFF(d,ff_csv."timestamp"::DATE,e."sale_date") AS "days_lead_to_sale"
                ,ff_csv."attribution_method"
         FROM e
         INNER JOIN ff_csv
              ON e."er_id" = ff_csv."er_id"
              AND e."sale_date" >= ff_csv."timestamp"::DATE
              AND e."email_address" = ff_csv."email"
          )
  ,ff_phone AS 
           (SELECT fk."er_id"
                  ,ff."lead_id"
                  ,c."ContactID"
                  ,ff."lead_timestamp"
                  ,ff."googleClientId"
                  ,DATEDIFF(d,ff."lead_timestamp"::DATE,f."first_invoice_date") AS "days_lead_to_sale"
                  ,ff."attribution_method"
            FROM "acc_contacts" c
            INNER JOIN "er_fk_table" fk
                ON CONTAINS(COALESCE(fk."xe_id",fk."my_id"),c."uid")
            INNER JOIN f
                ON c."ContactID" = f."ContactID"
                AND c."uid" = f."uid"
            INNER JOIN p
                ON c."ContactID" = p."ContactID"
                AND c."uid" = p."uid"
            INNER JOIN ff
              ON fk."er_id" = ff."er_id"
              AND ff."lead_timestamp"::DATE <= f."first_invoice_date"
              AND p."phone_number" = ff."phone_number"  
           )
  ,ff_phone_csv AS 
          (SELECT p_csv."uid"
                ,p_csv."er_id"
                ,p_csv."client_name"
                ,p_csv."sale_date"
                ,p_csv."revenue"
                ,ff_csv."timestamp"
                ,ff_csv."googleClientId"
                ,DATEDIFF(d,ff_csv."timestamp"::DATE,p_csv."sale_date") AS "days_lead_to_sale"
                ,ff_csv."attribution_method"
              FROM p_csv

              INNER JOIN ff_csv
              ON p_csv."er_id" = ff_csv."er_id"
              AND p_csv."sale_date" >= ff_csv."timestamp"::DATE
              AND p_csv."phone_number" = ff_csv."phone_number"
          )
  ,stacked AS 
          (SELECT * FROM wj_calls 
           UNION ALL 
           SELECT * FROM ff_email 
           UNION ALL
           SELECT * FROM ff_phone 
          )
  ,stacked_csv AS 
         (SELECT * FROM wj_calls_csv 
           UNION ALL 
           SELECT * FROM ff_email_csv
           UNION ALL
           SELECT * FROM ff_phone_csv
         )       
  ,with_rn AS 
         (SELECT ROW_NUMBER() OVER (PARTITION BY "er_id" || "ContactID" ORDER BY "lead_timestamp" ASC, "attribution_method") AS "row_number"
                ,"er_id"
                ,"lead_id"
                ,"ContactID"
                ,"lead_timestamp"
                ,"googleClientId"
                ,"days_lead_to_sale"
                ,"attribution_method"
          FROM stacked
          )
   ,with_rn_csv AS 
         (SELECT ROW_NUMBER() OVER (PARTITION BY "er_id" || "uid" ORDER BY "timestamp" ASC, "attribution_method") AS "row_number"
                ,"er_id"
                ,"uid"
                ,"client_name"
                ,"sale_date"
                ,"revenue"
                ,"timestamp"
                ,"googleClientId"
                ,"days_lead_to_sale"
                ,"attribution_method"
          FROM stacked_csv
          )
  ,a AS 
        (SELECT with_rn.*
               ,acc_invoices."TotalAmount"
         FROM with_rn 
         INNER JOIN "er_fk_table" fk
           ON with_rn."er_id" = fk."er_id"
         LEFT JOIN "acc_contacts" c
          ON CONTAINS(COALESCE(fk."xe_id",fk."my_id"),c."uid")
          AND with_rn."ContactID" = c."ContactID"
         LEFT JOIN "acc_contacts_phones" p
           ON c."uid" = p."uid"
           AND c."ContactID" = p."ContactID"
         LEFT JOIN "acc_invoices" acc_invoices
           ON acc_invoices."ContactID" = with_rn."ContactID"
           AND acc_invoices."Status" IN ('PAID','AUTHORISED',NULL,'')
           AND acc_invoices."Source" IN ('Invoices','Credit Notes')
         WHERE "row_number" = 1
         )

 ,b AS 
      (SELECT  a."er_id"
              ,a."lead_id"
              ,a."ContactID"
              ,a."lead_timestamp" as "timestamp"
              ,a."googleClientId"
              ,a."days_lead_to_sale"
              ,a."attribution_method"
              ,SUM("TotalAmount") as "Revenue"
     FROM a 
     GROUP BY 1,2,3,4,5,6,7
      )
 ,b_csv AS 
     (SELECT "er_id"
             ,"uid"
                ,"client_name"
                ,"sale_date"
                ,"revenue" as "Revenue"
                ,"timestamp"
                ,"googleClientId"
                ,"days_lead_to_sale"
                ,"attribution_method"
      FROM with_rn_csv
      WHERE "row_number" = 1
     )

SELECT "er_id"
      ,"attribution_method"
//      ,DATE_TRUNC('month',to_timestamp("timestamp"))
      ,sum("Revenue") as "Revenue"
      ,count(*) as "Rows"
      ,count(distinct "er_id") as "Distinct_Businesses"
      ,count(distinct "googleClientId") as "Distinct_Customers"   
FROM (SELECT "er_id","googleClientId","attribution_method","Revenue","timestamp" FROM B UNION ALL SELECT "er_id","googleClientId","attribution_method","Revenue","timestamp" FROM B_CSV)
GROUP BY 1,2
ORDER BY 1,2
 
  

                  