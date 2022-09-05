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
 ,gads AS 
    (SELECT
        fk."er_id"
        ,gad."segmentsDate"::date AS "date"
        ,CASE WHEN gad."campaignAdvertisingChannelType" = 'SEARCH' THEN 'Google Ads'
              ELSE initcap(gad."campaignAdvertisingChannelType")
              END AS "internal_channel"
        ,gad."campaignName" AS "campaign"
        ,gad."campaignId"::string AS "campaign_id"
        ,sum(gad."metricsClicks") AS "sessions_clicks"
        ,sum(gad."metricsCostMicros") / 750000 as "cost"  -- accounts for 75% margin and microns adjustment
      FROM (SELECT * FROM "gad_campaign_performance_final" UNION ALL SELECT * FROM "gad_campaign_performance_window") gad
      INNER JOIN "er_fk_table" fk
        ON gad."customerId" = fk."aw_id"
      WHERE gad."campaignAdvertisingChannelType" in ('DISPLAY','SEARCH','SHOPPING') 
      GROUP BY 1,2,3,4,5   
     )
 ,fb AS 
    (SELECT fk."er_id"
           ,fa."date_start" as "date"
           ,'Facebook Ads' as "internal_channel"
           ,fa."campaign_name" as "campaign"
           ,fa."campaign_id"::string as "campaign_id"
           ,mode("clicks") as "sessions_clicks" -- uses mode because the data sometimes has anomolies
           ,mode("spend") / 0.75 as "cost" -- uses mode because the data sometimes has anomolies, accounts for 75% margin
      from (SELECT * from "fa_campaign_insights_final" UNION ALL select * from "fa_campaign_insights_window") fa
      inner join "er_fk_table" fk
      on replace(fa."ex_account_id",'act_','') = fk."fa_ppc_id"
      group by 1,2,3,4,5 
    )
    
 ,linkedin AS 
   (SELECT fk."er_id"
          ,date_from_parts(b."start_year",b."start_month",b."start_day") as "date"
          ,'LinkedIn' as "internal_channel"
          ,ca."name" as "campaign"
          ,ca."id"::string as "campaign_id"
          ,sum(b."clicks") as "sessions_clicks"
          ,sum(c."costInLocalCurrency") / 0.75 as "cost"  -- accounts for 75% margin
      from (SELECT * FROM "li_ads_basic_stats_final" UNION ALL select * FROM "li_ads_basic_stats_window") b

      left outer join "li_campaigns" ca
        on regexp_replace(b."pivotValue",'\\D','') = ca."id"

      left outer join "li_ads_cost_final" c
        on b."pivotValue" = c."pivotValue"
        and b."start_year" = c."start_year"
        and b."start_month" = c."start_month"
        and b."start_day" = c."start_day"

      inner join "er_fk_table" fk
       on regexp_replace(ca."account",'\\D','') = fk."li_id"
      group by 1,2,3,4,5   
   )
 ,seo as 
 ( select ga_cc."er_id"
      ,ga_cc."date"
      ,ga_cc."internal_channel" as "internal_channel"
      ,ga_cc."internal_channel" as "campaign"
      ,ga_cc."internal_channel" as "campaign_id"
      ,ga_cc."sessions_clicks"
      ,ga_cc."cost"
    from
    (
      -- Google Analytics sessions and cost
      -- organic, Direct, Email and Other sessions are always sourced with this table
      -- Google Ads, Display, Facebook Ads and Linkendin sessions are only sourced from GA when there is no integration added for that client

      select
        ga."er_id"
        ,ga."date"
        ,ga."internal_channel" as "internal_channel"
        ,ga."sessions_clicks"
        ,iv."total_no_tax"
        ,gas."days_in_month"
        ,iv."total_no_tax" / gas."days_in_month" as "cost" -- averages out monthly invoices
      from
      (
        select
          fk."er_id"
          ,to_date (left(ga."dateHourMinute",8),'YYYYMMDD') as "date"
          -- make sure this matches https://www.notion.so/erdocs/How-we-define-an-internal-channel-867892a4e1fc412ba62ec59c2cfc9d2c
          ,case when ga."medium" = 'organic' then 'SEO'    -- claims all organic
                when (contains(lower(ga."source"),'facebook')
                      or lower(ga."source") = 'social')
                  and lower(ga."medium") in ('cpc','ppc')
                  then 'Facebook Ads'  -- claims all social cpc traffic
                when contains(lower(ga."source"),'linkedin')
                  and lower(ga."medium") in ('cpc','ppc')
                  then 'LinkedIn'
                when contains(lower(ga."source"),'facebook')
                  or contains(lower(ga."source"),'instagram')
                  or contains(lower(ga."source"),'linkedin')
                  or contains(lower(ga."source"),'lnkd.in')
                  or lower(ga."source") = 'social'
                  or lower(ga."medium") = 'social'
                  then 'Social'
                when lower(ga."source") = 'google'
                  and rlike(ga."medium",'^(cpc|ppc)$')
                  and ga."adDistributionNetwork" != 'Content'
                  then 'Google Ads'
                when ga."source" = '(direct)'
                  and ga."medium" in ('(not set)','(none)')
                  then 'Direct'
                when rlike(ga."medium",'^(display|cpm|banner)$')
                  or ga."adDistributionNetwork" = 'Content'
                  then 'Display'
                when ga."medium" = 'email' then 'Email'
                else 'Other'
                end as "internal_channel"
          ,sum(ga."sessions") as "sessions_clicks"
        from "ga_sessions" ga

        inner join "er_fk_table" fk
        on ga."idProfile" = fk."ga_id"

        group by 1,2,3
      ) ga


      inner join "er_fk_table" fk
      on ga."er_id" = fk."er_id"


      -- spend data for SEO from EngineRoom's Xero invoices

      left outer join
      (
        select
          fk."er_id"
          ,date_trunc('month',i."Date") as "month"
          ,sum(i."TotalAmount") as "total_no_tax"
        from "acc_invoices" i

        inner join "er_fk_table" fk
        on contains(fk."xero_contact_id",i."ContactID")

        where i."uid" = '8803f645-f822-46a2-a3d8-5834cfa27ca5'
        and i."ProductCode" in ('I-DM021','I-DM013','I-DM020','I-DM022','I-DM014','I-DM014.1')  -- seo solutions and net-dev
        and i."Status" in ('PAID','AUTHORISED')

        group by 1,2
      ) iv

      on ga."er_id" = iv."er_id"
      and date_trunc(month,ga."date") = iv."month"
      and ga."internal_channel" = 'SEO'


      -- in case there are days without activity to make the total cost add up for the month

      left outer join
      (
        select
          fk."er_id"
          ,date_trunc(month,to_date (left(ga."dateHourMinute",8),'YYYYMMDD')) as "month"
          ,count(distinct to_date (left(ga."dateHourMinute",8),'YYYYMMDD')) as "days_in_month"
        from "ga_sessions" ga

        inner join "er_fk_table" fk
        on ga."idProfile" = fk."ga_id"

        where ga."medium" = 'organic'   -- only need days with SEO activity

        group by 1,2
      ) gas

        on ga."er_id" = gas."er_id"
        and date_trunc(month,ga."date") = gas."month"
        and ga."internal_channel" = 'SEO'

        where not (ga."internal_channel" in ('Google Ads','Display') and fk."aw_id" is not null)    -- ensures there is no data if there is a google ads integration
        and not (ga."internal_channel" = 'Facebook Ads' and fk."fa_ppc_id" is not null)   -- ensures there is no data if there is a facebook ads integration
        and not (ga."internal_channel" = 'LinkedIn' and fk."li_id" is not null)    -- ensures there is no data if there is a linkedin integration

        group by 1,2,3,4,5,6,7
    ) ga_cc
 )
 ,stacked_costs as 
   (SELECT * FROM gads
    UNION ALL 
    SELECT * FROM fb 
    UNION ALL 
    SELECT * FROM linkedin 
    UNION ALL 
    SELECT * FROM seo)
 ,stacked_costs_grouped as 
   (SELECT "er_id"
           ,"internal_channel"
           ,DATE_TRUNC('month',to_timestamp("date")) as "month_date"
           ,sum("cost") as "cost" 
    FROM stacked_costs
    GROUP BY 1,2,3)
  ,lasttouch AS 
                (select ROW_NUMBER() OVER (PARTITION BY "google_client_id" ORDER BY "start_time" desc) as rn
                 ,"google_client_id"
                 ,"start_time"
                 ,case when "medium" = 'organic' then 'SEO'    -- claims all organic
                       when (contains(lower("source"),'facebook') or lower("source") = 'social') and lower("medium") in ('cpc','ppc')
                            then 'Facebook Ads'  -- claims all social cpc traffic
                       when contains(lower("source"),'linkedin') and lower("medium") in ('cpc','ppc')
                            then 'LinkedIn'
                       when contains(lower("source"),'facebook') or contains(lower("source"),'instagram') or contains(lower("source"),'linkedin')
                            or contains(lower("source"),'lnkd.in') or lower("source") = 'social'or lower("medium") = 'social'
                            then 'Social'
                       when lower("source") = 'google' and contains("channel",'Paid Search')
                            then 'Google Ads'
                       when "channel" in ('Direct','Display') then "channel"
                       when "medium" = 'email' then 'Email'
                       else 'Other'
                  end as channel
                 ,case when "medium" = 'organic' then 'SEO'    -- claims all organic
                       when (contains(lower("source"),'facebook') or lower("source") = 'social') and lower("medium") in ('cpc','ppc')
                            then 'Facebook Ads'  -- claims all social cpc traffic
                       when contains(lower("source"),'linkedin') and lower("medium") in ('cpc','ppc')
                            then 'LinkedIn'
                       when contains(lower("source"),'facebook') or contains(lower("source"),'instagram') or contains(lower("source"),'linkedin')
                            or contains(lower("source"),'lnkd.in') or lower("source") = 'social'or lower("medium") = 'social'
                            then 'LinkedIn'
                       when lower("source") = 'google' and contains("channel",'Paid Search')
                            then 'Google Ads'
                       when "channel" in ('Direct') then 'Direct'
                       else 'Other'
                  end as "LastTouch"
                 from "er_ga_sessions"
                 ) 
  ,base as (SELECT "er_id","googleClientId","attribution_method","Revenue",DATE_TRUNC('month',to_timestamp("timestamp")) as "month_timestamp" FROM B 
            UNION ALL 
            SELECT "er_id","googleClientId","attribution_method","Revenue",DATE_TRUNC('month',to_timestamp("timestamp")) as "month_timestamp" FROM B_CSV
           )
  ,base_joined as (SELECT base.*
                         ,x."LastTouch" --this maps out the last touch attribution to either Google Ads // LinkedIn // FB // SEO // Direct for each lead
                   FROM base 
                   LEFT JOIN lasttouch x 
                     on base."googleClientId" = x."google_client_id"
                     and rn=1
                   )

SELECT base_joined."er_id"
      ,"LastTouch"
      ,"attribution_method"
      ,sum("Revenue") as "Revenue"
      ,count(*) as "Rows"
      ,"cost"
      ,sum("Revenue")/"cost" as "ROI"
      ,count(distinct base_joined."er_id") as "Distinct_Businesses"
      ,count(distinct "googleClientId") as "Distinct_Customers"   
FROM base_joined 
       
LEFT JOIN stacked_costs_grouped
    on base_joined."er_id" = stacked_costs_grouped."er_id"
    and base_joined."month_timestamp" = stacked_costs_grouped."month_date"
    and base_joined."LastTouch" = stacked_costs_grouped."internal_channel" 

WHERE "month_timestamp" = '2022-06-01'
  
GROUP BY 1,2,3,6
ORDER BY 1         
