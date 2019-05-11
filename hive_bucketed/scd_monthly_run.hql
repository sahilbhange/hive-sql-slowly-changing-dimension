
-- Set the database
use yelp_data_bucket_sbhange;

-- Set the TEZ as an execution engine
set hive.execution.engine=tez;


-- Remove existing TXT and STG table data
dfs -rm -r -skipTrash /user/sahilbhange/bucket/yelp_stg_text/*;
dfs -rm -r -skipTrash /user/sahilbhange/bucket/yelp_stg_orc/*;

-- Load CSV file data to 'yelp_user_stg_text' table
dfs -copyFromLocal /home/sahilbhange/landing/yelp_data/yelp_user_${hiveconf:FILE_DATE}.csv /user/sahilbhange/bucket/yelp_stg_text/.;

-- Insert text data from 'yelp_user_stg_text' to 'yelp_user_stg' STG table in ORC format
insert into yelp_user_stg select * from yelp_user_stg_text;


-- Insert Expired records from HIST table to temporary STG_HIST table

INSERT INTO yelp_user_hist_stg
SELECT *
FROM   yelp_user_hist
WHERE  exp_dt != '2099-12-31'
UNION ALL
SELECT hist.*
FROM   yelp_user_hist hist
       INNER JOIN yelp_user_stg stg
               ON hist.user_id = stg.user_id
WHERE  hist.review_count = stg.review_count
       AND hist.useful = stg.useful
       AND hist.funny = stg.funny
       AND hist.cool = stg.cool
       AND hist.fans = stg.fans
       AND hist.average_stars = stg.average_stars
       AND hist.compliment_hot = stg.compliment_hot
       AND hist.compliment_more = stg.compliment_more
       AND hist.compliment_profile = stg.compliment_profile
       AND hist.compliment_cute = stg.compliment_cute
       AND hist.compliment_list = stg.compliment_list
       AND hist.compliment_note = stg.compliment_note
       AND hist.compliment_plain = stg.compliment_plain
       AND hist.compliment_cool = stg.compliment_cool
       AND hist.compliment_funny = stg.compliment_funny
       AND hist.compliment_writer = stg.compliment_writer
       AND hist.compliment_photos = stg.compliment_photos
       AND hist.exp_dt = '2099-12-31'
UNION ALL
SELECT stg.user_id,
       stg.NAME,
       stg.review_count,
       stg.yelping_since,
       stg.useful,
       stg.funny,
       stg.cool,
       stg.fans,
       stg.elite,
       stg.average_stars,
       stg.compliment_hot,
       stg.compliment_more,
       stg.compliment_profile,
       stg.compliment_cute,
       stg.compliment_list,
       stg.compliment_note,
       stg.compliment_plain,
       stg.compliment_cool,
       stg.compliment_funny,
       stg.compliment_writer,
       stg.compliment_photos,
       cast(Date_format('${hiveconf:RUN_DATE}', 'yyyy-MM-dd') AS DATE),
       cast(Date_format('2099-12-31', 'yyyy-MM-dd') AS DATE),
       stg.month_date
FROM   yelp_user_stg stg
       LEFT JOIN (SELECT *
                  FROM   yelp_user_hist
                  WHERE  exp_dt = '2099-12-31') hist
              ON hist.user_id = stg.user_id
WHERE  hist.user_id IS NULL
        OR hist.review_count != stg.review_count
        OR hist.useful != stg.useful
        OR hist.funny != stg.funny
        OR hist.cool != stg.cool
        OR hist.fans != stg.fans
        OR hist.average_stars != stg.average_stars
        OR hist.compliment_hot != stg.compliment_hot
        OR hist.compliment_more != stg.compliment_more
        OR hist.compliment_profile != stg.compliment_profile
        OR hist.compliment_cute != stg.compliment_cute
        OR hist.compliment_list != stg.compliment_list
        OR hist.compliment_note != stg.compliment_note
        OR hist.compliment_plain != stg.compliment_plain
        OR hist.compliment_cool != stg.compliment_cool
        OR hist.compliment_funny != stg.compliment_funny
        OR hist.compliment_writer != stg.compliment_writer
        OR hist.compliment_photos != stg.compliment_photos
UNION ALL
SELECT hist.user_id,
       hist.NAME,
       hist.review_count,
       hist.yelping_since,
       hist.useful,
       hist.funny,
       hist.cool,
       hist.fans,
       hist.elite,
       hist.average_stars,
       hist.compliment_hot,
       hist.compliment_more,
       hist.compliment_profile,
       hist.compliment_cute,
       hist.compliment_list,
       hist.compliment_note,
       hist.compliment_plain,
       hist.compliment_cool,
       hist.compliment_funny,
       hist.compliment_writer,
       hist.compliment_photos,
       hist.eff_dt,
       cast(Date_format('${hiveconf:EXP_DATE}', 'yyyy-MM-dd') AS DATE),
       hist.proc_dt
FROM   (SELECT *
        FROM   yelp_user_hist
        WHERE  exp_dt = '2099-12-31') hist
       LEFT JOIN yelp_user_stg stg
              ON hist.user_id = stg.user_id
WHERE  stg.user_id IS NULL
        OR hist.NAME != stg.NAME
        OR hist.review_count != stg.review_count
        OR hist.useful != stg.useful
        OR hist.funny != stg.funny
        OR hist.cool != stg.cool
        OR hist.fans != stg.fans
        OR hist.average_stars != stg.average_stars
        OR hist.compliment_hot != stg.compliment_hot
        OR hist.compliment_more != stg.compliment_more
        OR hist.compliment_profile != stg.compliment_profile
        OR hist.compliment_cute != stg.compliment_cute
        OR hist.compliment_list != stg.compliment_list
        OR hist.compliment_note != stg.compliment_note
        OR hist.compliment_plain != stg.compliment_plain
        OR hist.compliment_cool != stg.compliment_cool
        OR hist.compliment_funny != stg.compliment_funny
        OR hist.compliment_writer != stg.compliment_writer
        OR hist.compliment_photos != stg.compliment_photos; 


-- Remove existing HIST table data
dfs -rm -r -skipTrash /user/sahilbhange/bucket/yelp_hist/*;

-- Move HIST_STG data to HIST table
dfs -mv /user/sahilbhange/bucket/yelp_data_hist_stg/* /user/sahilbhange/bucket/yelp_hist/.;
