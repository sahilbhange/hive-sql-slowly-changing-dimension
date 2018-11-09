
-- Set the database
use yelp_data_part_sbhange;
set hive.exec.dynamic.partition.mode=nonstrict;

-- Set the TEZ as an execution engine
set hive.execution.engine=tez;


-- Remove existing TXT and STG table data
dfs -rm -r -skipTrash /user/sahilbhange/part/yelp_stg_text/*;
dfs -rm -r -skipTrash /user/sahilbhange/part/yelp_stg_orc/*;

-- Load CSV file data to 'yelp_user_stg_text' table
dfs -copyFromLocal /home/sahilbhange/landing/yelp_data/yelp_user_${hiveconf:FILE_DATE}.csv /user/sahilbhange/part/yelp_stg_text/.;

-- Insert text data from 'yelp_user_stg_text' to 'yelp_user_stg' STG table in ORC format
insert into yelp_user_stg partition (part_key)
select user_id,  name,  review_count,  yelping_since,  useful,  funny,  cool,  fans,  elite,  average_stars,  compliment_hot,  compliment_more,  compliment_profile,  compliment_cute,  compliment_list,  compliment_note,  compliment_plain,  compliment_cool,  compliment_funny,  compliment_writer,  compliment_photos,month_date,
CASE WHEN month(yelping_since) in (1,2,3) THEN 1
            WHEN month(yelping_since) in (4,5,6) THEN 2
            WHEN month(yelping_since) in (7,8,9) THEN 3
        ELSE 4 END AS part_key
from yelp_user_stg_text;


-- Insert Expired records from HIST table to STG_HIST table

insert into yelp_user_hist_stg partition (part_key)
select * from yelp_user_hist where exp_dt != '2099-12-31';


insert into yelp_user_hist_stg partition (part_key)
select hist.* from yelp_user_hist hist
inner join yelp_user_stg stg
on hist.user_id = stg.user_id
where hist.review_count  = stg.review_count
and hist.useful = stg.useful
and hist.funny = stg.funny
and hist.cool = stg.cool
and hist.fans = stg.fans
and hist.average_stars = stg.average_stars
and hist.compliment_hot = stg.compliment_hot
and hist.compliment_more = stg.compliment_more
and hist.compliment_profile = stg.compliment_profile
and hist.compliment_cute = stg.compliment_cute
and hist.compliment_list = stg.compliment_list
and hist.compliment_note = stg.compliment_note
and hist.compliment_plain = stg.compliment_plain
and hist.compliment_cool = stg.compliment_cool
and hist.compliment_funny = stg.compliment_funny
and hist.compliment_writer = stg.compliment_writer
and hist.compliment_photos = stg.compliment_photos
and hist.exp_dt = '2099-12-31';


insert into yelp_user_hist_stg partition (part_key)
select stg.user_id,  stg.name,  stg.review_count,  stg.yelping_since,  stg.useful,  stg.funny,
stg.cool,  stg.fans,  stg.elite,  stg.average_stars,  stg.compliment_hot,  stg.compliment_more,
stg.compliment_profile,  stg.compliment_cute,  stg.compliment_list,  stg.compliment_note,
stg.compliment_plain,  stg.compliment_cool,  stg.compliment_funny,  stg.compliment_writer,
stg.compliment_photos, cast(date_format('${hiveconf:RUN_DATE}','yyyy-MM-dd') as date),
cast(date_format('2099-12-31','yyyy-MM-dd') as date), stg.month_date ,stg.part_key
from yelp_user_stg stg
left join (select * from yelp_user_hist where exp_dt = '2099-12-31') hist
on hist.user_id = stg.user_id
where hist.user_id is null
or hist.review_count  != stg.review_count
or hist.useful != stg.useful
or hist.funny != stg.funny
or hist.cool != stg.cool
or hist.fans != stg.fans
or hist.average_stars != stg.average_stars
or hist.compliment_hot != stg.compliment_hot
or hist.compliment_more != stg.compliment_more
or hist.compliment_profile != stg.compliment_profile
or hist.compliment_cute != stg.compliment_cute
or hist.compliment_list != stg.compliment_list
or hist.compliment_note != stg.compliment_note
or hist.compliment_plain != stg.compliment_plain
or hist.compliment_cool != stg.compliment_cool
or hist.compliment_funny != stg.compliment_funny
or hist.compliment_writer != stg.compliment_writer
or hist.compliment_photos != stg.compliment_photos;


insert into yelp_user_hist_stg partition (part_key)
select hist.user_id,  hist.name,  hist.review_count,  hist.yelping_since,  hist.useful,  hist.funny,
hist.cool,  hist.fans,  hist.elite,  hist.average_stars,  hist.compliment_hot,  hist.compliment_more,
hist.compliment_profile,  hist.compliment_cute,  hist.compliment_list,  hist.compliment_note,
hist.compliment_plain,  hist.compliment_cool,  hist.compliment_funny,  hist.compliment_writer, hist.compliment_photos,
hist.eff_dt, cast(date_format('${hiveconf:EXP_DATE}','yyyy-MM-dd') as date), hist.proc_dt ,stg.part_key
from (select * from yelp_user_hist where exp_dt = '2099-12-31') hist
left join yelp_user_stg stg
on hist.user_id = stg.user_id
where hist.user_id is null
or hist.name != stg.name
or hist.review_count  != stg.review_count
or hist.useful != stg.useful
or hist.funny != stg.funny
or hist.cool != stg.cool
or hist.fans != stg.fans
or hist.average_stars != stg.average_stars
or hist.compliment_hot != stg.compliment_hot
or hist.compliment_more != stg.compliment_more
or hist.compliment_profile != stg.compliment_profile
or hist.compliment_cute != stg.compliment_cute
or hist.compliment_list != stg.compliment_list
or hist.compliment_note != stg.compliment_note
or hist.compliment_plain != stg.compliment_plain
or hist.compliment_cool != stg.compliment_cool
or hist.compliment_funny != stg.compliment_funny
or hist.compliment_writer != stg.compliment_writer
or hist.compliment_photos != stg.compliment_photos;

-- Remove existing HIST table data
dfs -rm -r -skipTrash /user/sahilbhange/part/yelp_hist/*;


-- Move HIST_STG data to HIST table
dfs -mv /user/sahilbhange/part/yelp_data_hist_stg/* /user/sahilbhange/part/yelp_hist/.;
