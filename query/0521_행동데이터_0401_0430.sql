# CTE 정의
with 
v as (SELECT ID, Event_Category, d.Label as label, d.action as action, d.count as count, d.Value as value, Event_Date
FROM `ballosodeuk.airbridge_mart.key_activity`, UNNEST(Detail) as d   
WHERE Event_Date between "2024-05-10" and "2024-05-19"),

# 각 피쳐 CTE 정의
total_go_bf_list as (select ID, sum(count) as tap_go__bf_list
from v
where Event_Category = "tap_go__bf_list (App)"and action != '오퍼월'
group by ID, Event_Category),

go_offerwall as (select ID, sum(count) as bf_offerwall
from v
where Event_Category = "tap_go__bf_list (App)" and action = '오퍼월'
group by ID, Event_Category),

coin as (select ID, sum(value) as coin_reward
from v
where Event_Category = "tap_get__reward_done (App)" 
group by ID, Event_Category),

rwd_coupang as (select ID, sum(value) as cp_reward
from v
where Event_Category = "view_get__reward_done (App)" and action = '쿠팡'
group by ID, Event_Category),

rwd_challenge as (select ID, sum(value) as chg_reward
from v
where Event_Category = "view_get__reward_done (App)" and label = '소득받기'
group by ID, Event_Category),

rwd_challenge_cnt as (select ID, sum(count) as chg_reward_cnt
from v
where Event_Category = "view_get__reward_done (App)" and label = '소득받기'
group by ID, Event_Category),

join_challenge as (select ID, sum(count) as chg_join
from v
where Event_Category = "view_get__ch_join_done (App)"
group by ID, Event_Category),

rwd_cps as (select ID, sum(value) as cps_reward
from v
where Event_Category = "view_get__reward_done (App)" and label = '매일버튼누르기챌린지'
group by ID, Event_Category),

spend as (select ID, sum(value) as spend
from v
where Event_Category = "Spend Credits (App)"
group by ID, Event_Category),

spend_cnt as (select ID, sum(count) as spend_cnt
from v
where Event_Category = "Spend Credits (App)"
group by ID, Event_Category),

adclick_noncoupang as (select ID, sum(count) as ad_click_no_coupang
from v
where Event_Category = "Ad Click (App)" and action != "직광고_쿠팡"
group by ID, Event_Category),

adclick_coupang as (select ID, sum(count) as ad_click_coupang
from v
where Event_Category = "Ad Click (App)" and action = "직광고_쿠팡"
group by ID, Event_Category),

# 방문일 테이블 생성
visit_dates AS (
    SELECT ID, ARRAY_AGG(DISTINCT Event_Date ORDER BY Event_Date) AS visit_days
    FROM v
    -- WHERE Event_Category = 'view_get__page (App)'
    GROUP BY ID
),

# 총 방문 일 수
total_visits AS (
    SELECT ID, COUNT(*) AS total_visits
    FROM (
        SELECT DISTINCT ID, Event_Date
        FROM visit_dates, UNNEST(visit_days) AS Event_Date
    )
    GROUP BY ID
),
# 평균 방문 주기
visit_intervals AS (
    SELECT ID, round(AVG(visit_interval),2) AS avg_visit_interval
    FROM (
        SELECT ID,
               DATE_DIFF(LEAD(Event_Date) OVER (PARTITION BY ID ORDER BY Event_Date), Event_Date, DAY) AS visit_interval
        FROM visit_dates, UNNEST(visit_days) AS Event_Date
    )
    WHERE visit_interval IS NOT NULL
    GROUP BY ID
)


# JOIN 및 결과 테이블 생성
SELECT 
    v.ID,
    COALESCE(t.tap_go__bf_list, 0) AS tap_go__bf_list,
    COALESCE(g.bf_offerwall, 0) AS bf_offerwall,
    COALESCE(c.coin_reward, 0) AS coin_reward,
    COALESCE(r.cp_reward, 0) AS cp_reward,
    COALESCE(rc.chg_reward, 0) AS chg_reward,
    COALESCE(rct.chg_reward_cnt, 0) AS chg_reward_cnt,
    COALESCE(jc.chg_join, 0) AS chg_join,
    COALESCE(rp.cps_reward, 0) AS cps_reward,
    COALESCE(s.spend, 0) AS spend,
    COALESCE(sc.spend_cnt, 0) AS spend_cnt,
    COALESCE(an.ad_click_no_coupang, 0) AS ad_click_no_coupang,
    COALESCE(ac.ad_click_coupang, 0) AS ad_click_coupang,
    COALESCE(vi.avg_visit_interval, 0) AS avg_visit_interval,
    COALESCE(tv.total_visits, 0) AS total_visits
FROM 
    (SELECT DISTINCT ID FROM v) AS v
LEFT JOIN 
    total_go_bf_list AS t ON v.ID = t.ID
LEFT JOIN 
    go_offerwall AS g ON v.ID = g.ID
LEFT JOIN 
    coin AS c ON v.ID = c.ID
LEFT JOIN 
    rwd_coupang AS r ON v.ID = r.ID
LEFT JOIN 
    rwd_challenge AS rc ON v.ID = rc.ID
LEFT JOIN 
    rwd_challenge_cnt AS rct ON v.ID = rct.ID
LEFT JOIN 
    join_challenge AS jc ON v.ID = jc.ID
LEFT JOIN 
    rwd_cps AS rp ON v.ID = rp.ID
LEFT JOIN 
    spend AS s ON v.ID = s.ID
LEFT JOIN 
    spend_cnt AS sc ON v.ID = sc.ID
LEFT JOIN 
    adclick_noncoupang AS an ON v.ID = an.ID
LEFT JOIN 
    adclick_coupang AS ac ON v.ID = ac.ID
LEFT JOIN 
    visit_intervals AS vi ON v.ID = vi.ID
LEFT JOIN 
    total_visits AS tv ON v.ID = tv.ID;
