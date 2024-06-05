# 메인 CTE 정의
with 
v as (SELECT ID, Event_Category, d.Label as label, d.action as action, d.count as count, d.Value as value, Event_Date
FROM `ballosodeuk.airbridge_mart.key_activity`, UNNEST(Detail) as d
WHERE Event_Date between "2024-04-01" and "2024-04-30"),

#### 행동 CTE 정의
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

rwd_coupang as (select ID, sum(value) as cp_reward_bf
from v
where Event_Category = "view_get__reward_done (App)" and action = '쿠팡' and label != '홈_상단_캐시버튼'
group by ID, Event_Category),

#신규
rwd_coupang_homeCash as (select ID, sum(value) as cp_reward_homeCash
from v
where Event_Category = "view_get__reward_done (App)" and action = '쿠팡' and label = '홈_상단_캐시버튼'
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

#신규
rwd_bongtu as (select ID, sum(value) as bongtu_reward
from v
where Event_Category = "view_get__reward_done (App)" and label = '보너스봉투'
group by ID, Event_Category),

#신규
rwd_offerwall_adpopcorn as (select ID, sum(value) as adPopcorn_offerwall_reward
from v
where Event_Category = "view_get__reward_done (App)" and label = '애드팝콘'
group by ID, Event_Category),

#신규
rwd_offerwall_avati as (select ID, sum(value) as avati_offerwall_reward
from v
where Event_Category = "view_get__reward_done (App)" and label = '아바티'
group by ID, Event_Category),

#신규
rwd_invite as (select ID, sum(value) as invite_reward
from v
where Event_Category = "view_get__reward_done (App)" and (label = '지원금보내기' or label = '친구초대입력')
group by ID, Event_Category),

#신규
rwd_rsp as (select ID, sum(value) as rsp_reward
from v
where Event_Category = "view_get__reward_done (App)" and label = '가위바위보'
group by ID, Event_Category),

spend as (select ID, sum(value) as spend
from v
where Event_Category = "Spend Credits (App)"
group by ID, Event_Category),

spend_cnt as (select ID, sum(count) as spend_cnt
from v
where Event_Category = "Spend Credits (App)"
group by ID, Event_Category),

### 광고 관련 행동

adclick_cnt AS (
    SELECT 
        ID,
        SUM(CASE WHEN label = "홈_챌린지리스트" and action =	"직광고_자체배너" THEN COUNT ELSE 0 END) AS adClick_homeChg_ownedBanner,
        SUM(CASE WHEN label = "홈_챌린지리스트" and action =	"네트워크_애드팝콘" THEN COUNT ELSE 0 END) AS adClick_homeChg_adPopcorn,
        SUM(CASE WHEN label = "홈_챌린지리스트" and action =	"네트워크_애드파이" THEN COUNT ELSE 0 END) AS adClick_homeChg_adPie,
        SUM(CASE WHEN label = "홈_챌린지리스트" and action =	"네트워크_모비온" THEN COUNT ELSE 0 END) AS adClick_homeChg_mobion,
        SUM(CASE WHEN label = "홈_상단_캐시버튼" and action =	"직광고_쿠팡" THEN COUNT ELSE 0 END) AS adClick_homeCash_coupang,
        SUM(CASE WHEN label like "%캐시버튼%" and action =	"네트워크_애드팝콘" THEN COUNT ELSE 0 END) AS adClick_homeCash_adPopcorn,
        SUM(CASE WHEN label = "홈_상단_캐시버튼" and action =	"네트워크_애드파이" THEN COUNT ELSE 0 END) AS adClick_homeCash_adPie,
        SUM(CASE WHEN label like "홈_상단%" and action =	"네트워크_애드팝콘카카오" THEN COUNT ELSE 0 END) AS adClick_home_adPopcorn,
        -- SUM(CASE WHEN label = "홈_상단" and action =	"네트워크_애드팝콘카카오" THEN COUNT ELSE 0 END) AS adClick_home_adPopcorn,
        SUM(CASE WHEN label = "홈_바텀시트2" and action =	"직광고_자체배너" THEN COUNT ELSE 0 END) AS adClick_homeBottom_owned,
        SUM(CASE WHEN label = "홈_바텀시트2" and action =	"네트워크_애드팝콘" THEN COUNT ELSE 0 END) AS adClick_homeBottom_adPopcorn,
        SUM(CASE WHEN label = "홈_바텀시트2" and action =	"네트워크_애드파이" THEN COUNT ELSE 0 END) AS adClick_homeBottom_adPie,
        SUM(CASE WHEN label = "혜택_상단" and action =	"네트워크_애드팝콘" THEN COUNT ELSE 0 END) AS adClick_benefit_adPopcorn,
        SUM(CASE WHEN label = "챌린지_챌린지모아보기" and action =	"네트워크_애드팝콘" THEN COUNT ELSE 0 END) AS adClick_chgList_adPopcorn,
        SUM(CASE WHEN label = "챌린지_챌린지모아보기" and action =	"네트워크_애드파이" THEN COUNT ELSE 0 END) AS adClick_chgList_adPie,
        SUM(CASE WHEN label = "챌린지_상단" and action =	"직광고_자체배너" THEN COUNT ELSE 0 END) AS adClick_chg_owned,
        SUM(CASE WHEN label = "챌린지_상단" and action =	"네트워크_애드팝콘카카오" THEN COUNT ELSE 0 END) AS adClick_chg_adPopcorn,
        SUM(CASE WHEN label = "전체_상단" and action =	"네트워크_애드팝콘카카오" THEN COUNT ELSE 0 END) AS adClick_info_adPopcorn,
        SUM(CASE WHEN label = "앱 종료" and action =	"네트워크_애드파이" THEN COUNT ELSE 0 END) AS adClick_exit_adPie,
        SUM(CASE WHEN label = "보너스봉투팝업_하단" and action =	"네트워크_애드팝콘" THEN COUNT ELSE 0 END) AS adClick_benefit_bongtu_adPopcorn,
        SUM(CASE WHEN label = "매일버튼누르기챌린지" and action =	"네트워크_애드팝콘" THEN COUNT ELSE 0 END) AS adClick_benefit_button_adPopcorn,
        SUM(CASE WHEN (label = "구매_상단" or label ="카테고리상세_상단") and action = "직광고_자체배너" THEN COUNT ELSE 0 END) AS adClick_purchase_owned
    FROM 
        v
    WHERE 
        Event_Category = 'Ad Click (App)'
    GROUP BY 
        ID, Event_Category
),





#### 커스텀 피쳐 
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

######
# JOIN 및 결과 테이블 생성
SELECT 
    v.ID,
    COALESCE(t.tap_go__bf_list, 0) AS tap_go__bf_list,
    COALESCE(g.bf_offerwall, 0) AS bf_offerwall,
    COALESCE(c.coin_reward, 0) AS coin_reward,
    COALESCE(r.cp_reward_bf, 0) AS cp_reward_bf,
    COALESCE(rc.chg_reward, 0) AS chg_reward,
    COALESCE(rct.chg_reward_cnt, 0) AS chg_reward_cnt,
    COALESCE(jc.chg_join, 0) AS chg_join,
    COALESCE(rp.cps_reward, 0) AS cps_reward,
    COALESCE(bt.bongtu_reward,0) AS bongtu_reward,
    COALESCE(ofad.adPopcorn_offerwall_reward,0) AS adPopcorn_offerwall_reward,
    COALESCE(ofav.avati_offerwall_reward,0) AS avati_offerwall_reward,
    COALESCE(invite_reward,0) AS invite_reward,
    COALESCE(rsp_reward,0) AS rsp_reward,
    COALESCE(cp_reward_homeCash,0) AS cp_reward_homeCash,

    COALESCE(s.spend, 0) AS spend,
    COALESCE(sc.spend_cnt, 0) AS spend_cnt,
    COALESCE(vi.avg_visit_interval, 0) AS avg_visit_interval,
    COALESCE(tv.total_visits, 0) AS total_visits,
    
    COALESCE(ad.adClick_homeChg_ownedBanner,0) as adClick_homeChg_ownedBanner,
    COALESCE(ad.adClick_homeChg_adPopcorn,0) as adClick_homeChg_adPopcorn,
    COALESCE(ad.adClick_homeChg_adPie,0) as adClick_homeChg_adPie,
    COALESCE(ad.adClick_homeChg_mobion,0) as adClick_homeChg_mobion,
    COALESCE(ad.adClick_homeCash_coupang,0) as adClick_homeCash_coupang,
    COALESCE(ad.adClick_homeCash_adPopcorn,0) as adClick_homeCash_adPopcorn,
    COALESCE(ad.adClick_homeCash_adPie,0) as adClick_homeCash_adPie,
    COALESCE(ad.adClick_home_adPopcorn,0) as adClick_home_adPopcorn,
    COALESCE(ad.adClick_homeBottom_owned,0) as adClick_homeBottom_owned,
    COALESCE(ad.adClick_homeBottom_adPopcorn,0) as adClick_homeBottom_adPopcorn,
    COALESCE(ad.adClick_homeBottom_adPie,0) as adClick_homeBottom_adPie,
    COALESCE(ad.adClick_benefit_adPopcorn,0) as adClick_benefit_adPopcorn,
    COALESCE(ad.adClick_purchase_owned,0) as adClick_purchase_owned,
    COALESCE(ad.adClick_chgList_adPopcorn,0) as adClick_chg_adPopcorn,
    COALESCE(ad.adClick_chgList_adPie,0) as adClick_chg_adPie,
    COALESCE(ad.adClick_chg_owned,0) as adClick_chg_owned,
    COALESCE(ad.adClick_chg_adPopcorn,0) as adClick_chg_adPopcorn,
    COALESCE(ad.adClick_info_adPopcorn,0) as adClick_info_adPopcorn,
    COALESCE(ad.adClick_exit_adPie,0) as adClick_exit_adPie,
    COALESCE(ad.adClick_benefit_bongtu_adPopcorn,0) as adClick_benefit_bongtu_adPopcorn,
    COALESCE(ad.adClick_benefit_button_adPopcorn,0) as adClick_benefit_button_adPopcorn,
    COALESCE(ad.adClick_purchase_owned,0) as adClick_purchase_owned


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
    adclick_cnt AS ad ON v.ID = ad.ID
LEFT JOIN 
    visit_intervals AS vi ON v.ID = vi.ID
LEFT JOIN 
    total_visits AS tv ON v.ID = tv.ID
LEFT JOIN 
    rwd_bongtu AS bt ON v.ID = bt.ID
LEFT JOIN 
    rwd_offerwall_adpopcorn AS ofad ON v.ID = ofad.ID
LEFT JOIN 
    rwd_offerwall_avati AS ofav ON v.ID = ofav.ID
LEFT JOIN 
    rwd_invite AS ivt ON v.ID = ivt.ID
LEFT JOIN 
    rwd_rsp AS rsp ON v.ID = rsp.ID
LEFT JOIN 
    rwd_coupang_homeCash AS cphc ON v.ID = cphc.ID