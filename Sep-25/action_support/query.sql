WITH benefit AS (
    SELECT user_seq, 
        COUNT(DISTINCT CASE WHEN benfefit_type = 'LIKE' THEN DATE(reg_dttm) END) AS like_count,
        COUNT(DISTINCT CASE WHEN benfefit_type = 'CARD' THEN DATE(reg_dttm) END) AS card,
        COUNT(DISTINCT CASE WHEN benfefit_type = 'QUIZ' THEN DATE(reg_dttm) END) AS quiz,
        COUNT(DISTINCT CASE WHEN benfefit_type = 'FOLLOW' THEN DATE(reg_dttm) END) AS follow,
        COUNT(DISTINCT CASE WHEN benfefit_type = 'COUPANG' THEN DATE(reg_dttm) END) AS coupang,
        COUNT(DISTINCT CASE WHEN benfefit_type = 'STRETCHING' THEN DATE(reg_dttm) END) AS stretching
    FROM PRD.ODS_RR_LIFE.BENEFIT_REWARD_HIST
    WHERE reg_dttm > current_date() - 31
    GROUP BY user_seq
),
buzzvill AS (
    SELECT user_seq, COUNT(DISTINCT DATE(reg_dttm)) AS buzzvill
    FROM PRD.ODS_RR_LIFE.OFFERWALL_BUZZVILL_REWARD_HIST
    WHERE reg_dttm > current_date() - 31
    GROUP BY user_seq
),
walk AS (
    SELECT user_seq, COUNT(DISTINCT DATE(reg_dttm)) AS walk
    FROM PRD.ODS_RR_LIFE.STEP_REWARD_HIST
    WHERE reg_dttm > current_date() - 31
    GROUP BY user_seq
),
raffle AS (
    SELECT user_seq, COUNT(DISTINCT DATE(reg_dttm)) AS raffle
    FROM PRD.ODS_RR_LIFE.RAFFLE_TICKET
    WHERE reg_dttm > current_date() - 31
    GROUP BY user_seq
),
chg AS (
    SELECT user_seq, COUNT(DISTINCT DATE(verify_dt)) AS chg
    FROM PRD.DM.AGG_CHL_DAILY_REWARD
    WHERE verify_cnt > 0 and verify_dt > current_date() - 31
    GROUP BY user_seq
),
lotto AS (
    SELECT mber_id AS user_seq
        -- ,COUNT(DISTINCT DSBR_DAY) AS lott_total
        ,COUNT(DISTINCT CASE WHEN dsbr_cd_nm = '광고리워드' THEN DSBR_DAY END) AS lott_ad
        -- ,COUNT(DISTINCT CASE WHEN dsbr_cd_nm != '광고리워드' THEN DSBR_DAY END) AS lott_nonad
    FROM PRD.ODS_RR_LOTTO.TB_MBER_LOT
    WHERE DSBR_DAY > current_date() - 31
    GROUP BY user_seq
),
all_users AS (
    SELECT user_seq FROM benefit
    UNION
    SELECT user_seq FROM buzzvill
    UNION
    SELECT user_seq FROM walk
    UNION
    SELECT user_seq FROM raffle
    UNION
    SELECT user_seq FROM chg
    UNION
    SELECT user_seq FROM lotto
)
SELECT 
    a.user_seq,
    COALESCE(b.like_count, 0) AS like_days,
    COALESCE(b.card, 0) AS card_days,
    COALESCE(b.quiz, 0) AS quiz_days,
    COALESCE(b.follow, 0) AS follow_days,
    COALESCE(b.coupang, 0) AS coupang_days,
    COALESCE(b.stretching, 0) AS stretching_days,
    COALESCE(bz.buzzvill, 0) AS buzzvill_days,
    COALESCE(w.walk, 0) AS walk_days,
    COALESCE(r.raffle, 0) AS raffle_days,
    COALESCE(c.chg, 0) AS chg_days,
    -- COALESCE(l.lott_total, 0) AS lott_total,
    COALESCE(l.lott_ad, 0) AS lott_ad,
    -- COALESCE(l.lott_nonad, 0) AS lott_nonad
FROM all_users a
LEFT JOIN benefit b ON a.user_seq = b.user_seq
LEFT JOIN buzzvill bz ON a.user_seq = bz.user_seq
LEFT JOIN walk w ON a.user_seq = w.user_seq
LEFT JOIN raffle r ON a.user_seq = r.user_seq
LEFT JOIN chg c ON a.user_seq = c.user_seq
LEFT JOIN lotto l ON a.user_seq = l.user_seq
WHERE (
    COALESCE(b.like_count, 0) + 
    COALESCE(b.card, 0) + 
    COALESCE(b.quiz, 0) + 
    COALESCE(b.follow, 0) + 
    COALESCE(b.coupang, 0) + 
    COALESCE(b.stretching, 0) + 
    COALESCE(bz.buzzvill, 0) + 
    COALESCE(w.walk, 0) + 
    COALESCE(r.raffle, 0) + 
    COALESCE(c.chg, 0) + 
    -- COALESCE(l.lott_total, 0) + 
    COALESCE(l.lott_ad, 0) 
    -- COALESCE(l.lott_nonad, 0
    
) > 0
ORDER BY a.user_seq;