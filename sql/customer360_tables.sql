CREATE TABLE IF NOT EXISTS mart_customer_search_trend (
    user_id VARCHAR(50),
    most_search_t6 VARCHAR(255),
    count_t6 INT,
    category_t6 VARCHAR(255),
    most_search_t7 VARCHAR(255),
    count_t7 INT,
    category_t7 VARCHAR(255),
    keyword_changed_flag VARCHAR(10),
    category_shift_flag VARCHAR(20),
    category_transition VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS mart_customer_content_profile (
    contract VARCHAR(50),
    total_giai_tri DOUBLE,
    total_phim_truyen DOUBLE,
    total_the_thao DOUBLE,
    total_thieu_nhi DOUBLE,
    total_truyen_hinh DOUBLE,
    most_watch_type VARCHAR(100),
    taste_profile VARCHAR(255),
    activity_level VARCHAR(20),
    active_days INT
);

CREATE TABLE IF NOT EXISTS mart_customer360_profile (
    user_id VARCHAR(50),
    contract VARCHAR(50),
    most_search_t6 VARCHAR(255),
    count_t6 INT,
    category_t6 VARCHAR(255),
    most_search_t7 VARCHAR(255),
    count_t7 INT,
    category_t7 VARCHAR(255),
    keyword_changed_flag VARCHAR(10),
    category_shift_flag VARCHAR(20),
    category_transition VARCHAR(255),
    most_watch_type VARCHAR(100),
    taste_profile VARCHAR(255),
    activity_level VARCHAR(20),
    active_days INT,
    total_giai_tri DOUBLE,
    total_phim_truyen DOUBLE,
    total_the_thao DOUBLE,
    total_thieu_nhi DOUBLE,
    total_truyen_hinh DOUBLE
);
