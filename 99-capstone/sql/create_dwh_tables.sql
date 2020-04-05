CREATE TABLE IF NOT EXISTS fact_submission (
    "submission_id" text PRIMARY KEY sortkey,
    "author_id" text,

    "archived" boolean NULL,
    "can_gild" boolean NOT NULL,
    "can_mod_post" boolean NOT NULL,
    "category" text NULL,
    "contest_mode" boolean NOT NULL,
    "created" timestamp NOT NULL,
    "discussion_type" text NULL,
    "domain" text NOT NULL,
    "edited" boolean NOT NULL,  -- needs to be transformed
    "event_end" timestamp NULL,  -- take care of null
    "event_is_live" boolean,  -- null needs to be transformed to false
    "event_start" timestamp NULL,  -- take care of null
    "gilded" int NOT NULL,
    "hidden" boolean NOT NULL,
    -- "id" text NOT NULL,
    "is_crosspostable" boolean NOT NULL,
    "is_meta" boolean NOT NULL,
    "is_original_content" boolean NOT NULL,
    "is_reddit_media_domain" boolean NOT NULL,
    "is_robot_indexable" boolean NOT NULL,
    "is_self" boolean NOT NULL,
    "is_video" boolean NOT NULL,
    "locked" boolean NOT NULl,
    "no_follow" boolean NOT NULL,
    "num_comments" int NOT NULL,
    "num_crossposts" int NOT NULL,
    "over_18" boolean NOT NULL,
    "permalink" text NOT NULL,
    "pinned" boolean NOT NULL,
    "post_hint" text NULL,
    "quarantine" boolean NOT NULL,
    "removal_reason" text NULL,
    -- "retrieved_on" int,
    "score" int NOT NULL,
    "selftext" varchar(max) NOT NULL,
    "spoiler" boolean NOT NULL,
    "stickied" boolean NOT NULL,
    "suggested_sort" text NOT NULL,  -- replace null with 'default'
    "thumbnail" text NOT NULL,
    "thumbnail_height" int NULL,
    "thumbnail_width" int NULL,
    "title" varchar(max) NOT NULL,
    "total_awards_received" int NOT NULL,
    "url" varchar(max) NOT NULL,
    "whitelist_status" text NOT NULL -- replace null with 'default' 
);

CREATE TABLE IF NOT EXISTS dim_author (
    "author_id" TEXT NOT NULL sortkey,
    "fullname" TEXT,  -- only available in submission dataset
    "created" TIMESTAMP NOT NULL,
    "karma" INT -- only available in author dataset
);

CREATE TABLE IF NOT EXISTS dim_subreddit (
    "subreddit_id" text NOT NULL,

    "accounts_active" int NULL,
    "accounts_active_is_fuzzed" boolean NULL,
    "active_user_count" int NULL,
    "advertiser_category" text NULL,
    "all_original_content" boolean NULL,
    "allow_discovery" boolean NULL,
    "allow_images" boolean NULL,
    "allow_videogifs" boolean NOT NULL,
    "allow_videos" boolean NOT NULL,
    "can_assign_link_flair" boolean NOT NULL,
    "can_assign_user_flair" boolean NOT NULL,
    "comment_score_hide_mins" int NULL,
    "community_icon" varchar NULL,
    "created" timestamp NOT NULL,
    "description" varchar(max) NULL,
    "display_name" varchar NOT NULL,
    "display_name_prefixed" varchar NOT NULL,
    "emojis_enabled" boolean NOT NULL,
    "free_form_reports" boolean NULL,
    "header_img" varchar NULL,
    "header_title" varchar(max) NULL,
    "hide_ads" boolean NULL,
    -- "id" varchar(max),
    "key_color" text NULL,
    "lang" text NULL,
    "name" text NOT NULL,
    "notification_level" text NULL,
    "original_content_tag_enabled" boolean NULL,
    "over18" boolean NULL,
    "primary_color" text NULL,
    "public_description" varchar(max) NULL,
    "public_traffic" boolean NULL,
    "quarantine" boolean NULL,
    "show_media" boolean NULL,
    "show_media_preview" boolean NULL,
    "spoilers_enabled" boolean NULL,
    "submission_type" text NULL,
    "submit_link_label" text NULL,
    "submit_text" varchar(max) NULL,
    "submit_text_label" text NULL,
    "subreddit_type" text NOT NULL,
    "subscribers" int NULL,
    "suggested_comment_sort" text NULL,
    "title" varchar(max) NOT NULL,
    "url" text NOT NULL,
    "videostream_links_count" int NULL,
    "whitelist_status" text NULL,
    "wiki_enabled" boolean NULL,
    "wls" int NULL
);

CREATE TABLE IF NOT EXISTS dim_time (
    "start_time" TIMESTAMP PRIMARY KEY sortkey,
    "hour" INT NOT NULL,
    "day" INT NOT NULL,
    "week" INT NOT NULL,
    "month" INT NOT NULL,
    "year" INT NOT NULL,
    "weekday" INT NOT NULL
);

