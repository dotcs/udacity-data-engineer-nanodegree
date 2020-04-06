# Table "public.dim_subreddit"

```
            Column            |            Type             | Collation | Nullable | Default 
------------------------------+-----------------------------+-----------+----------+---------
 subreddit_id                 | character varying(256)      |           | not null | 
 accounts_active              | integer                     |           |          | 
 accounts_active_is_fuzzed    | boolean                     |           |          | 
 active_user_count            | integer                     |           |          | 
 advertiser_category          | character varying(256)      |           |          | 
 all_original_content         | boolean                     |           |          | 
 allow_discovery              | boolean                     |           |          | 
 allow_images                 | boolean                     |           |          | 
 allow_videogifs              | boolean                     |           | not null | 
 allow_videos                 | boolean                     |           | not null | 
 can_assign_link_flair        | boolean                     |           | not null | 
 can_assign_user_flair        | boolean                     |           | not null | 
 comment_score_hide_mins      | integer                     |           |          | 
 community_icon               | character varying(256)      |           |          | 
 created                      | timestamp without time zone |           | not null | 
 description                  | character varying(65535)    |           |          | 
 display_name                 | character varying(256)      |           | not null | 
 display_name_prefixed        | character varying(256)      |           | not null | 
 emojis_enabled               | boolean                     |           | not null | 
 free_form_reports            | boolean                     |           |          | 
 header_img                   | character varying(256)      |           |          | 
 header_title                 | character varying(65535)    |           |          | 
 hide_ads                     | boolean                     |           |          | 
 key_color                    | character varying(256)      |           |          | 
 lang                         | character varying(256)      |           |          | 
 name                         | character varying(256)      |           | not null | 
 notification_level           | character varying(256)      |           |          | 
 original_content_tag_enabled | boolean                     |           |          | 
 over_18                      | boolean                     |           |          | 
 primary_color                | character varying(256)      |           |          | 
 public_description           | character varying(65535)    |           |          | 
 public_traffic               | boolean                     |           |          | 
 quarantine                   | boolean                     |           |          | 
 show_media                   | boolean                     |           |          | 
 show_media_preview           | boolean                     |           |          | 
 spoilers_enabled             | boolean                     |           |          | 
 submission_type              | character varying(256)      |           |          | 
 submit_link_label            | character varying(256)      |           |          | 
 submit_text                  | character varying(65535)    |           |          | 
 submit_text_label            | character varying(256)      |           |          | 
 subreddit_type               | character varying(256)      |           | not null | 
 subscribers                  | integer                     |           |          | 
 suggested_comment_sort       | character varying(256)      |           |          | 
 title                        | character varying(65535)    |           | not null | 
 url                          | character varying(256)      |           | not null | 
 videostream_links_count      | integer                     |           |          | 
 whitelist_status             | character varying(256)      |           |          | 
 wiki_enabled                 | boolean                     |           |          | 
 wls                          | integer                     |           |          | 
Indexes:
    "dim_subreddit_pkey" PRIMARY KEY, btree (subreddit_id)
```

Description (most relevant fields only):

- `subreddit_id`: Unique identifier of the subreddit (= lowercase `display_name` value)
- `created`: Date when the subreddit was created
- `description`: Description of the subreddit.
- `display_name`: Name of the subreddit as shown to the users
- `display_name_prefixed`: Same as `display_name` but prefixed with `r/`
- `over_18`: True if this subreddit contains adult content
- `lang`: Default language in which content in this subreddit is published
- `public_description`: Tagline of the subreddit
- `subreddit_type`: State in which the subreddit can be, e.g. 'archived'
- `whitelist_status`: Internal flag that determines which ads are shown to users
