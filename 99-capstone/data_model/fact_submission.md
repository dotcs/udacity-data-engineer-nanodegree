# Table "public.fact_submission"

```
         Column         |            Type             | Collation | Nullable | Default 
------------------------+-----------------------------+-----------+----------+---------
 submission_id          | character varying(256)      |           | not null | 
 author_id              | character varying(256)      |           | not null | 
 subreddit_id           | character varying(256)      |           | not null | 
 archived               | boolean                     |           |          | 
 can_gild               | boolean                     |           | not null | 
 can_mod_post           | boolean                     |           | not null | 
 category               | character varying(256)      |           |          | 
 contest_mode           | boolean                     |           | not null | 
 created                | timestamp without time zone |           | not null | 
 discussion_type        | character varying(256)      |           |          | 
 domain                 | character varying(256)      |           | not null | 
 edited                 | boolean                     |           | not null | 
 event_end              | timestamp without time zone |           |          | 
 event_is_live          | boolean                     |           |          | 
 event_start            | timestamp without time zone |           |          | 
 gilded                 | integer                     |           | not null | 
 hidden                 | boolean                     |           | not null | 
 is_crosspostable       | boolean                     |           | not null | 
 is_meta                | boolean                     |           | not null | 
 is_original_content    | boolean                     |           | not null | 
 is_reddit_media_domain | boolean                     |           | not null | 
 is_robot_indexable     | boolean                     |           | not null | 
 is_self                | boolean                     |           | not null | 
 is_video               | boolean                     |           | not null | 
 locked                 | boolean                     |           | not null | 
 no_follow              | boolean                     |           | not null | 
 num_comments           | integer                     |           | not null | 
 num_crossposts         | integer                     |           | not null | 
 over_18                | boolean                     |           | not null | 
 permalink              | character varying(256)      |           | not null | 
 pinned                 | boolean                     |           | not null | 
 post_hint              | character varying(256)      |           |          | 
 quarantine             | boolean                     |           | not null | 
 removal_reason         | character varying(256)      |           |          | 
 score                  | integer                     |           | not null | 
 selftext               | character varying(65535)    |           | not null | 
 spoiler                | boolean                     |           | not null | 
 stickied               | boolean                     |           | not null | 
 suggested_sort         | character varying(256)      |           | not null | 
 thumbnail              | character varying(256)      |           | not null | 
 thumbnail_height       | integer                     |           |          | 
 thumbnail_width        | integer                     |           |          | 
 title                  | character varying(65535)    |           | not null | 
 total_awards_received  | integer                     |           | not null | 
 url                    | character varying(65535)    |           | not null | 
 whitelist_status       | character varying(256)      |           | not null | 
Indexes:
    "fact_submission_pkey" PRIMARY KEY, btree (submission_id)
```

Description (most relevant only):

- `submission_id`: Unique identifier of the submission (field `id` in the source table)
- `author_id:` Unique username of the author of the submission (Foreign key: `dim_author.author_id`, lowercase)
- `subreddit_id`: Name of the subreddit (Foreign key: `dim_subreddit.subreddit_id`, lowercase)
- `category`: Defines the category of the submission
- `hidden`: Defines if a post is visible to users or not
- `is_self`: True if a submission is a textual post (not a media link)
- `locked`: True if the submission is locked for any changes by the user
- `over_18`: True if the submission contains adult only content
- `score`: Numeric value that defines how the submission has been rated by others
- `selftext`: If the content is a non-media post (`is_self == True` in this case) this field contains the text of the post
- `title`: The title of the submission
- `url`: The URL to the content if this is a media post (`is_self == False` in this case)
