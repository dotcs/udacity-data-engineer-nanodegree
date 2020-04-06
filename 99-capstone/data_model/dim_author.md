# Table "public.dim_author"

```
     Column     |            Type             | Collation | Nullable | Default 
----------------+-----------------------------+-----------+----------+---------
 author_id      | character varying(256)      |           | not null | 
 name           | character varying(256)      |           | not null | 
 created        | timestamp without time zone |           |          | 
 karma_posts    | integer                     |           |          | 
 karma_comments | integer                     |           |          | 
 karma          | integer                     |           |          | 
 deleted        | boolean                     |           | not null | 
Indexes:
    "dim_author_pkey" PRIMARY KEY, btree (author_id)
```

Description:

- `author_id`: Unique username of the author (= lowercase name)
- `name`: Username of the author
- `created`: Time when the user account has been created
- `karma_posts`: Karma that the user eared with posts (can be negative)
- `karma_comments`: Karma that the user eared with comments (can be negative)
- `karma`: Sum of karma_posts and karma_comments (can be negative)
- `deleted`: True if the user has been deleted. In this case the columns `created`, `karma_posts`, `karma_comments` and `karma` are `null` values.
