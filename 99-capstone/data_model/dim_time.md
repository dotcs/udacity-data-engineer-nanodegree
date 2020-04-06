# Table "public.dim_time"

```
   Column   |            Type             | Collation | Nullable | Default 
------------+-----------------------------+-----------+----------+---------
 start_time | timestamp without time zone |           | not null | 
 hour       | integer                     |           | not null | 
 day        | integer                     |           | not null | 
 week       | integer                     |           | not null | 
 month      | integer                     |           | not null | 
 year       | integer                     |           | not null | 
 weekday    | integer                     |           | not null | 
Indexes:
    "dim_time_pkey" PRIMARY KEY, btree (start_time)
```

Description:

- `start_time`: Datetime as a timestamp value (without timezone, implicitly UTC)
- `hour`: Hour of the day
- `day`: Day
- `week`: Number of the week within the year
- `month`: Month
- `year`: Year
- `weekday`: Number of the day in the week (from 0–6, starting with Sunday)
