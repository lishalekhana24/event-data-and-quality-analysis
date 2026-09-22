# Event Data Quality & Analysis

## Objective

Analyze event data using Apache Spark and check whether the data is clean.

## Dataset

The dataset contains:

- event_id
- user_id
- event_type
- event_date
- duration_seconds
- device

There are **20 records** in the original dataset.

## Data Quality Issues

The dataset contains:

- One duplicate `event_id` (105)
- 3 missing `duration_seconds` values
- Inconsistent uppercase/lowercase values in `event_type` and `device`

## Data Cleaning

- Removed duplicate records using `event_id`.
- Converted `event_type` and `device` to lowercase.
- Replaced missing duration values with the median duration of **20 seconds**.

After cleaning, there are **19 records** and no missing values.

## Analysis

The project calculates:

- Event count and average duration by event type
- Event count by date
- Spark execution plan using `explain()`

## Technologies

- Apache Spark 3.5.6
- Scala 2.12.18
- SBT
- Ubuntu / WSL
