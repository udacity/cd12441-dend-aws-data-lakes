# Exercise 2: CDC with Bookmarks

## Overview
Implement Change Data Capture (CDC) using bookmarks to incrementally load only new or updated records from PostgreSQL.

## Learning Objectives
- Implement CDC patterns using timestamp bookmarks
- Build incremental queries with WHERE clauses
- Track max timestamp for next run
- Use append mode for incremental loads
- Test CDC by updating source data

## Prerequisites
- Exercise 1 completed
- PostgreSQL orders table with `updated_at` column
- Understanding of incremental data loading

## Instructions
1. Complete `starter/cdc_bookmarks_starter.py`
2. Build incremental query using bookmark parameter
3. Calculate max updated_at for next bookmark
4. Write data in append mode
5. Test by updating PostgreSQL and re-running

## Expected Outcomes
- Only new/updated records loaded
- Bookmark tracking for next run
- Efficient incremental loading without full scans
