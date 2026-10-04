ALTER TABLE jobs ALTER COLUMN max_attempts SET DEFAULT 3;
UPDATE jobs SET max_attempts = GREATEST(3, attempt_count + 1) WHERE max_attempts = 1;
