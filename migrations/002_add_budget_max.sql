-- Add budget_max column for storing the upper bound of budget ranges
ALTER TABLE leads ADD COLUMN IF NOT EXISTS budget_max NUMERIC(15, 2);
