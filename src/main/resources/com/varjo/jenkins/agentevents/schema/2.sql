-- An allocation's duration is released_at - allocated_at.
ALTER TABLE allocations DROP COLUMN duration_ms;
