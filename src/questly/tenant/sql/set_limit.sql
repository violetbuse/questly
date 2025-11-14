UPDATE tenants
SET per_day_limit = $2
WHERE id = $1;
