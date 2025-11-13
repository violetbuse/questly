UPDATE tenants
SET tokens = tokens + $2
WHERE
  id = $1 AND
  tokens < per_day_limit;
