SELECT * FROM tenants WHERE created_at > $1 ORDER BY created_at ASC LIMIT 1000;
