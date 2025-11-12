UPDATE locks
SET expires_at = $3
WHERE resource_id = $1 AND nonce = $2;
