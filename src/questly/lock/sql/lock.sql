INSERT INTO locks (resource_id, nonce, expires_at)
VALUES ($1, $2, $3)
ON CONFLICT DO NOTHING;
