DELETE FROM locks WHERE resource_id = $1 AND nonce = $2;
