DELETE FROM locks WHERE expires_at < $1;
