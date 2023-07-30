SELECT
  CAST(token_address as STRING) as token_address,
  CAST(symbol as STRING) as symbol,
  CAST(name as STRING) as name,
  CAST(decimals as STRING) as decimals,
  CAST(total_supply as STRING) as total_supply,
  CAST(from_address as STRING) as from_address,
  CAST(to_address as STRING) as to_address,
  CAST("value" as STRING) as eth_value,
  CAST(transaction_hash as STRING) as transaction_hash,
  CAST(log_index as INTEGER) as log_index,
  CAST(tt.block_timestamp as TIMESTAMP) as block_timestamp,
  CAST(tt.block_number as INTEGER) as block_number,
  CAST(tt.block_hash as STRING) as block_hash,
FROM {BQ_TOKEN_TRANSFER_SOURCE} as tt
JOIN {BQ_TOKENS_SOURCE} as t
ON tt.token_address = t.address
WHERE
DATE(tt.block_timestamp) = '{DS}'
AND token_address IS NOT NULL
AND transaction_hash IS NOT NULL
AND log_index IS NOT NULL
AND tt.block_timestamp IS NOT NULL
AND tt.block_number IS NOT NULL
AND tt.block_hash IS NOT NULL
AND token_address LIKE '0x%'
AND tt.block_number > 0
AND log_index > 0
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
LIMIT { "{{ params.query_limit }}" }