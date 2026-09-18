-- name: IndexEmbedding :exec
INSERT INTO knowledge.chunk_embeddings(chunk_id,model,vector,dimensions) VALUES($1,$2,$3,$4)
ON CONFLICT(chunk_id,model) DO UPDATE SET vector=excluded.vector,dimensions=excluded.dimensions;
