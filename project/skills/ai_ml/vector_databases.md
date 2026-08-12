# Skill: Vector Databases

## Overview
Storing and querying high-dimensional embeddings for semantic search, RAG, and recommendation systems.

## Key Patterns

### How Vector Search Works
1. Convert data (text, image, audio) to embedding vector via an embedding model
2. Store vector alongside metadata in a vector index
3. At query time, embed the query and find nearest neighbours by similarity
4. Return top-k most similar items ranked by distance score

### Similarity Metrics
| Metric | Best For |
|---|---|
| Cosine similarity | Text embeddings (direction, not magnitude) |
| Dot product | When vectors are normalised (same as cosine) |
| L2 (Euclidean) | Image embeddings, spatial data |
| Hamming | Binary embeddings |

### Index Types
- **HNSW (Hierarchical Navigable Small World)** — fast approximate search; high recall; most popular
- **IVF (Inverted File Index)** — partition space into clusters; good for very large datasets
- **Flat (brute force)** — exact search; slow at scale; useful for small datasets or ground truth

### Filtering
- Pre-filter — apply metadata filter before vector search (may miss relevant results)
- Post-filter — vector search first, then filter results (may return fewer than k)
- In-filter — filter integrated into index traversal (best accuracy, harder to implement)

### Hybrid Search
- Combine vector similarity with keyword (BM25) scoring
- Reciprocal Rank Fusion (RRF) merges ranked lists from both methods
- Better recall than either method alone — especially for rare or domain-specific terms

## Best Practices
- Choose embedding dimensions based on accuracy vs storage trade-off (1536 vs 384)
- Store metadata alongside vectors for filtering without a separate lookup
- Re-embed when switching embedding models (existing vectors become incompatible)
- Use namespaces or collections to isolate data by tenant or use case
- Benchmark recall@k against a labelled test set before selecting an index type

## Common Pitfalls
- Using different embedding models at index time vs query time
- Not filtering on metadata — full index scan for every query
- Ignoring vector index warmup time after cold start
- Storing raw text separately — causes sync issues; store alongside vectors

## Tools
- **Pinecone** — managed, serverless, production-ready
- **Weaviate** — open-source, hybrid search, multi-modal
- **Qdrant** — open-source, Rust-based, rich filtering
- **Chroma** — lightweight, local-first, great for prototyping
- **pgvector** — Postgres extension; good for existing Postgres users
- **Databricks Vector Search** — integrated with Delta Lake and MLflow
