# Skill: MLOps Patterns

## Overview
Operationalising machine learning models: training pipelines, deployment, monitoring, and lifecycle management.

## Key Patterns

### ML Pipeline Stages
```
Data Ingestion → Feature Engineering → Model Training → Evaluation → Registry → Deployment → Monitoring
```

### Feature Stores
- Centralised repository of reusable features with point-in-time correctness
- Online store (low-latency, key-value) for serving; offline store (columnar) for training
- Prevents training-serving skew — same features used at train and inference time
- Tools: Feast, Tecton, Hopsworks, Databricks Feature Store

### Experiment Tracking
- Log parameters, metrics, and artefacts for every training run
- Compare runs to find best model configuration
- Reproduce any past experiment exactly
- Tools: MLflow, Weights & Biases, Comet ML

### Model Registry
- Versioned storage of trained model artefacts
- Stages: `Staging` → `Production` → `Archived`
- Metadata: training dataset version, evaluation metrics, owner
- Enables rollback to previous model version

### Model Serving
| Pattern | Latency | Use Case |
|---|---|---|
| REST API (FastAPI/Flask) | Medium | General inference |
| Batch scoring | High | Offline predictions |
| Streaming (Kafka + model) | Low | Real-time event scoring |
| Serverless (Lambda/Cloud Run) | Variable | Sporadic traffic |
| Edge deployment | Lowest | On-device inference |

### Model Monitoring
- **Data drift** — input distribution shifts vs training data
- **Concept drift** — relationship between features and target changes
- **Model performance** — degradation in accuracy, precision, recall
- **Infrastructure** — latency, throughput, error rates

## Best Practices
- Treat ML code like software — version control, code review, CI/CD
- Automate retraining triggers on data drift detection
- Shadow deploy new models alongside production before full cut-over
- A/B test model versions with traffic splitting
- Document model cards: purpose, training data, limitations, bias analysis

## Common Pitfalls
- Training-serving skew — different preprocessing at train and inference time
- No monitoring after deployment — model silently degrades
- Manual deployment steps — hard to reproduce, audit, or rollback
- Ignoring data quality issues upstream of the model

## Tools
- **MLflow** — experiment tracking, model registry, serving
- **Weights & Biases** — experiment tracking and collaboration
- **Seldon / BentoML / Ray Serve** — model serving
- **Evidently AI** — data and model drift monitoring
- **Kubeflow / Vertex AI Pipelines** — ML pipeline orchestration
