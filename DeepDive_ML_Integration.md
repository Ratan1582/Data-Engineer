# ML Integration in Data Pipelines -- A Complete Deep Dive

**From Feature Engineering to Batch Inference at Scale**

---

## Table of Contents

1. [Why ML in Data Pipelines](#1-why-ml-in-data-pipelines)
2. [Feature Engineering with Spark](#2-feature-engineering-with-spark)
3. [Spark MLlib Overview](#3-spark-mllib-overview)
4. [Feature Store Concepts](#4-feature-store-concepts)
5. [Batch Inference Patterns](#5-batch-inference-patterns)
6. [pandas_udf for ML Inference](#6-pandas_udf-for-ml-inference)
7. [MLflow Deep Dive](#7-mlflow-deep-dive)
8. [Model Versioning and Promotion](#8-model-versioning-and-promotion)
9. [End-to-End ML Pipeline Architecture](#9-end-to-end-ml-pipeline-architecture)
10. [Model Drift and Monitoring](#10-model-drift-and-monitoring)
11. [PyTorch + Spark Integration](#11-pytorch--spark-integration)
12. [Anti-Patterns](#12-anti-patterns)
13. [Quick-Reference Cheat Sheet](#13-quick-reference-cheat-sheet)

---

## 1. Why ML in Data Pipelines

### 1.1 The Data Engineer's Role in ML

Data engineers don't typically train ML models, but they are responsible for:
- **Feature engineering**: Computing and serving features at scale
- **Batch inference**: Running trained models on large datasets
- **Pipeline orchestration**: Scheduling training/inference pipelines
- **Data quality**: Ensuring training data is clean and consistent
- **Model serving infrastructure**: Deploying models and monitoring their performance

### 1.2 ML in the Data Pipeline

```
Raw Data → ETL → Feature Store → Training Pipeline → Model Registry
                      ↓                                     ↓
              Inference Pipeline ← Load Production Model ←──┘
                      ↓
              Predictions → Business Applications
```

---

## 2. Feature Engineering with Spark

### 2.1 What Are Features

Features are the input variables that an ML model uses to make predictions. Raw data must be transformed into features that the model can understand.

```python
# Raw data
| device_id | timestamp           | voltage | current | temperature |
| D001      | 2024-01-15 10:00:00 | 230.5   | 5.2     | 45.3        |

# Engineered features
| device_id | hour_of_day | voltage_zscore | avg_current_24h | temp_rolling_std_7d | is_peak_hour |
| D001      | 10          | 0.52           | 4.8             | 2.1                 | 1            |
```

### 2.2 Common Feature Engineering Operations

**Temporal features**:
```python
df = df.withColumn("hour", hour(col("timestamp")))
df = df.withColumn("day_of_week", dayofweek(col("timestamp")))
df = df.withColumn("is_weekend", when(dayofweek(col("timestamp")).isin(1, 7), 1).otherwise(0))
df = df.withColumn("month", month(col("timestamp")))
df = df.withColumn("is_peak_hour", when(col("hour").between(9, 17), 1).otherwise(0))
```

**Aggregation features (window functions)**:
```python
from pyspark.sql.window import Window

w_24h = Window.partitionBy("device_id").orderBy("timestamp").rangeBetween(-86400, 0)
w_7d = Window.partitionBy("device_id").orderBy("timestamp").rangeBetween(-604800, 0)

df = df.withColumn("avg_voltage_24h", avg("voltage").over(w_24h))
df = df.withColumn("max_current_24h", max("current").over(w_24h))
df = df.withColumn("std_temp_7d", stddev("temperature").over(w_7d))
df = df.withColumn("reading_count_24h", count("*").over(w_24h))
```

**Statistical features**:
```python
# Z-score normalization per device
device_stats = df.groupBy("device_id").agg(
    avg("voltage").alias("mean_voltage"),
    stddev("voltage").alias("std_voltage")
)
df = df.join(device_stats, "device_id")
df = df.withColumn("voltage_zscore", 
    (col("voltage") - col("mean_voltage")) / col("std_voltage"))
```

**Lag features**:
```python
w = Window.partitionBy("device_id").orderBy("timestamp")
df = df.withColumn("prev_voltage", lag("voltage", 1).over(w))
df = df.withColumn("voltage_change", col("voltage") - col("prev_voltage"))
df = df.withColumn("prev_2_voltage", lag("voltage", 2).over(w))
```

**Categorical encoding**:
```python
from pyspark.ml.feature import StringIndexer, OneHotEncoder

indexer = StringIndexer(inputCol="device_type", outputCol="device_type_idx")
encoder = OneHotEncoder(inputCol="device_type_idx", outputCol="device_type_vec")
```

### 2.3 MLlib Feature Transformers

```python
from pyspark.ml.feature import VectorAssembler, StandardScaler, MinMaxScaler

# Combine multiple columns into a single feature vector
assembler = VectorAssembler(
    inputCols=["voltage_zscore", "avg_current_24h", "temp_rolling_std", "hour", "is_peak"],
    outputCol="features"
)
df = assembler.transform(df)

# Standardize features (zero mean, unit variance)
scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withMean=True, withStd=True)
scaler_model = scaler.fit(df)
df = scaler_model.transform(df)
```

---

## 3. Spark MLlib Overview

### 3.1 Pipeline API

MLlib uses a pipeline abstraction similar to scikit-learn:

```python
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# Define pipeline stages
indexer = StringIndexer(inputCol="category", outputCol="category_idx")
assembler = VectorAssembler(inputCols=["feature1", "feature2", "category_idx"], outputCol="features")
rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=100)

pipeline = Pipeline(stages=[indexer, assembler, rf])

# Train
model = pipeline.fit(train_df)

# Predict
predictions = model.transform(test_df)

# Evaluate
evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
auc = evaluator.evaluate(predictions)
```

### 3.2 Key MLlib Algorithms

| Category | Algorithms |
|----------|-----------|
| **Classification** | Logistic Regression, Random Forest, GBT, SVM, Naive Bayes |
| **Regression** | Linear Regression, Random Forest, GBT, Decision Tree |
| **Clustering** | K-Means, Bisecting K-Means, GMM, LDA |
| **Recommendation** | ALS (Alternating Least Squares) |
| **Feature** | TF-IDF, Word2Vec, PCA, StandardScaler, Bucketizer |

### 3.3 Cross-Validation

```python
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

paramGrid = ParamGridBuilder() \
    .addGrid(rf.numTrees, [50, 100, 200]) \
    .addGrid(rf.maxDepth, [5, 10, 15]) \
    .build()

crossval = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=paramGrid,
    evaluator=evaluator,
    numFolds=3
)

cv_model = crossval.fit(train_df)
best_model = cv_model.bestModel
```

---

## 4. Feature Store Concepts

### 4.1 What Is a Feature Store

A feature store is a centralized repository for storing and serving ML features. It solves:
- **Feature reuse**: Same features used across multiple models
- **Consistency**: Training and inference use the same feature computation
- **Point-in-time correctness**: Features for training must reflect what was known at that time (no data leakage)
- **Discovery**: Data scientists can search and reuse existing features

### 4.2 Architecture

```
Feature Computation (Spark)
        ↓
Feature Store (Offline: Delta Lake, Online: Redis/DynamoDB)
        ↓                        ↓
Training Pipeline          Inference Pipeline
(reads historical features) (reads latest features)
```

**Offline store**: Stores historical features for training. Usually a data lake (Delta Lake, Parquet).
**Online store**: Stores the latest feature values for low-latency inference. Usually a key-value store (Redis, DynamoDB, Cassandra).

### 4.3 Point-in-Time Correctness

When building training data, you must join features as they existed **at the time of the label event**, not as they exist today:

```python
# WRONG: feature leakage (using future data to predict past events)
features_current = feature_store.read_latest()
training_data = labels.join(features_current, "device_id")

# RIGHT: point-in-time join
training_data = feature_store.read_as_of(labels, timestamp_col="event_time")
# For each label event, gets the feature values that existed at event_time
```

### 4.4 Databricks Feature Store

```python
from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()

# Create feature table
fs.create_table(
    name="production.features.device_features",
    primary_keys=["device_id"],
    timestamp_keys=["timestamp"],
    df=feature_df
)

# Read features for training
training_set = fs.create_training_set(
    df=labels_df,
    feature_lookups=[
        FeatureLookup(table_name="production.features.device_features", 
                      lookup_key="device_id",
                      timestamp_lookup_key="event_time")
    ],
    label="is_anomaly"
)
training_df = training_set.load_df()
```

---

## 5. Batch Inference Patterns

### 5.1 Basic Batch Inference

```python
import mlflow

# Load production model
model = mlflow.spark.load_model("models:/anomaly_detector/Production")

# Score entire dataset
predictions = model.transform(feature_df)

# Write predictions
predictions.select("device_id", "timestamp", "prediction", "probability") \
    .write.format("delta").mode("overwrite").save("/data/predictions/")
```

### 5.2 Broadcast Model Pattern

For non-Spark models (scikit-learn, XGBoost), broadcast the model to all executors:

```python
import mlflow
from pyspark.sql.functions import pandas_udf

# Load model on driver
model = mlflow.sklearn.load_model("models:/anomaly_detector/Production")

# Broadcast to all executors
model_broadcast = spark.sparkContext.broadcast(model)

@pandas_udf("double")
def predict(feature1: pd.Series, feature2: pd.Series, feature3: pd.Series) -> pd.Series:
    model = model_broadcast.value
    X = pd.DataFrame({"f1": feature1, "f2": feature2, "f3": feature3})
    return pd.Series(model.predict(X))

result = df.withColumn("prediction", predict(col("f1"), col("f2"), col("f3")))
```

### 5.3 Inference at Scale: Partitioned Processing

```python
# For very large datasets, process in partitions
dates = df.select("date").distinct().collect()

for row in dates:
    date = row["date"]
    partition_df = df.filter(col("date") == date)
    predictions = model.transform(partition_df)
    predictions.write.format("delta").mode("append") \
        .option("replaceWhere", f"date = '{date}'") \
        .save("/data/predictions/")
```

---

## 6. pandas_udf for ML Inference

### 6.1 Iterator Pattern (Recommended)

Load the model once, apply to many batches:

```python
from typing import Iterator

@pandas_udf("double")
def predict_udf(batch_iter: Iterator[Tuple[pd.Series, pd.Series]]) -> Iterator[pd.Series]:
    model = load_model("/models/production/anomaly_v2")
    
    for feature1, feature2 in batch_iter:
        X = pd.DataFrame({"f1": feature1, "f2": feature2})
        yield pd.Series(model.predict(X))

result = df.withColumn("prediction", predict_udf(col("feature1"), col("feature2")))
```

### 6.2 applyInPandas for Per-Group Inference

```python
def predict_per_device(pdf: pd.DataFrame) -> pd.DataFrame:
    model = load_model("/models/production/")
    X = pdf[["voltage", "current", "temperature"]]
    pdf["anomaly_score"] = model.predict_proba(X)[:, 1]
    return pdf

schema = "device_id string, timestamp timestamp, voltage double, current double, temperature double, anomaly_score double"
result = df.groupBy("device_id").applyInPandas(predict_per_device, schema=schema)
```

---

## 7. MLflow Deep Dive

### 7.1 Components

| Component | Purpose |
|-----------|---------|
| **Tracking** | Log parameters, metrics, artifacts for each experiment run |
| **Models** | Standard model packaging format (MLflow Model) |
| **Model Registry** | Versioned model store with staging/production stages |
| **Projects** | Reproducible packaging of ML code |

### 7.2 Tracking API

```python
import mlflow

mlflow.set_experiment("/Experiments/device_anomaly_detection")

with mlflow.start_run(run_name="xgboost_v3") as run:
    # Log parameters
    mlflow.log_param("n_estimators", 200)
    mlflow.log_param("max_depth", 8)
    mlflow.log_param("learning_rate", 0.1)
    mlflow.log_param("training_data_path", "/data/features/2024-01/")
    
    # Train
    model = xgb.XGBClassifier(n_estimators=200, max_depth=8, learning_rate=0.1)
    model.fit(X_train, y_train)
    
    # Log metrics
    y_pred = model.predict(X_test)
    mlflow.log_metric("accuracy", accuracy_score(y_test, y_pred))
    mlflow.log_metric("precision", precision_score(y_test, y_pred))
    mlflow.log_metric("recall", recall_score(y_test, y_pred))
    mlflow.log_metric("f1", f1_score(y_test, y_pred))
    mlflow.log_metric("auc", roc_auc_score(y_test, model.predict_proba(X_test)[:, 1]))
    
    # Log model
    mlflow.xgboost.log_model(model, "model", registered_model_name="anomaly_detector")
    
    # Log artifacts (feature importance plot, confusion matrix, etc.)
    mlflow.log_artifact("feature_importance.png")
    mlflow.log_artifact("confusion_matrix.png")
    
    print(f"Run ID: {run.info.run_id}")
```

### 7.3 Model Registry

```python
client = mlflow.tracking.MlflowClient()

# Register a new model version
result = client.create_model_version(
    name="anomaly_detector",
    source=f"runs:/{run_id}/model",
    run_id=run_id
)

# Transition to staging for testing
client.transition_model_version_stage("anomaly_detector", version=4, stage="Staging")

# After validation, promote to production
client.transition_model_version_stage("anomaly_detector", version=4, stage="Production")

# Archive old version
client.transition_model_version_stage("anomaly_detector", version=3, stage="Archived")
```

### 7.4 Model Stages

```
None → Staging → Production → Archived
         ↑           ↑
      (testing)  (validated)
```

**Staging**: Model is being tested against production data.
**Production**: Model is serving live predictions.
**Archived**: Model is no longer in use but preserved for audit.

---

## 8. Model Versioning and Promotion

### 8.1 Promotion Pipeline

```python
def promote_model(model_name, candidate_version):
    client = mlflow.tracking.MlflowClient()
    
    # Load candidate model
    candidate = mlflow.pyfunc.load_model(f"models:/{model_name}/{candidate_version}")
    
    # Load current production model
    try:
        production = mlflow.pyfunc.load_model(f"models:/{model_name}/Production")
    except Exception:
        production = None
    
    # Evaluate both on validation set
    val_data = spark.read.format("delta").load("/data/validation/")
    
    candidate_metrics = evaluate_model(candidate, val_data)
    
    if production:
        production_metrics = evaluate_model(production, val_data)
        if candidate_metrics["f1"] > production_metrics["f1"] * 1.02:  # 2% improvement
            client.transition_model_version_stage(model_name, candidate_version, "Production")
            print(f"Promoted v{candidate_version}: F1 {candidate_metrics['f1']:.4f} > {production_metrics['f1']:.4f}")
        else:
            print(f"Candidate v{candidate_version} did not improve sufficiently")
    else:
        client.transition_model_version_stage(model_name, candidate_version, "Production")
        print(f"First production model: v{candidate_version}")
```

---

## 9. End-to-End ML Pipeline Architecture

### 9.1 Architecture

```
Data Pipeline:
  Raw Data → Bronze → Silver → Gold → Feature Store (offline)
                                          ↓
Training Pipeline (periodic):
  Feature Store → Training Data → Train Model → Evaluate → Register in MLflow
                                                              ↓
Inference Pipeline (daily/hourly):
  Feature Store → Load Production Model → Predict → Write Predictions → Serve
                                                                         ↓
Monitoring:
  Predictions + Actuals → Drift Detection → Alert → Trigger Retraining
```

### 9.2 Orchestration

```python
# Databricks Workflow with ML pipeline
{
    "name": "ml_pipeline",
    "tasks": [
        {"task_key": "feature_engineering", "notebook_task": {"notebook_path": "/ML/features"}},
        {"task_key": "training", "depends_on": [{"task_key": "feature_engineering"}],
         "notebook_task": {"notebook_path": "/ML/train"}},
        {"task_key": "evaluation", "depends_on": [{"task_key": "training"}],
         "notebook_task": {"notebook_path": "/ML/evaluate"}},
        {"task_key": "promote", "depends_on": [{"task_key": "evaluation"}],
         "notebook_task": {"notebook_path": "/ML/promote"},
         "condition_task": {"op": "GREATER_THAN", "left": "{{tasks.evaluation.values.f1}}", "right": "0.85"}}
    ]
}
```

---

## 10. Model Drift and Monitoring

### 10.1 Types of Drift

| Type | What Changes | Example |
|------|-------------|---------|
| **Data drift** | Input feature distributions shift | Average temperature increases in summer |
| **Concept drift** | Relationship between features and target changes | User behavior changes after a policy update |
| **Prediction drift** | Model output distribution shifts | Prediction scores trending higher over time |

### 10.2 Detection Methods

```python
from scipy import stats

def detect_data_drift(reference_df, current_df, columns, threshold=0.05):
    drift_results = {}
    for col_name in columns:
        ref_values = reference_df.select(col_name).toPandas()[col_name].dropna()
        cur_values = current_df.select(col_name).toPandas()[col_name].dropna()
        
        # Kolmogorov-Smirnov test
        ks_stat, p_value = stats.ks_2samp(ref_values, cur_values)
        drift_results[col_name] = {
            "ks_statistic": ks_stat,
            "p_value": p_value,
            "is_drifted": p_value < threshold
        }
    
    return drift_results

# Compare training data distribution with current inference data
drift = detect_data_drift(training_features, current_features, feature_columns)
drifted_columns = [col for col, result in drift.items() if result["is_drifted"]]
if drifted_columns:
    alert(f"Data drift detected in: {drifted_columns}")
```

### 10.3 Retraining Triggers

```
Automatic retraining when:
1. Data drift detected (KS test p-value < threshold)
2. Prediction accuracy drops below threshold (requires ground truth)
3. Scheduled retraining (weekly/monthly regardless of drift)
4. Significant data volume increase (new data patterns)
```

---

## 11. PyTorch + Spark Integration

### 11.1 Batch Inference with PyTorch

```python
import torch

def pytorch_inference(batch_iter: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = torch.load("/models/anomaly_net.pt", map_location=device)
    model.eval()
    
    for pdf in batch_iter:
        features = torch.tensor(pdf[["f1", "f2", "f3"]].values, dtype=torch.float32).to(device)
        with torch.no_grad():
            outputs = model(features)
            scores = torch.sigmoid(outputs).cpu().numpy()
        pdf["anomaly_score"] = scores[:, 0]
        yield pdf[["device_id", "timestamp", "anomaly_score"]]

result = df.mapInPandas(pytorch_inference, schema="device_id string, timestamp timestamp, anomaly_score double")
```

### 11.2 Distributed Training with PyTorch

```python
# Using Horovod or PyTorch Distributed on Spark
from sparkdl import HorovodRunner

def train_fn():
    import horovod.torch as hvd
    hvd.init()
    model = MyModel()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001 * hvd.size())
    optimizer = hvd.DistributedOptimizer(optimizer)
    # ... training loop ...

hr = HorovodRunner(np=4)  # 4 workers
hr.run(train_fn)
```

---

## 12. Anti-Patterns

### 12.1 Training on Shuffled Data

```python
# BAD: random shuffle loses temporal ordering
train, test = df.randomSplit([0.8, 0.2])
# If data has time-series nature, this causes data leakage (future data in training)

# GOOD: temporal split
train = df.filter(col("date") < "2024-01-01")
test = df.filter(col("date") >= "2024-01-01")
```

### 12.2 Feature Leakage

```python
# BAD: feature computed from future data
df = df.withColumn("avg_next_7d", avg("value").over(Window.orderBy("ts").rowsBetween(0, 7)))
# The model "knows" the future during training

# GOOD: only use past data for features
df = df.withColumn("avg_past_7d", avg("value").over(Window.orderBy("ts").rowsBetween(-7, -1)))
```

### 12.3 collect() for Large Predictions

```python
# BAD: collecting millions of predictions to driver
predictions = model.transform(large_df).collect()

# GOOD: write predictions directly to storage
model.transform(large_df).write.format("delta").save("/predictions/")
```

---

## 13. Quick-Reference Cheat Sheet

### ML Pipeline Stages

```
1. Feature Engineering: Spark transformations → Feature Store
2. Training: Feature Store → Train Model → MLflow Tracking
3. Evaluation: Test set → Metrics → Compare with production
4. Registration: MLflow → Model Registry → Staging
5. Promotion: Staging → Validation → Production
6. Inference: Production Model → Batch Predict → Delta Lake
7. Monitoring: Predictions → Drift Detection → Retrain Trigger
```

### Inference Method Selection

```
MLlib model? → model.transform(df) (native Spark)
scikit-learn/XGBoost? → broadcast + pandas_udf
PyTorch? → mapInPandas with model loading in iterator
Per-group inference? → applyInPandas
Real-time (< 100ms)? → Model Serving endpoint
```

### MLflow Commands

```python
mlflow.start_run()                    # start tracking
mlflow.log_param("key", value)        # log parameter
mlflow.log_metric("key", value)       # log metric
mlflow.log_artifact("file.png")       # log file
mlflow.sklearn.log_model(m, "model")  # log model
mlflow.register_model(uri, name)      # register
mlflow.pyfunc.load_model(uri)         # load for inference
```
