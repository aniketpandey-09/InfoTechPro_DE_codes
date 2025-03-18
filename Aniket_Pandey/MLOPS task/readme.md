# Iris Dataset Binary Classification with MLflow

## 📌 Project Overview
This project performs **binary classification** on the Iris dataset using **Apache Spark (PySpark) and MLflow** in **Databricks Community Edition**. The goal is to classify whether an Iris flower is **Setosa (1) or Not Setosa (0)**. The workflow includes **data preprocessing, model training, evaluation, logging, deployment, batch inference, and real-time inference.**

---

## 📂 Workflow

### 1️⃣ **Data Preparation**
- The dataset is loaded from **DBFS (`dbfs:/FileStore/irisdata.csv`)**.
- The schema includes **four continuous features** (`sepal_length`, `sepal_width`, `petal_length`, `petal_width`) and **one categorical target (`class`)**.
- The target class is **converted to binary labels**:
  - `Iris-setosa` → **1**
  - `Iris-versicolor` & `Iris-virginica` → **0**
- The feature columns are assembled into a **single vector column (`features`)** using `VectorAssembler`.

### 2️⃣ **Train-Test Split**
- The dataset is randomly split into **70% training data, 30% testing data**.
- `seed=42` ensures reproducibility.

### 3️⃣ **Model Training**
- A **Logistic Regression** model is trained on the **transformed dataset**.
- Features: `sepal_length`, `sepal_width`, `petal_length`, `petal_width`.
- Target: `label` (Binary classification: `1 = Setosa`, `0 = Not Setosa`).

### 4️⃣ **Model Evaluation**
- The model is evaluated using **ROC AUC Score** with `BinaryClassificationEvaluator`.
- If using **multiclass classification**, `MulticlassClassificationEvaluator` should be used instead.

### 5️⃣ **MLflow Model Logging**
- MLflow is used to **log model parameters, metrics, and the trained model**.
- Model is stored under `iris_binary_classification`.
- The logged model is retrieved using the `run_id`.

### 6️⃣ **Batch Inference**
- The trained model is loaded for inference.
- New batch data is **transformed** and passed through the model.
- Predictions are displayed.

### 7️⃣ **Real-Time Inference**
- A function `real_time_inference()` is defined to accept **new feature values**.
- Converts inputs into a DataFrame and transforms them using `VectorAssembler`.
- Uses the loaded MLflow model to predict in real time.

---

## 🚀 Running the Project on Databricks Community Edition

### 🔧 **Prerequisites**
- A **Databricks Community Edition** account.
- Upload `irisdata.csv` to **DBFS (`dbfs:/FileStore/irisdata.csv`)**.
- Install **PySpark and MLflow** (`pip install mlflow pyspark`).

### ▶️ **Steps to Execute**
1. **Open Databricks Notebook**.
2. **Copy and paste the code** into the notebook.
3. **Run the cells step-by-step**.
4. Ensure `irisdata.csv` is uploaded correctly.
5. View **MLflow experiment results** under `/Users/<your-email>/iris_binary_classification`.
6. Perform **batch and real-time inference**.

---

## ⚠️ Limitations & Future Improvements

### ❌ **Limitations**
- The model is **binary classification** (`Iris-setosa` vs. Others). A **multiclass model** should be used for full classification.
- **Overfitting** may occur since the dataset is small (**150 samples**).
- MLflow **Model Registry is not enabled in Databricks Community Edition**, so the model is logged but not registered.

### 🔄 **Future Enhancements**
✅ Upgrade to **Multiclass Classification** (predicting `Setosa`, `Versicolor`, `Virginica`).  
✅ Hyperparameter tuning for better generalization.  
✅ Use **Databricks MLflow Registry** (requires a paid Databricks workspace).  
✅ Deploy model using **Databricks Serving API** for real-world applications.  



