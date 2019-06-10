## Data files

This folder contains one CSV file: cluster.csv
cluster.csv is the dataset used to run clustering algorithm.

The dataset that we used to run user-to-user algorithm is too large. Therefore, we stored it in a
public Google Cloud Storage bucket.

Download it by typing the following code in your terminal:
```
gsutil cp gs://hanjiaxu/user_to_user_data.csv user_to_user_data.csv
```
