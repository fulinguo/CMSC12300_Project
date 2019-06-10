# CMSC12300_Project Yelp Recommendation

Group members:  
Ying Sun  
Jiaxu Han  
Fulin Guo  
Ellen Hsieh

## How to run:

### In terminal:

#### Clustering Algorithm:
python3 cluster_model_1.py -r dataproc --num-core-instances 6 cluster_data.csv

python3 cluster_model_2.py -r dataproc --num-core-instances 6 cluster_data.csv

python3 cluster_model_3.py -r dataproc --num-core-instances 6 cluster_data.csv

python3 cluster_model_4.py -r dataproc --num-core-instances 6 cluster_data.csv

#### User-to-user Algorithm:

python3 user_to_user.py -r dataproc --num-core-instances 6 user_to_user_data.csv

