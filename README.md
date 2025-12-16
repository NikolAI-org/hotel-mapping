## Project Setup
1. Run below command
    ```
    poetry install
    ```
    Note: Python path can be found out using below command,
    ```
    poetry env info --path
    ```
2. Additional Dependecies,
    ```
    poetry add pyspark==4.0.1
    rm -rf ~/.ivy2 ~/.cache/pyspark
    ```
3. Run the program
    ```
    poetry run python -m hotel_data.pipeline.preprocessor.preprocessing_pipeline

    poetry run python -m hotel_data.delta.table_ops

    poetry run python -m hotel_data.delta.table_ops_spark_sql
    ```
    Note: Disable the pycache
    ```
    export PYTHONDONTWRITEBYTECODE=1
    OR
    PYTHONDONTWRITEBYTECODE=1 poetry run python -m hotel_data.pipeline.preprocessor.preprocessing_pipeline
    OR
    find . -type d -name "__pycache__" -exec rm -r {} +
    ```
4. Run the test cases
    ```
    poetry run pytest tests/unit/test_orchestrator.py -v
    ```
# Hotel Pairs Column details
    ```
        
| Column name                       | Description                                        |
|-----------------------------------|----------------------------------------------------|
| id                                | vervotec id                                        |
| providerHotelId                   | provider Hotel Id                                  |
| name                              | hotel name                                         |
| normalized_name                   | normalized hotel name                              |
| geo_distance_km                   | geo distance km btw pair                           |
| name_score_jaccard_lcs            | hotel name score using jaccard_lcs algo            |
| normalized_name_score_jaccard_lcs | hotel normalized name score using jaccard_lcs algo |
| name_score_sbert                  | hotel name score using sbert algo                  |
| normalized_name_score_sbert       | hotel normalized name score using sbert algo       |
| star_ratings_score                | hotel star ratings score                           |
| address_line1_score               | hotel address line1 score                          |
| address_sbert_score               | hotel address line1 score using sbert              |
| postal_code_match                 | postal code match score                            |
| country_match                     | country name match                                 |
| phone match score                 | last 10 phone number match score                   |
| email_match_score                 |  email_match_score                                 |
| fax_match_score                   |   |


    ```
# Clustering 
- Pipelines for executing the cluster logic
```
Create actual cluster: poetry run python -m hotel_data.pipeline.clustering.hotel_clustering_pipeline
Generate the insights: poetry run python -m hotel_data.pipeline.clustering.pairing_insights_pipeline
```    
- 06_final_clusters schema
+----------------------------+-------------+-------+
|col_name                    |data_type    |comment|
+----------------------------+-------------+-------+
|name                        |string       |NULL   |
|id                          |string       |NULL   |
|normalized_name             |string       |NULL   |
|relevanceScore              |string       |NULL   |
|providerId                  |string       |NULL   |
|providerHotelId             |string       |NULL   |
|providerName                |string       |NULL   |
|language                    |string       |NULL   |
|geoCode_lat                 |string       |NULL   |
|geoCode_long                |string       |NULL   |
|geohash                     |array<string>|NULL   |
|contact_address_line1       |string       |NULL   |
|contact_address_city_name   |string       |NULL   |
|contact_address_state_name  |string       |NULL   |
|contact_address_country_code|string       |NULL   |
|contact_address_country_name|string       |NULL   |
|contact_address_postalCode  |string       |NULL   |
|name_embedding              |array<float> |NULL   |
|normalized_name_embedding   |array<float> |NULL   |
|contact_phones              |array<string>|NULL   |
|contact_fax                 |array<string>|NULL   |
|contact_emails              |array<string>|NULL   |
|type                        |string       |NULL   |
|category                    |string       |NULL   |
|starRating                  |string       |NULL   |
|distance                    |string       |NULL   |
|attributes                  |array<string>|NULL   |
|imageCount                  |string       |NULL   |
|availableSuppliers          |array<string>|NULL   |
|combined_address            |string       |NULL   |
|address_embedding           |array<float> |NULL   |
|processing_time_utc         |timestamp    |NULL   |
|original_message            |string       |NULL   |
|cluster_id                  |string       |NULL   |
+----------------------------+-------------+-------+
- Intermediate data for scoring comparison. scoring_metadata column will have details comparison
+----------------------------------------+---------+-------+
|col_name                                |data_type|comment|
+----------------------------------------+---------+-------+
|id_i                                    |string   |NULL   |
|id_j                                    |string   |NULL   |
|providerHotelId_i                       |string   |NULL   |
|providerHotelId_j                       |string   |NULL   |
|name_i                                  |string   |NULL   |
|name_j                                  |string   |NULL   |
|normalized_name_i                       |string   |NULL   |
|normalized_name_j                       |string   |NULL   |
|geo_distance_km                         |double   |NULL   |
|name_score_jaccard_lcs                  |float    |NULL   |
|normalized_name_score_jaccard_lcs       |float    |NULL   |
|name_score_sbert                        |float    |NULL   |
|normalized_name_score_sbert             |float    |NULL   |
|star_ratings_score                      |float    |NULL   |
|address_line1_score                     |float    |NULL   |
|postal_code_match                       |float    |NULL   |
|country_match                           |float    |NULL   |
|address_sbert_score                     |float    |NULL   |
|phone_match_score                       |float    |NULL   |
|email_match_score                       |float    |NULL   |
|fax_match_score                         |float    |NULL   |
|geo_distance_km_passed                  |boolean  |NULL   |
|name_score_jaccard_lcs_passed           |boolean  |NULL   |
|normalized_name_score_jaccard_lcs_passed|boolean  |NULL   |
|name_score_sbert_passed                 |boolean  |NULL   |
|normalized_name_score_sbert_passed      |boolean  |NULL   |
|star_ratings_score_passed               |boolean  |NULL   |
|address_line1_score_passed              |boolean  |NULL   |
|postal_code_match_passed                |boolean  |NULL   |
|country_match_passed                    |boolean  |NULL   |
|address_sbert_score_passed              |boolean  |NULL   |
|phone_match_score_passed                |boolean  |NULL   |
|email_match_score_passed                |boolean  |NULL   |
|fax_match_score_passed                  |boolean  |NULL   |
|scoring_metadata                        |string   |NULL   |
|is_matched                              |boolean  |NULL   |
|match_status                            |string   |NULL   |
|match_score                             |double   |NULL   |
|scoring_version                         |string   |NULL   |
|scoring_timestamp                       |timestamp|NULL   |
+----------------------------------------+---------+-------+
- Clustering can be done using two algorithm: unionfind, labelpropagation

# Minikube Setup
* Start Minikube
    ```
    minikube start --cpus=4 --memory=8192 --driver=docker
    ```
* Enable ingress
    ```
    minikube addons enable ingress
    ```
* Switch context
    ```
    kubectl config use-context minikube
    ```
* Install Helm
    ```
    curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
    ```
* Add Spark Operator Helm repo
    ```
    helm repo add spark-operator https://kubeflow.github.io/spark-operator

    helm repo update
    ```
# Docker 
* Prerequisite: Install Docker
* Docker Setup
    - Refer Dockerfile
    - Use minikube docker env or Push the image to registry. Below is command to use minikube docker env.
    ```
    eval $(minikube docker-env)

    ```
    - Run below command where Dockerfile is located.
    ```
    docker build -t hotel-preprocess-pipeline:latest .
    ```
* Install Spark Operator
    ```
    helm install release-v1 spark-operator/spark-operator \
        --namespace spark-operator \
        --create-namespace \
        --set sparkJobNamespace=default \
        --set webhook.enable=true
    ```
    1. Check the pods
        ```
        kubectl get pods -n spark-operator
        ```
* Uninstall Spark Operator
    ```
    helm uninstall spark-operator --namespace spark-operator
    kubectl delete namespace spark-operator
    ```
* Check And Validate the Job
    ```
    kubectl get sparkapplication
    kubectl describe sparkapplication spark-app
    ```

# MinIO setup

- Install the go using link https://go.dev/doc/install
- Make sure that Go's bin dir is added in PATH enviornment
```
echo 'export PATH=$PATH:$HOME/go/bin' >> ~/.bashrc
source ~/.bashrc
```
- Install MinIO using source,
```
go install github.com/minio/minio@latest
```
- Verify if MinIO is installed.
```
ls ~/go/bin/minio
which minio
minio --version
```
- If any issues probably its because of the incorrect environment variable
```
export GOPATH=$HOME/go
export PATH=$PATH:$GOPATH/bin
```
- Start the MinIO server
```
minio server ~/data
```
- Default MinIO username/password is 
```
   RootUser: minioadmin 
   RootPass: minioadmin 
```
- Install MC tool
```
curl -O https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc
sudo mv mc /usr/local/bin/
# Verify the setup
mc --version
mc alias set local http://127.0.0.1:9000 minioadmin minioadmin
mc ls local
mc admin info local
mc admin heal local
```


# Markdown View Command (VS Code)
```
Ctrl+Shift+V
```