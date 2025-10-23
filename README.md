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


# Markdown View Command (VS Code)
```
Ctrl+Shift+V
```