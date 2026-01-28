# Kubernetes deployment steps
kind create cluster --name faang-prep
docker info
kubectl config use-context kind-faang-prep
kubectl get nodes

# build
kubectl config set-context --current --namespace=faang-prep
docker build --no-cache -f online_status_application_websocket/Dockerfile -t online-status:latest . 
kind load docker-image online-status:latest --name faang-prep
kubectl apply -f online_status_application_websocket/k8s-deployment.yaml
kubectl port-forward service/online-status-webapp 8081:80


# rebuild and redeploy
docker build --no-cache -f online_status_application_websocket/Dockerfile -t online-status:latest . 
kind load docker-image online-status:latest --name faang-prep
kubectl apply -f online_status_application_websocket/k8s-deployment.yaml
kubectl delete pods --selector=app=online-status-webapp
kubectl get pods --selector=app=online-status-webapp
kubectl get pods
kubectl port-forward service/online-status-webapp 8081:80

# kubectl commands to check status
kubectl get pods
kubectl get deployments
kubectl get services
kubectl get pods --selector=app=online-status-webapp -o jsonpath='{.items[*].metadata.name}'
kubectl get endpoints online-status-webapp

# logs
kubectl logs --selector=app=online-status-webapp --tail=20
kubectl logs online-status-webapp-68b648cb88-2rwks --tail=20


kubectl port-forward service/online-status-webapp 8080:80
curl -s http://localhost:8080 | head -20

# delete and redeploy
kubectl delete -f k8s-deployment.yaml
kubectl apply -f online_status_application_websocket/k8s-deployment.yaml

# Basic kubectl commands
kubectl cluster-info
kubectl get nodes

# Set context and namespace
kubectl config current-context
kubectl get ns
kubectl config set-context --current --namespace=faang-prep

kubectl config current-context
kubectl config get-contexts
kubectl config use-context kind-faang-prep

# Describe resources
kubectl describe deployment redis
kubectl describe deployment online-status-webapp
kubectl describe service redis
kubectl describe service online-status-webapp

# Scale deployment
kubectl scale deployment online-status-webapp --replicas=3

# Update deployment with new image
# Build and load new image
docker build --no-cache -f online_status_application_websocket/Dockerfile -t online-status:latest .
kind load docker-image online-status:latest --name faang-prep
kubectl set image deployment/online-status-webapp webapp=online-status:latest

# get endpointslice
kubectl get endpointslice -l kubernetes.io/service-name=online-status-webapp

# Delete deployments and services
kubectl delete -f online_status_application_websocket/k8s-deployment.yaml
kind delete cluster --name faang-prep




