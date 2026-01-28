# Install a Container Engine
brew install --cask docker

2. Start the Application
 
 Installing is not enough; the engine must be running:

 Open your Applications folder.

 Launch Docker or OrbStack.

 Wait for the status icon in your menu bar to show "Running."
 
Once Docker is running, verify it:

	docker info
	
	
"Staff Engineer" Tip : Since you are applying for a Staff Engineer role, mention in interviews that you prefer OrbStack or Colima over Docker Desktop for local development because they use significantly less CPU/RAM, which is critical when running multi-node Kubernetes clusters (Kind) alongside heavy IDEs like IntelliJ.

3. **Container runtime:** 
	Option A: Docker Desktop (Simplest)

		If you already have Docker Desktop installed, go to Settings > Kubernetes and check "Enable Kubernetes". It spins up a single-node cluster automatically.

	Option B: Kind (Kubernetes in Docker) - Best for Senior SWEs

	kind runs Kubernetes nodes as Docker containers. It is extremely fast and allows for multi-node clusters.

```
		Install: brew install kind
		Create Cluster: kind create cluster --name faang-prep
```


4. **Initialize and Create Cluster**

If that works, run your command again:

	kind create cluster --name faang-prep

```
Creating cluster "faang-prep" ...
 ✓ Ensuring node image (kindest/node:v1.35.0) 🖼 
 ✓ Preparing nodes 📦  
 ✓ Writing configuration 📜 
 ✓ Starting control-plane 🕹️ 
 ✓ Installing CNI 🔌 
 ✓ Installing StorageClass 💾 
Set kubectl context to "kind-faang-prep"
You can now use your cluster with:

kubectl cluster-info --context kind-faang-prep
```

5. **Verify Cluster**

`kubectl cluster-info --context kind-faang-prep`

output should be similar to:
```
Kubernetes control plane is running at https://127.0.0.1:60204
CoreDNS is running at https://127.0.0.1:60204/api/v1/namespaces/kube-system/services/kube-dns:dns/proxy
```

2. **Essential CLI Tools**

You will need these to interact with your cluster:

- kubectl: The main CLI (`brew install kubectl`).
- k9s: A terminal UI that makes managing clusters 10x faster (``brew install derailed/k9s/k9s`).
- Helm: The package manager for Kubernetes (    `brew install helm`).


3. Testing Your Local Setup

Once you have started your cluster (e.g., via kind or minikube), run the following commands to "smoke test" the environment.

- Step 1: Check Nodes


`kubectl get nodes`

```
NAME                       STATUS   ROLES           AGE   VERSION
faang-prep-control-plane   Ready    control-plane   53m   v1.35.0
```

Step 2: Deploy a Sample App (Nginx)

```
# Create a deployment
kubectl create deployment web-server --image=nginx

# Expose it as a service
kubectl expose deployment web-server --port=80 --type=NodePort

# Check if pods are running
kubectl get pods

```
```
sanjivsingh@IMUL-ML0406 distributed_transactions % kubectl create deployment web-server --image=nginx
deployment.apps/web-server created
sanjivsingh@IMUL-ML0406 distributed_transactions % kubectl expose deployment web-server --port=80 --type=NodePort
service/web-server exposed
sanjivsingh@IMUL-ML0406 distributed_transactions % kubectl get pods
NAME                          READY   STATUS    RESTARTS   AGE
web-server-8488f64789-b7zcp   1/1     Running   0          37s
```

Step 3: Access the App

If using Kind/Docker Desktop, you typically use kubectl port-forward:

`kubectl port-forward service/web-server 8080:80`

```
Forwarding from 127.0.0.1:8080 -> 80
Forwarding from [::1]:8080 -> 80
Handling connection for 8080
Handling connection for 8080
...
...

```
# Now visit http://localhost:8080 in your browser

4. Pro-Tip 
    To simulate "scale" issues locally, don't just deploy a Hello World. Try to:

    - Set Resource Limits: Define CPU/Memory limits to see how K8s handles OOM (Out of Memory) kills.
    - Liveness/Readiness Probes: Write a small Python/Go app that "breaks" its health check and watch K8s restart it.
    - ConfigMaps/Secrets: Practice injecting configuration without rebuilding images.