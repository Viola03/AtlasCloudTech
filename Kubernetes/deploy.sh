# kubectl apply -f persistent-volume.yaml

kubectl apply -f rabbitmq-deployment.yaml

kubectl apply -f loader-job.yaml
kubectl apply -f worker-deployment.yaml
kubectl apply -f aggregator-job.yaml

kubectl port-forward deployment/rabbitmq 15672:15672