This project demonstrates the analysis of particle physics data (Higgs boson decays to four leptons) using scalable architectures. Each implementation showcases how modern cloud and containerization technologies can optimize data processing workflows for particle physics experiments. Navigate to the appropiate directory for each implementation.

1. Volume-Based Implementation:
An initial architecture using shared volumes between docker containers for data exchange. Key components:
- Loader: splits ROOT files into chunks and writes them to a shared volume
- Worker: processes chunks and saves results back to the volume
- Aggregator: reads processed data and generates a histogram plot
Setup: Build and run the containers using

`docker-compmose up --build`

Note: as the initial architecture for validation the directory was not 'cleaned up' as the RabbitMQ implementation was an remains in a more segmented state to allow for debugging of each service independently.

2. RabbitMQ-Based Implementation:
Replacing shared volumes with RabbitMQ for inter-process communication.
- Loader: Publishes chunks as messages to the queue
- Worker: Processes messages from the data_chunks queue and publishes results to the processed_chunks queue
- Aggregator: subscribes to the processed_queue, aggregates the data and generates the plot
Setup: Build and run the containers using

`docker compose up --build`

Access the RabbitMQ Management UI at http://localhost:15672, login with Username: user, Password: password

3. Kubernetes-Based Implementation:
Leverages Kubernetes for container orchestration. Setup can be achieved using the provided shell scripts:
`./deploy.sh`

Verify services with commands in: `status.sh`

System reset: `./clear.sh`

However, the current setup is designed to save the output to a local path described in the persisting volume yaml file for plot validation. This should be changed to a local path on the host machine.
