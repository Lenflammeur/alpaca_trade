# Automated Stock Trading Application with AWS 

This project is an automated stock trading application that uses a pair trading strategy to place trades. The application is dockerized and deployed on AWS ECS/EKS, with scheduled runs managed by AWS EventBridge. It also uses AWS SNS for sending trade notifications.

## Project Structure

- `app/` - Directory containing the application code.
  - `main.py` - This is the main script that contains the logic for the moving average crossover strategy.
  - `Dockerfile` - Dockerfile to build the Docker image of the application.
- `infrastructure/` - Directory containing the infrastructure code (possibly CloudFormation templates).

## Prerequisites

1. **AWS Account** - Required to deploy and run the application.
2. **Docker** - Required to build and test the Docker image locally.
3. **Python** - Required for running the application.

## Setup Instructions

1. **Docker Image Build**: Navigate to the `app/` directory and build the Docker image using the command `docker build -t trading-app .`.
2. **AWS Setup**: Login to your AWS account and set up the necessary resources (ECS/EKS cluster, EventBridge rules, SNS topics).
3. **Deploy the Application**: Upload the Docker image to your ECS/EKS cluster and configure the EventBridge rule to run the task/pod at the desired times.
4. **Notification Setup**: Subscribe to the SNS topic to receive trade notifications.

## Running the Application

Once deployed, the application will automatically run at the scheduled times. Trade notifications will be sent via email.

## Monitoring

You can monitor the application through the AWS Management Console. Logs from the application are sent to CloudWatch Logs.
