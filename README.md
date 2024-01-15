# OneAcreFund's OpenFaaS RabbitMQ Connector

An enhanced version of the RabbitMQ Connector for triggering OpenFaaS functions. It maps RabbitMQ messages to functions by using the `Routing keys`.

## Usage

Deploy your OpenFaaS function using either the [OpenFaaS CLI](https://github.com/openfaas/faas-cli) or [Rest API](https://github.com/openfaas/faas/tree/master/api-docs) with an `annotation` named `topic`, which should be a comma-separated string of relevant topics. 

Ensure your functions can handle being invoked potentially twice with the same payload, as in the event of an error, messages will attempt re-routing back to the queue.

Please familiarize yourself with the official RabbitMQ documentation [here](https://www.rabbitmq.com/production-checklist.html) and [here](https://www.rabbitmq.com/monitoring.html) to mitigate message loss.

## Configuration

General Connector settings:
- `basic_auth`: Toggle for basic_auth (`1` or `true`).
- `secret_mount_path`: Path to the basic auth secret file for OpenFaaS gateway.
- `OPEN_FAAS_GW_URL`: Defaults to `http://gateway:8080`.
- `REQ_TIMEOUT`: Defaults to `30s`.
- `TOPIC_MAP_REFRESH_TIME`: Defaults to `60s`.
- `INSECURE_SKIP_VERIFY`: Defaults to `false`. Avoid enabling to prevent potential MITM attacks.
- `MAX_CLIENT_PER_HOST`: Defaults to `256`.

TLS Configuration:
- `TLS_ENABLED`: Default `false`.
- `TLS_CA_CERT_PATH`, `TLS_SERVER_CERT_PATH`, `TLS_SERVER_KEY_PATH`: Ensure accessibility by the Go process.

RabbitMQ Settings:
- `RMQ_HOST`, `RMQ_PORT`, `RMQ_VHOST`, `RMQ_USER`, `RMQ_PASS`
- `PATH_TO_TOPOLOGY`: Required path to the topology YAML.

## Topology Configuration

Use your existing Exchange definitions with this new format. Example:

```yaml
- name: Exchange_Name
  topics: [Foo, Bar]
  declare: true
  type: "direct"
  durable: false
  auto-deleted: false
```

Queues are configured according to their exchange declaration. Queue names follow the **`{Exchange_Name}_{Topic}`** schema.

### **Examples**

```yaml
- name: sapb1_items_ex
  topics:
    - sapb1.item.add
    - sapb1.item.update
  declare: true
  type: "topic"
  durable: true
  auto-deleted: false
  queue: sapb1_items_queue
 
- name: sapb1_warehouse_ex
  topics:
    - sapb1.warehouse.add
    - sapb1.warehouse.update
  declare: true
  type: "topic"
  durable: true
  auto-deleted: false
  queue: sapb1_warehouse_queue
  ttl: 6000
  dead-letter-exchange: "rts_ex_dead_letter"

```

## **Kubernetes Deployment**

To deploy the RabbitMQ Connector in Kubernetes:

1. Apply the topology:
    
    ```bash
    kubectl create cm topology --namespace=openfaas --from-file=rabbitmq-connector.topology.yaml
    
    ```
    
2. Apply the configuration:kubectl create -f rabbitmq-connector.config.yaml

    
    ```bash
    kubectl create -f rabbitmq-connector.config.yaml
    ```
    
3. Deploy the connector:
    
    ```bash
    kubectl create -f rabbitmq-connector.deployment.yaml
    
    ```
    

## **Issues & Feature Requests**

For bugs and feature requests, visit the **[Issue Tab](https://github.com/OneAcreFund/rabbitmq-connector/issues)**.