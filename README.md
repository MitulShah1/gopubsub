# gopubsub

gopubsub is a Go library that provides a simple and flexible way to publish and subscribe to messages using various message brokers and providers. It allows you to easily integrate with different message brokers like Kafka, NATS, RabbitMQ, Google Cloud Pub/Sub, and Azure Event Hubs.

![Logo](docs/image/go_pubsub.png)


```sh
pubsub/
├── examples/
│   ├── kafka_example.go
│   ├── nats_example.go
│   └── ...
├── internal/
│   └── util/
│       ├── retry.go
│       ├── backoff.go
│       └── logging.go
├── pkg/
│   ├── pubsub/
│   │   ├── options.go
│   │   ├── message.go
│   │   ├── publisher.go
│   │   ├── subscriber.go
│   │   └── errors.go
│   └── providers/
│       ├── kafka/
│       ├── nats/
│       ├── rabbitmq/
│       ├── googlecloud/
│       └── eventhub/
├── test/
│   ├── integration/
│   └── mocks/
├── docs/                     
├── go.mod
├── go.sum
├── README.md
└── LICENSE
```
