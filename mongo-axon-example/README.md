# Mongo Axon Springboot Example

This is an example SpringBoot application using the Mongo Axon extension.
It uses the Mongo as the Event Store and Token Store as well.

## How to run

### Preparation

You will need `docker` and `docker-compose` to run this example.

Please run:

```bash
docker-compose -f ./mongo-axon-example/docker-compose.yaml up -d
```

This will start Mongo with default values.

Now build the application by running:

```bash
mvn clean package -f ./mongo-axon-example
```

### Running example application

You can start the application by running `java -jar ./mongo-axon-example/target/mongo-axon-example.jar`.

You can access the mongo-express UI on [http://localhost:8081/db/axonframework/](http://localhost:8081/db/axonframework/)
where you can see the tables used by axon and inspect events, tokens and snapshots.
