# RabbitMQ transport tests

The unit tests run without a broker. Broker tests are skipped unless
`CAP_RABBITMQ_TEST_HOST` is set. To run them with a local RabbitMQ instance:

```sh
docker run --rm --detach --name cap-rabbitmq-test \
  -p 5679:5672 \
  -e RABBITMQ_DEFAULT_USER=cap \
  -e RABBITMQ_DEFAULT_PASS=cap \
  rabbitmq:4-management

CAP_RABBITMQ_TEST_HOST=127.0.0.1 dotnet test test/DotNetCore.CAP.RabbitMQ.Test/DotNetCore.CAP.RabbitMQ.Test.csproj -c Release

docker stop cap-rabbitmq-test
```

The host can be overridden with `CAP_RABBITMQ_TEST_PORT`, and credentials with
`CAP_RABBITMQ_TEST_USERNAME` / `CAP_RABBITMQ_TEST_PASSWORD`. Broker tests use
unique exchanges and queues, then delete the resources they create.

The publish confirmation timeout test pauses a broker, so it starts its own
disposable container instead of using `CAP_RABBITMQ_TEST_HOST`. It is skipped
unless `CAP_RABBITMQ_DOCKER_TEST=1` is set and requires a local Docker CLI:

```sh
CAP_RABBITMQ_DOCKER_TEST=1 dotnet test test/DotNetCore.CAP.RabbitMQ.Test/DotNetCore.CAP.RabbitMQ.Test.csproj -c Release
```

Override the image with `CAP_RABBITMQ_DOCKER_IMAGE` (default `rabbitmq:4-management`).
