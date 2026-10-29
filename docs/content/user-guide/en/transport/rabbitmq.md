# RabbitMQ

RabbitMQ is an open-source message broker software that implements the Advanced Message Queuing Protocol (AMQP) and has been extended with a plug-in architecture to support additional protocols such as Streaming Text Oriented Messaging Protocol (STOMP), Message Queuing Telemetry Transport (MQTT), and others.

RabbitMQ can be used in CAP as a message transporter. 

!!! warning "Important Notes"
    CAP declares queues and bindings when a consumer starts and registers its subscriptions. If a message is published before its queue and binding exist, RabbitMQ does not retain it for that queue. With the default `MandatoryPublishing=false`, CAP can still report an unroutable publish as successful. In production, start consumers before publishing or provision the required queues and bindings in advance. Enable mandatory publishing and tracked confirmations to have unroutable publishes fail.

## Configuration

To use RabbitMQ as a transporter, you need to install the following package from NuGet:

```powershell
PM> Install-Package DotNetCore.CAP.RabbitMQ

```

Next, add configuration items to the `ConfigureServices` method of `Startup.cs`.

```csharp

public void ConfigureServices(IServiceCollection services)
{
    // ...

    services.AddCap(x =>
    {
        x.UseRabbitMQ(opt=>
        {
            //RabbitMQOptions
        });
        // x.UseXXX ...
    });
}

```

#### RabbitMQ Options

The RabbitMQ configuration parameters provided directly by CAP:

NAME | DESCRIPTION | TYPE | DEFAULT
:---|:---|---|:---
HostName | Broker host address | string | localhost
UserName | Broker user name | string | guest
Password | Broker password | string | guest
VirtualHost | Broker virtual host | string | /
Port | Port | int | -1
ExchangeName | Default exchange name | string | cap.default.router
QueueArguments  | Extra queue `x-arguments` | QueueArgumentsOptions  |  N/A
QueueOptions  | Change Options for created queue | QueueRabbitOptions  |  { Durable=true, Exclusive=false, AutoDelete=false }
ConnectionFactoryOptions  |  RabbitMQClient native connection options | ConnectionFactory | N/A
CustomHeadersBuilder  | Custom subscribe headers |  See the blow |  N/A
PublishConfirms | Enable [publisher confirms](https://www.rabbitmq.com/confirms.html#publisher-confirms) on the channel | bool | false
PublisherConfirmationTracking | Wait for a per-message broker acknowledgement or negative acknowledgement. Requires `PublishConfirms=true`. | bool | false
MandatoryPublishing | Publish as mandatory and fail when RabbitMQ returns an unroutable message. Requires `PublishConfirms=true` and `PublisherConfirmationTracking=true`. | bool | false
PublishConfirmTimeout | Maximum time to wait for one publish and its broker confirmation before the send fails. `null` applies no CAP-specific limit. Requires `PublisherConfirmationTracking=true`; must be positive. | TimeSpan? | null
DeclareExchange | Declare CAP's topic exchange, or passively check that it already exists when false. | bool | true
BasicQosOptions | Specify [Qos](https://www.rabbitmq.com/consumer-prefetch.html) of message prefetch | BasicQos | N/A

`QueueArguments` supports these optional queue declaration arguments:

| Property | Description | Default |
| :--- | :--- | :--- |
| `QueueType` | Queue type, such as `classic` or `quorum` (`x-queue-type`). | Not set |
| `QueueMode` | Queue mode (`x-queue-mode`). | Not set |
| `MessageTTL` | Message time-to-live in milliseconds (`x-message-ttl`). | 864000000 (10 days) |


#### Publisher confirms and mandatory publishing

`PublishConfirms` enables confirm mode on the RabbitMQ channel. For backward
compatibility, it does not by itself make CAP wait for an individual publish
acknowledgement; `PublisherConfirmationTracking` enables RabbitMQ.Client's
per-publication tracking and awaits the broker's ack or nack. A nack is returned
as a failed CAP send.

`MandatoryPublishing` asks RabbitMQ to return a message when no queue is bound
for its routing key. With confirmation tracking enabled, a returned message
fails the CAP send even if RabbitMQ also acknowledges the publish. These settings
are opt-in and do not prove that a consumer processed a message or prevent
redelivery after an ambiguous connection failure.

The required combination is:

```csharp
o.UseRabbitMQ(options =>
{
    options.PublishConfirms = true;
    options.PublisherConfirmationTracking = true;
    options.MandatoryPublishing = true;
});
```

Enabling confirmation tracking without `PublishConfirms`, or mandatory publishing
without confirmation tracking, fails options validation at application startup.

Set `PublishConfirmTimeout` to bound how long a tracked publish may wait for the
broker, for example when RabbitMQ blocks publishers or stops responding. When the
timeout elapses, the CAP send fails and the channel is discarded rather than
returned to the pool; CAP's normal retry handling applies. A timeout is
ambiguous: the broker may still have accepted the message, so consumers should
be idempotent. The option requires `PublisherConfirmationTracking=true`, because
without tracking the publish completes once the frame is written and there is no
confirmation to wait for.

#### Externally managed exchanges

Set `DeclareExchange=false` when the exchange is provisioned outside CAP. CAP
checks that the exchange exists with a passive declaration at startup and when
creating publisher and consumer channels; it never creates the exchange in this
mode. A missing exchange causes startup/channel creation to fail. CAP still
creates consumer queues and bindings, so this setting does not make the entire
transport topology read-only. The RabbitMQ user still needs permissions for passive
exchange checks, publishing, and CAP's queue and binding declarations.

#### ConnectionFactory Option

If you need **more** native `ConnectionFactory` configuration options, you can set it by 'ConnectionFactoryOptions' option:

```csharp

services.AddCap(x =>
{
    x.UseRabbitMQ(o =>
    {
        o.HostName = "localhost";
        o.ConnectionFactoryOptions = opt => { 
            //rabbitmq client ConnectionFactory config
        };
    });
});

```

#### CustomHeadersBuilder Option

When the message is sent from the RabbitMQ management console or a heterogeneous system, CAP requires additional headers to be defined. By providing this parameter, you can set custom headers to ensure the subscriber works correctly.

You can find the description of [Header Information](../cap/messaging.md#heterogeneous-system-integration) here.

Example：

```cs
x.UseRabbitMQ(aa =>
{
    aa.CustomHeadersBuilder = (msg, sp) =>
    [
        new(DotNetCore.CAP.Messages.Headers.MessageId, sp.GetRequiredService<ISnowflakeId>().NextId().ToString()),
        new(DotNetCore.CAP.Messages.Headers.MessageName, msg.RoutingKey)
    ];
});
```

#### How to Connect to a Cluster

Use a comma-separated connection string like this:

```
x=> x.UseRabbitMQ("localhost:5672,localhost:5673,localhost:5674")
```
