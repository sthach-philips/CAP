# RabbitMQ

RabbitMQ是实现了高级消息队列协议（AMQP）的开源消息代理软件（亦称面向消息的中间件）。RabbitMQ 服务器是用 Erlang 语言编写的，而聚类和故障转移是构建在开源的通讯平台框架上的。所有主要的编程语言均有与代理接口通讯的客户端库。

CAP 支持使用 RabbitMQ 作为消息传输器。

!!! warning "注意事项" 
    CAP 在消费者启动并注册订阅时声明队列和绑定。若发布时对应的队列和绑定尚未创建，RabbitMQ 不会为该队列保留消息。默认 `MandatoryPublishing=false` 时，即使消息无法路由，CAP 仍可能报告发布成功。生产环境建议在开始发布前启动消费者，或自行预先创建所需队列和绑定；启用强制发布和确认跟踪后，无法路由的消息会导致发布失败。

## 配置

要使用 RabbitMQ 作为消息传输器，你需要从 NuGet 安装以下扩展包：

```shell

Install-Package DotNetCore.CAP.RabbitMQ

```

然后，你可以在 `Startup.cs` 的 `ConfigureServices` 方法中添加基于 RabbitMQ 的配置项。

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

CAP 直接对外提供的 RabbitMQ 配置参数如下：

配置项 | 描述 | 类型 | 默认值
:---|:---|---|:---
HostName | 宿主地址，如果要配置集群可以使用逗号分隔，例如 `192.168.1.111,192.168.1.112` | string | localhost
UserName | 用户名 | string | guest
Password | 密码 | string | guest
VirtualHost | 虚拟主机 | string | /
Port | 端口号 | int | -1
ExchangeName | CAP默认Exchange名称 | string | cap.default.router
QueueArguments  | 队列额外参数 x-arguments | QueueArgumentsOptions  |  N/A
QueueOptions  | 更改已创建队列的选项 | QueueRabbitOptions  |  { Durable=true, Exclusive=false, AutoDelete=false }
ConnectionFactoryOptions  |  RabbitMQClient原生参数 | ConnectionFactory | N/A
CustomHeadersBuilder  | 订阅者自定义头信息 |  见下文 |  N/A
PublishConfirms | 在通道上启用[发布确认](https://www.rabbitmq.com/confirms.html#publisher-confirms) | bool | false
PublisherConfirmationTracking | 等待每条消息的代理确认或拒绝确认。要求 `PublishConfirms=true`。 | bool | false
MandatoryPublishing | 使用强制发布；RabbitMQ 返回无法路由的消息时使发布失败。要求 `PublishConfirms=true` 且 `PublisherConfirmationTracking=true`。 | bool | false
DeclareExchange | 声明 CAP 的 topic exchange；设为 false 时仅被动检查其是否存在。 | bool | true
BasicQosOptions | 指定消费的[Qos](https://www.rabbitmq.com/consumer-prefetch.html) | BasicQos | N/A

`QueueArguments` 支持以下可选的队列声明参数：

| 属性 | 说明 | 默认值 |
| :--- | :--- | :--- |
| `QueueType` | 队列类型，例如 `classic` 或 `quorum`（`x-queue-type`）。 | 未设置 |
| `QueueMode` | 队列模式（`x-queue-mode`）。 | 未设置 |
| `MessageTTL` | 消息生存时间，单位为毫秒（`x-message-ttl`）。 | 864000000（10 天） |


#### 发布确认与强制发布

`PublishConfirms` 会在 RabbitMQ 通道上启用确认模式。为保持向后兼容，
单独启用它并不会让 CAP 等待每条消息的确认；`PublisherConfirmationTracking`
会启用 RabbitMQ.Client 的逐条发布跟踪，并等待代理的确认或拒绝确认。收到拒绝
确认时，CAP 会将发送结果标记为失败。

`MandatoryPublishing` 会要求 RabbitMQ 在消息无法路由到队列时返回消息。启用确认跟踪后，
即使 RabbitMQ 同时确认了发布，被返回的消息仍会使 CAP 发送失败。这些选项默认关闭，
也不代表消费者已处理消息；连接中断导致结果不明确时仍可能发生重复投递。

所需配置组合如下：

```csharp
o.UseRabbitMQ(options =>
{
    options.PublishConfirms = true;
    options.PublisherConfirmationTracking = true;
    options.MandatoryPublishing = true;
});
```

未启用 `PublishConfirms` 却启用确认跟踪，或未启用确认跟踪却启用强制发布，都会在应用
启动时导致选项验证失败。

#### 外部管理的 Exchange

如果 exchange 由外部预先创建，请设置 `DeclareExchange=false`。CAP 会在启动时以及创建
发布者和消费者通道时通过被动声明检查 exchange 是否存在；此模式下不会创建 exchange。
如果 exchange 不存在，应用启动或通道创建会失败。CAP 仍会创建消费者队列和绑定，因此此
设置并不会让整个传输拓扑变为只读。RabbitMQ 用户仍需拥有被动检查 exchange、发布消息以及
CAP 声明队列和绑定所需的权限。

#### ConnectionFactory Option

如果你需要 **更多** 原生 `ConnectionFactory` 相关的配置项，可以通过 `ConnectionFactoryOptions` 配置项进行设定：

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

当需要从异构系统或者直接接收从RabbitMQ 控制台发送的消息时，由于 CAP 需要定义额外的头信息才能正常订阅，所以此时会出现异常。通过提供此参数来进行自定义头信息的设置来使订阅者正常工作。

你可以在这里找到有关 [头信息](../cap/messaging.md#异构系统集成) 的说明。

用法如下：

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


#### 如何连接 RabbitMQ 集群？

使用逗号分隔连接字符串即可，如下：

```
x=> x.UseRabbitMQ("localhost:5672,localhost:5673,localhost:5674")
```
