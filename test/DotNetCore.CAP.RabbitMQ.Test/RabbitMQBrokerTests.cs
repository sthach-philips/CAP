using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DotNetCore.CAP.Messages;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using Xunit;

namespace DotNetCore.CAP.RabbitMQ.Test;

public sealed class RabbitMQBrokerTests
{
    [RabbitMqFact]
    public async Task Defaults_keep_unroutable_publication_succeeded()
    {
        var options = TestOptions();
        await using var scope = new RabbitMqTestScope(options);

        var result = await scope.SendAsync("unbound.event");

        Assert.True(result.Succeeded, result.Exception?.ToString());
    }

    [RabbitMqFact]
    public async Task Mandatory_unroutable_publication_fails_with_return_message()
    {
        var options = TestOptions(publishConfirms: true, trackConfirmations: true, mandatoryPublishing: true);
        await using var scope = new RabbitMqTestScope(options);

        var result = await scope.SendAsync("unbound.event");

        Assert.False(result.Succeeded);
        Assert.Contains("could not be routed", result.Exception?.Message);
    }

    [RabbitMqFact]
    public async Task Reject_publish_overflow_is_reported_as_failure()
    {
        var options = TestOptions(publishConfirms: true, trackConfirmations: true);
        await using var scope = new RabbitMqTestScope(options);
        var queue = "cap.test." + Guid.NewGuid().ToString("N");
        await using var admin = await TestConnection(options);
        await using var channel = await admin.CreateChannelAsync();
        await channel.ExchangeDeclareAsync(scope.Pool.Exchange, RabbitMQOptions.ExchangeType, true);
        await channel.QueueDeclareAsync(queue, true, false, false, new Dictionary<string, object?>
        {
            ["x-max-length"] = 1,
            ["x-overflow"] = "reject-publish"
        });
        await channel.QueueBindAsync(queue, scope.Pool.Exchange, "bounded.event");

        try
        {
            var first = await scope.SendAsync("bounded.event");
            var second = await scope.SendAsync("bounded.event");

            Assert.True(first.Succeeded, first.Exception?.ToString());
            Assert.False(second.Succeeded);
        }
        finally
        {
            await channel.QueueDeleteAsync(queue);
        }
    }

    [RabbitMqFact]
    public async Task Disabled_declaration_fails_startup_when_exchange_is_missing()
    {
        var options = TestOptions(declareExchange: false);
        await using var scope = new RabbitMqTestScope(options);
        var check = scope.CreateStartupCheck();

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            check.StartAsync(CancellationToken.None));

        Assert.Contains(options.ExchangeName, exception.Message);
        Assert.Contains(nameof(RabbitMQOptions.DeclareExchange), exception.Message);
    }

    [RabbitMqFact]
    public async Task Disabled_declaration_passively_checks_existing_exchange_and_publishes()
    {
        var options = TestOptions(declareExchange: false, publishConfirms: true, trackConfirmations: true);
        var queue = "cap.test." + Guid.NewGuid().ToString("N");
        var alternateExchange = options.ExchangeName + ".alternate";
        await using var admin = await TestConnection(options);
        await using var adminChannel = await admin.CreateChannelAsync();
        await adminChannel.ExchangeDeclareAsync(alternateExchange, RabbitMQOptions.ExchangeType, true);
        await adminChannel.ExchangeDeclareAsync(options.ExchangeName, RabbitMQOptions.ExchangeType, true,
            arguments: new Dictionary<string, object?> { ["alternate-exchange"] = alternateExchange });
        await adminChannel.QueueDeclareAsync(queue, true, false, false);
        await adminChannel.QueueBindAsync(queue, options.ExchangeName, "existing.event");
        await using var scope = new RabbitMqTestScope(options);
        var check = scope.CreateStartupCheck();

        try
        {
            await check.StartAsync(CancellationToken.None);
            var result = await scope.SendAsync("existing.event");
            var received = await adminChannel.BasicGetAsync(queue, true);

            Assert.True(result.Succeeded, result.Exception?.ToString());
            Assert.NotNull(received);
            Assert.Equal("test-message-id", received.BasicProperties.MessageId);
            Assert.Equal(Encoding.UTF8.GetBytes("test-payload"), received.Body.ToArray());
        }
        finally
        {
            await adminChannel.QueueDeleteAsync(queue);
            await adminChannel.ExchangeDeleteAsync(options.ExchangeName);
            await adminChannel.ExchangeDeleteAsync(alternateExchange);
        }
    }

    [RabbitMqFact]
    public async Task Disabled_declaration_uses_passive_check_for_consumer_channel()
    {
        var options = TestOptions(declareExchange: false);
        var queue = "cap.test." + Guid.NewGuid().ToString("N");
        await using var admin = await TestConnection(options);
        await using var adminChannel = await admin.CreateChannelAsync();
        await adminChannel.ExchangeDeclareAsync(options.ExchangeName, "direct", true);
        await using var scope = new RabbitMqTestScope(options);
        await using var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection().BuildServiceProvider();
        var client = new RabbitMqConsumerClient(queue, 1, scope.Pool, Options.Create(options), services);

        try
        {
            await client.ConnectAsync();
        }
        finally
        {
            await client.DisposeAsync();
            await adminChannel.QueueDeleteAsync(queue);
            await adminChannel.ExchangeDeleteAsync(options.ExchangeName);
        }
    }

    private static RabbitMQOptions TestOptions(bool declareExchange = true, bool publishConfirms = false,
        bool trackConfirmations = false, bool mandatoryPublishing = false) => new()
    {
        HostName = Environment.GetEnvironmentVariable("CAP_RABBITMQ_TEST_HOST")!,
        Port = int.Parse(Environment.GetEnvironmentVariable("CAP_RABBITMQ_TEST_PORT") ?? "5679"),
        UserName = Environment.GetEnvironmentVariable("CAP_RABBITMQ_TEST_USERNAME") ?? "cap",
        Password = Environment.GetEnvironmentVariable("CAP_RABBITMQ_TEST_PASSWORD") ?? "cap",
        ExchangeName = "cap.test." + Guid.NewGuid().ToString("N"),
        DeclareExchange = declareExchange,
        PublishConfirms = publishConfirms,
        PublisherConfirmationTracking = trackConfirmations,
        MandatoryPublishing = mandatoryPublishing
    };

    private static Task<IConnection> TestConnection(RabbitMQOptions options)
    {
        var factory = new ConnectionFactory
        {
            HostName = options.HostName,
            Port = options.Port,
            UserName = options.UserName,
            Password = options.Password,
            VirtualHost = options.VirtualHost
        };
        return factory.CreateConnectionAsync();
    }

    public sealed class RabbitMqFactAttribute : FactAttribute
    {
        public RabbitMqFactAttribute()
        {
            if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("CAP_RABBITMQ_TEST_HOST")))
                Skip = "Set CAP_RABBITMQ_TEST_HOST to run RabbitMQ broker integration tests.";
        }
    }

    private sealed class RabbitMqTestScope : IAsyncDisposable
    {
        private readonly RabbitMQOptions _options;

        public RabbitMqTestScope(RabbitMQOptions options)
        {
            _options = options;
            Pool = new ConnectionChannelPool(NullLogger<ConnectionChannelPool>.Instance,
                Options.Create(new CapOptions { Version = "v1" }), Options.Create(options));
            Transport = new RabbitMqTransport(NullLogger<RabbitMqTransport>.Instance, Pool, Options.Create(options));
        }

        public ConnectionChannelPool Pool { get; }

        public RabbitMQExchangeStartupCheck CreateStartupCheck() => new(
            new ServiceCollection().AddSingleton<IConnectionChannelPool>(Pool).BuildServiceProvider(),
            Options.Create(_options));
        private RabbitMqTransport Transport { get; }

        public Task<OperateResult> SendAsync(string routingKey) => Transport.SendAsync(new TransportMessage(
            new Dictionary<string, string?>
            {
                [DotNetCore.CAP.Messages.Headers.MessageId] = "test-message-id",
                [DotNetCore.CAP.Messages.Headers.MessageName] = routingKey
            }, Encoding.UTF8.GetBytes("test-payload")));

        public async ValueTask DisposeAsync()
        {
            Pool.Dispose();
            await using var connection = await TestConnection(_options);
            await using var channel = await connection.CreateChannelAsync();
            try
            {
                await channel.ExchangeDeleteAsync(Pool.Exchange);
            }
            catch (OperationInterruptedException)
            {
            }
        }
    }
}
