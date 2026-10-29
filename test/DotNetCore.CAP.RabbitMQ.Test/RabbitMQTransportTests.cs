using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DotNetCore.CAP.Messages;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using Xunit;

namespace DotNetCore.CAP.RabbitMQ.Test;

public sealed class RabbitMQTransportTests
{
    [Fact]
    public async Task Successful_publish_returns_channel_to_pool()
    {
        var (pool, channel) = Substitutes((_, _) => ValueTask.CompletedTask);
        var transport = Transport(pool, new RabbitMQOptions());

        var result = await transport.SendAsync(Message());

        Assert.True(result.Succeeded);
        pool.Received(1).Return(channel);
        await channel.DidNotReceive().DisposeAsync();
    }

    [Fact]
    public async Task Default_options_publish_without_cancellation_and_not_mandatory()
    {
        var mandatory = true;
        var token = new CancellationTokenSource().Token;
        var (pool, _) = Substitutes((m, t) => { mandatory = m; token = t; return ValueTask.CompletedTask; });

        await Transport(pool, new RabbitMQOptions()).SendAsync(Message());

        Assert.False(mandatory);
        Assert.False(token.CanBeCanceled);
    }

    [Theory]
    [InlineData(true, "could not be routed")]
    [InlineData(false, "negatively acknowledged")]
    public async Task Publish_exception_fails_send_and_disposes_channel(bool isReturn, string expected)
    {
        var (pool, channel) = Substitutes((_, _) => throw new PublishException(1, isReturn));
        var transport = Transport(pool, new RabbitMQOptions());

        var result = await transport.SendAsync(Message());

        Assert.False(result.Succeeded);
        Assert.Contains(expected, result.Exception!.Message);
        pool.DidNotReceive().Return(Arg.Any<IChannel>());
        await channel.Received(1).DisposeAsync();
    }

    [Fact]
    public async Task Publish_confirm_timeout_fails_send_and_disposes_channel()
    {
        var (pool, channel) = Substitutes((_, token) => new ValueTask(Task.Delay(Timeout.Infinite, token)));
        var options = new RabbitMQOptions
        {
            PublishConfirms = true,
            PublisherConfirmationTracking = true,
            PublishConfirmTimeout = TimeSpan.FromMilliseconds(50)
        };

        var result = await Transport(pool, options).SendAsync(Message()).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.False(result.Succeeded);
        Assert.Contains(nameof(RabbitMQOptions.PublishConfirmTimeout), result.Exception!.Message);
        Assert.IsAssignableFrom<OperationCanceledException>(result.Exception.InnerException);
        pool.DidNotReceive().Return(Arg.Any<IChannel>());
        await channel.Received(1).DisposeAsync();
    }

    private static (IConnectionChannelPool Pool, IChannel Channel) Substitutes(
        Func<bool, CancellationToken, ValueTask> publish)
    {
        var channel = Substitute.For<IChannel>();
        channel.IsOpen.Returns(true);
        channel.BasicPublishAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<BasicProperties>(),
                Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<CancellationToken>())
            .Returns(call => publish(call.ArgAt<bool>(2), call.ArgAt<CancellationToken>(5)));
        var pool = Substitute.For<IConnectionChannelPool>();
        pool.Exchange.Returns("cap.test");
        pool.Rent().Returns(Task.FromResult(channel));
        return (pool, channel);
    }

    private static RabbitMqTransport Transport(IConnectionChannelPool pool, RabbitMQOptions options) =>
        new(NullLogger<RabbitMqTransport>.Instance, pool, Options.Create(options));

    private static TransportMessage Message() => new(new Dictionary<string, string?>
    {
        [DotNetCore.CAP.Messages.Headers.MessageId] = "test-message-id",
        [DotNetCore.CAP.Messages.Headers.MessageName] = "test.event"
    }, new byte[] { 1, 2, 3 });
}
