// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DotNetCore.CAP.Internal;
using DotNetCore.CAP.Messages;
using DotNetCore.CAP.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace DotNetCore.CAP.RabbitMQ;

internal sealed class RabbitMqTransport : ITransport
{
    private readonly IConnectionChannelPool _connectionChannelPool;
    private readonly string _exchange;
    private readonly ILogger _logger;
    private readonly bool _mandatoryPublishing;
    private readonly TimeSpan? _publishConfirmTimeout;

    public RabbitMqTransport(ILogger<RabbitMqTransport> logger, IConnectionChannelPool connectionChannelPool,
        IOptions<RabbitMQOptions> options)
    {
        _logger = logger;
        _connectionChannelPool = connectionChannelPool;
        _exchange = _connectionChannelPool.Exchange;
        _mandatoryPublishing = options.Value.MandatoryPublishing;
        _publishConfirmTimeout = options.Value.PublishConfirmTimeout;
    }

    public BrokerAddress BrokerAddress => new("RabbitMQ", _connectionChannelPool.HostAddress);

    public async Task<OperateResult> SendAsync(TransportMessage message)
    {
        IChannel? channel = null;
        var disposeChannel = false;
        using var timeout = _publishConfirmTimeout.HasValue ? new CancellationTokenSource() : null;
        try
        {
            channel = await _connectionChannelPool.Rent();

            var props = new BasicProperties
            {
                MessageId = message.GetId(),
                DeliveryMode = DeliveryModes.Persistent,
                Headers = message.Headers.ToDictionary(x => x.Key, object? (x) => x.Value)
            };

            timeout?.CancelAfter(_publishConfirmTimeout!.Value);
            await channel.BasicPublishAsync(_exchange, message.GetName(), _mandatoryPublishing, props, message.Body,
                timeout?.Token ?? CancellationToken.None);

            _logger.LogInformation("CAP message '{0}' published, internal id '{1}'", message.GetName(), message.GetId());

            return OperateResult.Success;
        }
        catch (Exception ex)
        {
            disposeChannel = true;
            if (ex is AlreadyClosedException && channel?.IsOpen == true)
            {
                // There are cases when channel's property IsOpen returns true, but the connection is actually closed, e.g. https://github.com/rabbitmq/rabbitmq-dotnet-client/issues/1871
                // This is a workaround to abort the channel in this case to avoid returning a faulty channel back to the pool.

                _logger.LogWarning("Channel state inconsistency detected: channel is reported as open, but its underlying connection is closed. Forcing channel closure.");
            }

            var description = ex switch
            {
                PublishException { IsReturn: true } => $"RabbitMQ returned the message from exchange '{_exchange}' because it could not be routed.",
                PublishException => "RabbitMQ negatively acknowledged the published message.",
                OperationCanceledException when timeout?.IsCancellationRequested == true =>
                    $"RabbitMQ did not confirm the published message within {nameof(RabbitMQOptions.PublishConfirmTimeout)} ({_publishConfirmTimeout}).",
                _ => ex.Message
            };
            var wrapperEx = new PublisherSentFailedException(description, ex);
            var errors = new OperateError
            {
                Code = ex.HResult.ToString(),
                Description = description
            };

            return OperateResult.Failed(wrapperEx, errors);
        }
        finally
        {
            if (channel != null)
            {
                if (!disposeChannel)
                {
                    _connectionChannelPool.Return(channel);
                }
                else if (timeout?.IsCancellationRequested == true)
                {
                    // Closing waits for the unresponsive broker, so do not hold the failed send for it.
                    _ = DisposeChannelAsync(channel);
                }
                else
                {
                    await DisposeChannelAsync(channel);
                }
            }
        }
    }

    private async Task DisposeChannelAsync(IChannel channel)
    {
        try
        {
            await channel.DisposeAsync();
        }
        catch (Exception disposeException)
        {
            _logger.LogWarning(disposeException, "Failed to dispose a RabbitMQ channel after a publish failure.");
        }
    }
}