// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace DotNetCore.CAP.RabbitMQ;

internal sealed class RabbitMQExchangeStartupCheck : IHostedService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly RabbitMQOptions _options;

    public RabbitMQExchangeStartupCheck(IServiceProvider serviceProvider, IOptions<RabbitMQOptions> options)
    {
        _serviceProvider = serviceProvider;
        _options = options.Value;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (_options.DeclareExchange) return;

        // Resolve the pool only when the check is needed, so the default configuration has no side effects here.
        var connectionChannelPool = _serviceProvider.GetRequiredService<IConnectionChannelPool>();
        try
        {
            await using var channel = await connectionChannelPool.GetConnection()
                .CreateChannelAsync(cancellationToken: cancellationToken);
            await channel.ExchangeDeclarePassiveAsync(connectionChannelPool.Exchange, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            throw new InvalidOperationException(
                $"Could not verify RabbitMQ exchange '{connectionChannelPool.Exchange}' while {nameof(RabbitMQOptions.DeclareExchange)} is false. Ensure the broker is reachable, the user has permission to check the exchange, and the exchange exists.",
                exception);
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
