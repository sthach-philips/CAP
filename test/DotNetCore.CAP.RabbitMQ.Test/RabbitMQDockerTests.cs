using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DotNetCore.CAP.Messages;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using Xunit;
using Xunit.Abstractions;

namespace DotNetCore.CAP.RabbitMQ.Test;

public sealed class RabbitMqDockerFactAttribute : FactAttribute
{
    public RabbitMqDockerFactAttribute()
    {
        if (Environment.GetEnvironmentVariable("CAP_RABBITMQ_DOCKER_TEST") != "1")
            Skip = "Set CAP_RABBITMQ_DOCKER_TEST=1 to pause a separate disposable RabbitMQ Docker container.";
    }
}

public sealed class RabbitMQDockerTests
{
    private readonly ITestOutputHelper _output;

    public RabbitMQDockerTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [RabbitMqDockerFact]
    public async Task Publish_confirm_timeout_fails_send_when_broker_stops_responding()
    {
        var name = "cap-rabbitmq-test-" + Guid.NewGuid().ToString("N");
        using var reservation = new TcpListener(IPAddress.Loopback, 0);
        reservation.Start();
        var port = ((IPEndPoint)reservation.LocalEndpoint).Port;
        reservation.Stop();
        await DockerAsync("run", "--detach", "--name", name, "--publish", $"127.0.0.1:{port}:5672",
            Environment.GetEnvironmentVariable("CAP_RABBITMQ_DOCKER_IMAGE") ?? "rabbitmq:4-management");
        var paused = false;
        try
        {
            var options = new RabbitMQOptions
            {
                HostName = "127.0.0.1",
                Port = port,
                ExchangeName = "cap.test.timeout",
                PublishConfirms = true,
                PublisherConfirmationTracking = true,
                PublishConfirmTimeout = TimeSpan.FromSeconds(1)
            };
            await UntilBrokerAcceptsConnectionsAsync(options);

            using var pool = new ConnectionChannelPool(NullLogger<ConnectionChannelPool>.Instance,
                Options.Create(new CapOptions { Version = "v1" }), Options.Create(options));
            var transport = new RabbitMqTransport(NullLogger<RabbitMqTransport>.Instance, pool, Options.Create(options));

            var warmUp = await transport.SendAsync(Message());
            Assert.True(warmUp.Succeeded, warmUp.Exception?.ToString());

            await DockerAsync("pause", name);
            paused = true;
            var stopwatch = Stopwatch.StartNew();
            var result = await transport.SendAsync(Message()).WaitAsync(TimeSpan.FromSeconds(60));
            stopwatch.Stop();
            _output.WriteLine($"Timed-out send returned after {stopwatch.Elapsed}.");

            Assert.False(result.Succeeded);
            Assert.Contains(nameof(RabbitMQOptions.PublishConfirmTimeout), result.Exception?.Message);
            Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(10), $"Send returned after {stopwatch.Elapsed}.");

            await DockerAsync("unpause", name);
            paused = false;
        }
        finally
        {
            if (paused) await DockerAsync("unpause", name);
            await DockerAsync("rm", "--force", name);
        }
    }

    private static TransportMessage Message() => new(new Dictionary<string, string?>
    {
        [DotNetCore.CAP.Messages.Headers.MessageId] = Guid.NewGuid().ToString("N"),
        [DotNetCore.CAP.Messages.Headers.MessageName] = "unbound.event"
    }, Encoding.UTF8.GetBytes("test-payload"));

    private static async Task UntilBrokerAcceptsConnectionsAsync(RabbitMQOptions options)
    {
        var factory = new ConnectionFactory { HostName = options.HostName, Port = options.Port };
        var deadline = DateTime.UtcNow.AddSeconds(90);
        while (true)
        {
            try
            {
                await using var connection = await factory.CreateConnectionAsync();
                return;
            }
            catch when (DateTime.UtcNow < deadline)
            {
                await Task.Delay(500);
            }
        }
    }

    private static async Task<string> DockerAsync(params string[] arguments)
    {
        var info = new ProcessStartInfo("docker") { RedirectStandardOutput = true, RedirectStandardError = true, CreateNoWindow = true };
        foreach (var argument in arguments) info.ArgumentList.Add(argument);
        using var process = Process.Start(info)!;
        var output = process.StandardOutput.ReadToEndAsync();
        var error = process.StandardError.ReadToEndAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        await process.WaitForExitAsync(timeout.Token);
        Assert.True(process.ExitCode == 0, await error);
        return await output;
    }
}
