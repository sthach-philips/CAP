using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Xunit;

namespace DotNetCore.CAP.RabbitMQ.Test;

public sealed class RabbitMQOptionsTests
{
    [Fact]
    public void Defaults_preserve_existing_publish_and_exchange_behavior()
    {
        var options = new RabbitMQOptions();

        Assert.False(options.PublishConfirms);
        Assert.False(options.PublisherConfirmationTracking);
        Assert.False(options.MandatoryPublishing);
        Assert.True(options.DeclareExchange);
        Assert.Null(options.PublishConfirmTimeout);
    }

    [Fact]
    public void Tracked_mandatory_configuration_is_valid()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        new RabbitMqCapOptionsExtension(options =>
        {
            options.PublishConfirms = true;
            options.PublisherConfirmationTracking = true;
            options.MandatoryPublishing = true;
        }).AddServices(services);

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<RabbitMQOptions>>().Value;

        Assert.True(options.PublishConfirms);
        Assert.True(options.PublisherConfirmationTracking);
        Assert.True(options.MandatoryPublishing);
    }

    [Fact]
    public async Task Exchange_startup_check_does_not_touch_the_broker_by_default()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        new RabbitMqCapOptionsExtension(_ => { }).AddServices(services);
        services.AddSingleton<IConnectionChannelPool>(_ =>
            throw new InvalidOperationException("The channel pool should not be resolved."));

        await using var provider = services.BuildServiceProvider();
        var check = provider.GetServices<IHostedService>().OfType<RabbitMQExchangeStartupCheck>().Single();

        await check.StartAsync(CancellationToken.None);
    }

    [Theory]
    [InlineData(true, true, 0)]
    [InlineData(true, true, -1)]
    [InlineData(true, false, 1)]
    public void Invalid_publish_confirm_timeout_fails_options_validation(bool publishConfirms,
        bool trackConfirmations, int timeoutSeconds)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        new RabbitMqCapOptionsExtension(options =>
        {
            options.PublishConfirms = publishConfirms;
            options.PublisherConfirmationTracking = trackConfirmations;
            options.PublishConfirmTimeout = TimeSpan.FromSeconds(timeoutSeconds);
        }).AddServices(services);

        using var provider = services.BuildServiceProvider();
        var exception = Assert.Throws<OptionsValidationException>(() =>
            provider.GetRequiredService<IOptions<RabbitMQOptions>>().Value);

        Assert.Contains(nameof(RabbitMQOptions.PublishConfirmTimeout), exception.Message);
    }

    [Theory]
    [InlineData(false, true, false, "PublisherConfirmationTracking")]
    [InlineData(false, false, true, "MandatoryPublishing")]
    [InlineData(true, false, true, "MandatoryPublishing")]
    public void Invalid_confirmation_combinations_fail_options_validation(bool publishConfirms,
        bool trackConfirmations, bool mandatoryPublishing, string expectedOption)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        new RabbitMqCapOptionsExtension(options =>
        {
            options.PublishConfirms = publishConfirms;
            options.PublisherConfirmationTracking = trackConfirmations;
            options.MandatoryPublishing = mandatoryPublishing;
        }).AddServices(services);

        using var provider = services.BuildServiceProvider();
        var exception = Assert.Throws<OptionsValidationException>(() =>
            provider.GetRequiredService<IOptions<RabbitMQOptions>>().Value);

        Assert.Contains(expectedOption, exception.Message);
    }
}
