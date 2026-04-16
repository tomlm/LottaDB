using System.ComponentModel;
using System.Globalization;

namespace Lotta.Internal;

internal sealed class UtcDateTimeConverter : TypeConverter
{
    private readonly string _format;

    public UtcDateTimeConverter(string format)
    {
        _format = format;
    }

    public override bool CanConvertFrom(ITypeDescriptorContext? context, Type sourceType)
        => sourceType == typeof(string) || sourceType == typeof(DateTime) || base.CanConvertFrom(context, sourceType);

    public override bool CanConvertTo(ITypeDescriptorContext? context, Type? destinationType)
        => destinationType == typeof(string) || base.CanConvertTo(context, destinationType);

    public override object? ConvertFrom(ITypeDescriptorContext? context, CultureInfo? culture, object value)
    {
        if (value is string text)
        {
            if (_format.Equals("O", StringComparison.OrdinalIgnoreCase))
            {
                return DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind | DateTimeStyles.AdjustToUniversal);
            }

            return DateTime.ParseExact(text, _format, CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
        }

        if (value is DateTime dateTime)
        {
            return dateTime.Kind == DateTimeKind.Utc ? dateTime : dateTime.ToUniversalTime();
        }

        return base.ConvertFrom(context, culture, value);
    }

    public override object? ConvertTo(ITypeDescriptorContext? context, CultureInfo? culture, object? value, Type destinationType)
    {
        if (destinationType == typeof(string) && value is DateTime dateTime)
        {
            var utc = dateTime.Kind == DateTimeKind.Utc ? dateTime : dateTime.ToUniversalTime();
            return utc.ToString(_format, CultureInfo.InvariantCulture);
        }

        return base.ConvertTo(context, culture, value, destinationType);
    }
}

internal sealed class UtcDateTimeOffsetConverter : TypeConverter
{
    private readonly string _format;

    public UtcDateTimeOffsetConverter(string format)
    {
        _format = format;
    }

    public override bool CanConvertFrom(ITypeDescriptorContext? context, Type sourceType)
        => sourceType == typeof(string) || sourceType == typeof(DateTimeOffset) || base.CanConvertFrom(context, sourceType);

    public override bool CanConvertTo(ITypeDescriptorContext? context, Type? destinationType)
        => destinationType == typeof(string) || base.CanConvertTo(context, destinationType);

    public override object? ConvertFrom(ITypeDescriptorContext? context, CultureInfo? culture, object value)
    {
        if (value is string text)
        {
            if (_format.Equals("O", StringComparison.OrdinalIgnoreCase))
            {
                return DateTimeOffset.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind).ToUniversalTime();
            }

            return DateTimeOffset.ParseExact(text, _format, CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
        }

        if (value is DateTimeOffset dateTimeOffset)
        {
            return dateTimeOffset.ToUniversalTime();
        }

        return base.ConvertFrom(context, culture, value);
    }

    public override object? ConvertTo(ITypeDescriptorContext? context, CultureInfo? culture, object? value, Type destinationType)
    {
        if (destinationType == typeof(string) && value is DateTimeOffset dateTimeOffset)
        {
            return dateTimeOffset.ToUniversalTime().ToString(_format, CultureInfo.InvariantCulture);
        }

        return base.ConvertTo(context, culture, value, destinationType);
    }
}
