using Azure;
using Azure.Data.Tables;
using System.Globalization;
using System.Text.Json;

namespace Lotta.Internal
{
    internal class LottaTableEntity : Dictionary<string, object>, ITableEntity
    {
        public LottaTableEntity()
        {
        }

        /// <summary>
        /// the realized type of the stored object, used for deserialization. Stored as a separate field to allow querying by type.
        /// </summary>
        public string Type { get => GetString(nameof(Type)); set => this[nameof(Type)] = value; }

        /// <summary>
        /// Gets or sets the JSON representation of the object or data associated with this property.
        /// </summary>
        [Queryable]
        public string Json { get => GetString(nameof(Json)); set => this[nameof(Json)] = value; }

        /// <summary>
        /// The partition key is a unique identifier for the partition within a given table and forms the first part of an entity's primary key.
        /// </summary>
        /// <value>A string containing the partition key for the entity.</value>
        public string PartitionKey
        {
            get { return GetString(nameof(PartitionKey)); }
            set { this[nameof(PartitionKey)] = value; }
        }

        /// <summary>
        /// The row key is a unique identifier for an entity within a given partition. Together, the <see cref="PartitionKey" /> and RowKey uniquely identify an entity within a table.
        /// </summary>
        /// <value>A string containing the row key for the entity.</value>
        public string RowKey
        {
            get { return GetString(nameof(RowKey)); }
            set { this[nameof(RowKey)] = value; }
        }

        /// <summary>
        /// The Timestamp property is a DateTimeOffset value that is maintained on the server side to record the time an entity was last modified.
        /// The Table service uses the Timestamp property internally to provide optimistic concurrency. The value of Timestamp is a monotonically increasing value,
        /// meaning that each time the entity is modified, the value of Timestamp increases for that entity. This property should not be set on insert or update operations (the value will be ignored).
        /// </summary>
        /// <value>A <see cref="DateTimeOffset"/> containing the timestamp of the entity.</value>
        public DateTimeOffset? Timestamp
        {
            get { return GetValue<DateTimeOffset?>(nameof(Timestamp)); }
            set { this[nameof(Timestamp)] = value!; }
        }

        /// <summary>
        /// Gets or sets the entity's ETag.
        /// </summary>
        /// <value>An <see cref="ETag"/> containing the ETag value for the entity.</value>
        public ETag ETag
        {
            get
            {
                if (TryGetValue(nameof(ETag), out var v) && v is string s && !string.IsNullOrEmpty(s))
                    return new ETag(s);
                return default;
            }
            set { this[nameof(ETag)] = value.ToString(); }
        }


        /// <summary>
        /// Get the value of a <see cref="TableEntity"/>'s
        /// <see cref="string"/> property called
        /// <paramref name="key"/>.
        /// </summary>
        /// <param name="key">The name of the property.</param>
        /// <returns>The value of the property.</returns>
        /// <exception cref="InvalidOperationException">Value associated with given <paramref name="key"/> is not of type <see cref="string" />.</exception>
        public string GetString(string key) => GetValue<string>(key);

        /// <summary>
        /// Get the value of a <see cref="TableEntity"/>'s
        /// <see cref="BinaryData"/> property called
        /// <paramref name="key"/>.
        /// </summary>
        /// <param name="key">The name of the property.</param>
        /// <returns>The value of the property.</returns>
        /// <exception cref="InvalidOperationException">Value associated with given <paramref name="key"/> is not of type byte array.</exception>
        public BinaryData GetBinaryData(string key) => GetValue<BinaryData>(key);

        /// <summary>
        /// Get the value of a <see cref="TableEntity"/>'s
        /// <see cref="byte"/> property called
        /// <paramref name="key"/>.
        /// </summary>
        /// <param name="key">The name of the property.</param>
        /// <returns>The value of the property.</returns>
        /// <exception cref="InvalidOperationException">Value associated with given <paramref name="key"/> is not of type byte array.</exception>
        public byte[] GetBinary(string key) => GetValue<byte[]>(key);

        /// <summary>
        /// Get the value of a <see cref="TableEntity"/>'s
        /// <see cref="string"/> property called
        /// <paramref name="key"/>.
        /// </summary>
        /// <param name="key">The name of the property.</param>
        /// <returns>The value of the property.</returns>
        /// <exception cref="InvalidOperationException">Value associated with given <paramref name="key"/> is not of type <see cref="bool" />.</exception>
        public bool? GetBoolean(string key) => GetValue<bool?>(key);

        /// <summary>
        /// Get the value of a <see cref="TableEntity"/>'s
        /// <see cref="DateTime"/> property called
        /// <paramref name="key"/>.
        /// </summary>
        /// <param name="key">The name of the property.</param>
        /// <returns>The value of the property.</returns>
        /// <exception cref="InvalidOperationException">Value associated with given <paramref name="key"/> is not of type <see cref="DateTime" />.</exception>
        public DateTime? GetDateTime(string key) => GetValue<DateTime?>(key);

        /// <summary>
        /// Get the value of a <see cref="TableEntity"/>'s
        /// <see cref="DateTimeOffset"/> property called
        /// <paramref name="key"/>.
        /// </summary>
        /// <param name="key">The name of the property.</param>
        /// <returns>The value of the property.</returns>
        /// <exception cref="InvalidOperationException">Value associated with given <paramref name="key"/> is not of type <see cref="DateTimeOffset" />.</exception>
        public DateTimeOffset? GetDateTimeOffset(string key) => GetValue<DateTimeOffset?>(key);

        /// <summary>
        /// Get the value of a <see cref="TableEntity"/>'s
        /// <see cref="double"/> property called
        /// <paramref name="key"/>.
        /// </summary>
        /// <param name="key">The name of the property.</param>
        /// <returns>The value of the property.</returns>
        /// <exception cref="InvalidOperationException">Value associated with given <paramref name="key"/> is not of type <see cref="double" />.</exception>
        public double? GetDouble(string key) => GetValue<double?>(key);

        /// <summary>
        /// Get the value of a <see cref="TableEntity"/>'s
        /// <see cref="Guid"/> property called
        /// <paramref name="key"/>.
        /// </summary>
        /// <param name="key">The name of the property.</param>
        /// <returns>The value of the property.</returns>
        /// <exception cref="InvalidOperationException">Value associated with given <paramref name="key"/> is not of type <see cref="Guid" />.</exception>
        public Guid? GetGuid(string key) => GetValue<Guid?>(key);

        /// <summary>
        /// Get the value of a <see cref="TableEntity"/>'s
        /// <see cref="int"/> property called
        /// <paramref name="key"/>.
        /// </summary>
        /// <param name="key">The name of the property.</param>
        /// <returns>The value of the property.</returns>
        /// <exception cref="InvalidOperationException">Value associated with given <paramref name="key"/> is not of type <see cref="int" />.</exception>
        public int? GetInt32(string key) => GetValue<int?>(key);

        /// <summary>
        /// Get the value of a <see cref="TableEntity"/>'s
        /// <see cref="long"/> property called
        /// <paramref name="key"/>.
        /// </summary>
        /// <param name="key">The name of the property.</param>
        /// <returns>The value of the property.</returns>
        /// <exception cref="InvalidOperationException">Value associated with given <paramref name="key"/> is not of type <see cref="long" />.</exception>
        public long? GetInt64(string key) => GetValue<long?>(key);

        /// <summary>
        /// Get an entity property.
        /// </summary>
        /// <typeparam name="T">The expected type of the property value.</typeparam>
        /// <param name="key">The property name.</param>
        /// <returns>The value of the property.</returns>
        /// <exception cref="InvalidOperationException">Value associated with given <paramref name="key"/> is not of given type <typeparamref name="T"/>.</exception>
        private T GetValue<T>(string key) => (T)GetValue(key, typeof(T));

        /// <summary>
        /// Get an entity property.
        /// </summary>
        /// <param name="key">The property name.</param>
        /// <param name="type">The expected type of the property value.</param>
        /// <returns>The value of the property.</returns>
        /// <exception cref="InvalidOperationException">Value associated with given <paramref name="key"/> is not of type <paramref name="type"/>.</exception>
        private object GetValue(string key, Type? type = null)
        {
            if (!this.TryGetValue(key, out object? value) || value == null)
            {
                return null!;
            }

            if (type != null)
            {
                var valueType = value.GetType();
                if (type == typeof(DateTime?) && valueType == typeof(DateTimeOffset))
                {
                    return ((DateTimeOffset)value).UtcDateTime;
                }
                if (type == typeof(DateTimeOffset?) && valueType == typeof(DateTime))
                {
                    return new DateTimeOffset((DateTime)value);
                }
                if (type == typeof(BinaryData) && value is byte[] byteArray)
                {
                    return new BinaryData(byteArray);
                }
                EnforceType(type, valueType);
            }

            return value;
        }

        /// <summary>
        /// Ensures that the given type matches the type of the existing
        /// property; throws an exception if the types do not match.
        /// </summary>
        private static void EnforceType(Type requestedType, Type givenType)
        {
            if (!requestedType.IsAssignableFrom(givenType))
            {
                throw new InvalidOperationException(string.Format(
                    CultureInfo.InvariantCulture,
                    $"Cannot return {requestedType} type for a {givenType} typed property."));
            }
        }

        /// <summary>
        /// Performs type coercion for numeric types.
        /// <param name="newValue"/> of type int will be coerced to long or double if <param name="existingValue"/> is typed as long or double.
        /// All other type assignment changes will be accepted as is.
        /// </summary>
        private static object CoerceType(object existingValue, object newValue)
        {
            if (!existingValue.GetType().IsAssignableFrom(newValue.GetType()))
            {
#pragma warning disable CS8603 // Possible null reference return.
                return existingValue switch
                {
                    double => newValue switch
                    {
                        // if we already had a double value, preserve it as double even if newValue was an int.
                        // example: entity["someDoubleValue"] = 5;
                        int newIntValue => (double)newIntValue,
                        _ => newValue
                    },
                    long _ => newValue switch
                    {
                        // if we already had a long value, preserve it as long even if newValue was an int.
                        // example: entity["someLongValue"] = 5;
                        int newIntValue => (long)newIntValue,
                        _ => newValue
                    },
                    string when newValue is DateTime || newValue is DateTimeOffset => newValue.ToString(),
                    _ => newValue
                };
#pragma warning restore CS8603 // Possible null reference return.
            }

            return newValue;
        }
    }
}
