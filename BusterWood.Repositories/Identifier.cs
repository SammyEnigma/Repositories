using System;
using static System.StringComparison;

namespace BusterWood.Repositories
{
    public struct Identifier : IEquatable<Identifier>
    {
        public static readonly Identifier Empty = new Identifier();

        public string Schema { get; }
        public string Name { get; }

        public Identifier(string name) : this("dbo", name)
        {
        }

        public Identifier(string schema, string name)
        {
            if (string.IsNullOrWhiteSpace(schema))
                throw new ArgumentNullException(nameof(schema));
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name));
            Schema = schema;
            Name = name;
        }

        public override bool Equals(object obj) => obj is Identifier && Equals((Identifier)obj);

        public bool Equals(Identifier other) => string.Equals(Schema, other.Schema, OrdinalIgnoreCase) && string.Equals(Name, other.Name, OrdinalIgnoreCase);

        public override int GetHashCode() => (Schema?.GetHashCode() + Name?.GetHashCode()).GetValueOrDefault();

        public override string ToString() => this == Empty ? "" : $"[{Schema}].[{Name}]";

        public static implicit operator Identifier(string name) => new Identifier(name);

        public static implicit operator string(Identifier id) => id.ToString();

        public static bool operator ==(Identifier left, Identifier right) => left.Equals(right);

        public static bool operator !=(Identifier left, Identifier right) => !left.Equals(right);


    }
}