using System;
using System.Configuration;
using System.Data.Common;
using System.Data.SqlClient;

namespace BusterWood.Repositories
{
    public interface IDbConnectionFactory
    {
        DbConnection Create(); 
    }

    public class SqlConnectionFactory : IDbConnectionFactory
    {
        readonly string connectionString;

        public SqlConnectionFactory(string connectionString)
        {
            if (connectionString == null)
                throw new ArgumentNullException(nameof(connectionString));
            this.connectionString = connectionString;
        }

        public DbConnection Create() => new SqlConnection(connectionString);
    }

    public class ConfiguredSqlConnectionFactory : IDbConnectionFactory
    {
        readonly string connectionString;

        public ConfiguredSqlConnectionFactory(string configName)
        {
            if (configName == null)
                throw new ArgumentNullException(nameof(configName));
            connectionString = ConfigurationManager.ConnectionStrings[configName]?.ConnectionString;
            if (connectionString == null)
                throw new ArgumentNullException(nameof(connectionString));
        }

        public DbConnection Create() => new SqlConnection(connectionString);
    }
}