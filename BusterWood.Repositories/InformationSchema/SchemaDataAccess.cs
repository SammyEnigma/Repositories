﻿using BusterWood.Mapper;
using System;
using System.Threading.Tasks;
using System.Data;

namespace BusterWood.Repositories.InformationSchema
{
    public class SchemaDataAccess : IInformationSchemaRepository
    {
        readonly IDbConnectionFactory connectionFactory;
        readonly string ignoreColumnsStartingWith;
        readonly string sql;

        public SchemaDataAccess(IDbConnectionFactory connectionFactory, string ignoreColumnsStartingWith = null)
        {
            if (connectionFactory == null)
                throw new ArgumentNullException(nameof(connectionFactory));
            this.connectionFactory = connectionFactory;
            this.ignoreColumnsStartingWith = ignoreColumnsStartingWith;
            sql = CreateSql(ignoreColumnsStartingWith);
        }

        internal static string CreateSql(string ignoreColumnsStartingWith)
        {
            string ignored = string.IsNullOrEmpty(ignoreColumnsStartingWith) ? "" : $"{Environment.NewLine}AND c.COLUMN_NAME NOT LIKE '{ignoreColumnsStartingWith}%'";

            return $@"SELECT c.*, CAST(COLUMNPROPERTY(object_id(c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') as bit) as IsIdentity
FROM INFORMATION_SCHEMA.COLUMNS c
WHERE c.TABLE_NAME = @table
AND c.TABLE_SCHEMA = @schema{ignored}
ORDER BY c.ORDINAL_POSITION";
        }

        public virtual async Task<TableSchema> TableSchemaAsync(string schema, string table)
        {
            if (string.IsNullOrWhiteSpace(schema)) throw new ArgumentNullException(nameof(schema));
            if (string.IsNullOrWhiteSpace(table)) throw new ArgumentNullException(nameof(table));

            using (var cnn = connectionFactory.Create())
            {
                await cnn.OpenAsync();
                var cols = await cnn.QueryAsync(sql, new { schema, table }).ToListAsync<ColumnSchema>();
                if (cols.Count == 0)
                    throw new DataException($"{schema}.{table} has no columns, are you sure it exists?");
                return new TableSchema
                {
                    Schema = schema,
                    Name = table,
                    Columns = cols
                };
            }
        }

    }
}
