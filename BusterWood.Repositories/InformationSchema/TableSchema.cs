using BusterWood.Mapper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BusterWood.Repositories.InformationSchema
{
    public class TableSchema
    {
        static readonly ColumnSchema sentinal = new ColumnSchema();

        ColumnSchema identityColumn;
        ColumnSchema activeColumn;

        public Identifier Table { get; set; }

        public IReadOnlyList<ColumnSchema> Columns { get; set; }

        public ColumnSchema IdentityColumn
        {
            get
            {
                if (identityColumn == null)
                    identityColumn = Columns.SingleOrDefault(c => c.IsIdentity) ?? sentinal;
                return identityColumn == sentinal ? null : identityColumn;
            }
        }

        public ColumnSchema ActiveColumn
        {
            get
            {
                if (activeColumn == null)
                    activeColumn = Columns.SingleOrDefault(c => c.DataType.Equals("bit", StringComparison.OrdinalIgnoreCase) && c.ColumnName.EndsWith("ACTIVE", StringComparison.OrdinalIgnoreCase)) ?? sentinal;
                return activeColumn == sentinal ? null : activeColumn;
            }
        }

        public string InsertSql<T>()
        {
            var result = Mapping.CreateFromDestination(typeof(T), Columns); //TODO: cache the resulting SQL?

            var sql = new StringBuilder(200);
            sql.Append("INSERT INTO ").Append(Table).Append(" (");

            foreach (var map in result.Mapped.Where(map => map.To != IdentityColumn))
            {
                sql.AppendLine().Append(" [").Append(map.To.Name).Append("],");
            }
            if (ActiveColumn != null)
                sql.AppendLine().Append(" [").Append(ActiveColumn.ColumnName).Append("],");
            sql.Length -= 1; // remove last comma

            sql.AppendLine().Append(") VALUES (");
            foreach (var map in result.Mapped.Where(map => map.To != IdentityColumn))
            {
                sql.AppendLine().Append(" @").Append(map.From.Name).Append(",");
            }
            if (ActiveColumn != null)
                    sql.AppendLine().Append(" 1,");
            sql.Length -= 1; // remove last comma

            sql.AppendLine().Append(')');
            if (IdentityColumn != null)
            {
                sql.AppendLine().Append("SELECT SCOPE_IDENTITY() as ID");
            }
            return sql.ToString();
        }

        public string UpdateSql<T>()
        {
            var result = Mapping.CreateFromDestination(typeof(T), Columns); //TODO: cache the resulting SQL?
            var sql = new StringBuilder(200);
            sql.Append("UPDATE ").Append(Table).Append(" SET");
            foreach (var map in result.Mapped.Where(map => map.To != IdentityColumn))
            {
                sql.AppendLine().Append(" [").Append(map.To.Name).Append("] = @").Append(map.From.Name).Append(",");
            }
            if (ActiveColumn != null)
                sql.AppendLine().Append(" [").Append(ActiveColumn.ColumnName).Append("] = 1,");
            sql.Length -= 1; // remove last comma
            sql.AppendLine().Append("WHERE [ID] = @Id");
            return sql.ToString();
        }

        public string DeleteSql()
        {
            if (ActiveColumn == null)
                return $"DELETE FROM {Table} WHERE [ID] = @Id";
            else
                return $"UPDATE {Table} SET [{ActiveColumn.ColumnName}] = 0 WHERE [ID] = @Id";
        }

        public string SelectByIdSql(string fieldName = "ID")
        {
            var sql = new StringBuilder(200);
            SelectCore(sql);
            sql.AppendLine().Append("WHERE [").Append(fieldName).Append("] = @Id");
            if (ActiveColumn != null)
                sql.AppendLine().Append("AND [").Append(ActiveColumn.ColumnName).Append("] = 1");
            return sql.ToString();
        }

        public string SelectSql()
        {
            var sql = new StringBuilder(200);
            SelectCore(sql);
            if (ActiveColumn != null)
                sql.AppendLine().Append("WHERE [").Append(ActiveColumn.ColumnName).Append("] = 1");
            return sql.ToString();
        }

        void SelectCore(StringBuilder sql)
        {
            sql.Append("SELECT ");
            foreach (var col in Columns.Where(c => c != ActiveColumn))
            {
                sql.Append('[').Append(col.ColumnName).Append("], ");
            }
            sql.Length -= 2; // remove last comma and space
            sql.AppendLine().Append("FROM ").Append(Table);
        }

    }
}
