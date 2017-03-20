using BusterWood.Repositories.InformationSchema;
using NUnit.Framework;

namespace UnitTests
{
    [TestFixture]
    public class TableSchemaTests
    {
        [TestCase]
        public void creates_valid_select_sql()
        {
            var s = TestSchema();
            var sql = s.SelectSql();
            Assert.AreEqual(@"SELECT [id], [description], [super_region]
FROM [dbo].[order]", sql);
        }

        [TestCase]
        public void creates_valid_select_sql_only_active()
        {
            var s = TestSchemaWithActive();
            var sql = s.SelectSql();
            Assert.AreEqual(@"SELECT [id], [description], [super_region]
FROM [dbo].[order]
WHERE [is_active] = 1", sql);
        }

        [TestCase]
        public void creates_valid_select_by_id_sql()
        {
            var s = TestSchema();
            var sql = s.SelectByIdSql();
            Assert.AreEqual(@"SELECT [id], [description], [super_region]
FROM [dbo].[order]
WHERE [ID] = @Id", sql);
        }

        [TestCase]
        public void creates_valid_select_by_id_sql_only_active()
        {
            var s = TestSchemaWithActive();
            var sql = s.SelectByIdSql();
            Assert.AreEqual(@"SELECT [id], [description], [super_region]
FROM [dbo].[order]
WHERE [ID] = @Id
AND [is_active] = 1", sql);
        }

        [TestCase]
        public void creates_insert_sql_for_identity_column()
        {
            var s = TestSchema();
            var sql = s.InsertSql();
            Assert.AreEqual(@"INSERT INTO [dbo].[order] (
 [description],
 [super_region]
) VALUES (
 @Description,
 @SuperRegion
)
SELECT SCOPE_IDENTITY() as ID", sql);
        }

        [TestCase]
        public void creates_insert_sql_for_identity_column_with_active()
        {
            var s = TestSchemaWithActive();
            var sql = s.InsertSql();
            Assert.AreEqual(@"INSERT INTO [dbo].[order] (
 [description],
 [super_region],
 [is_active]
) VALUES (
 @Description,
 @SuperRegion,
 1
)
SELECT SCOPE_IDENTITY() as ID", sql);
        }

        [TestCase]
        public void creates_insert_sql_without_identity_column()
        {
            var s = TestSchema();
            s.Columns[0].IsIdentity = false;
            var sql = s.InsertSql();
            Assert.AreEqual(@"INSERT INTO [dbo].[order] (
 [id],
 [description],
 [super_region]
) VALUES (
 @Id,
 @Description,
 @SuperRegion
)", sql);
        }

        [TestCase]
        public void creates_insert_sql_without_identity_column_with_active()
        {
            var s = TestSchemaWithActive();
            s.Columns[0].IsIdentity = false;
            var sql = s.InsertSql();
            Assert.AreEqual(@"INSERT INTO [dbo].[order] (
 [id],
 [description],
 [super_region],
 [is_active]
) VALUES (
 @Id,
 @Description,
 @SuperRegion,
 1
)", sql);
        }

        [TestCase]
        public void creates_update_sql()
        {
            var s = TestSchema();
            var sql = s.UpdateSql();
            Assert.AreEqual(@"UPDATE [dbo].[order] SET
 [description] = @Description,
 [super_region] = @SuperRegion
WHERE [ID] = @Id", sql);
        }

        [TestCase]
        public void creates_update_sql_with_active_flag()
        {
            var s = TestSchemaWithActive();
            var sql = s.UpdateSql();
            Assert.AreEqual(@"UPDATE [dbo].[order] SET
 [description] = @Description,
 [super_region] = @SuperRegion,
 [is_active] = 1
WHERE [ID] = @Id", sql);
        }

        [TestCase]
        public void delete_sql_with_active_flag()
        {
            var s = TestSchemaWithActive();
            var sql = s.DeleteSql();
            Assert.AreEqual(@"UPDATE [dbo].[order] SET [is_active] = 0 WHERE [ID] = @Id", sql);
        }

        [TestCase]
        public void delete_sql_without_active_flag()
        {
            var s = TestSchema();
            var sql = s.DeleteSql();
            Assert.AreEqual(@"DELETE FROM [dbo].[order] WHERE [ID] = @Id", sql);
        }

        static TableSchema TestSchema()
        {
            return new TableSchema
            {
                Schema = "dbo",
                Name = "order",
                Columns = new[]
                {
                    new ColumnSchema { ColumnName="id", DataType="bigint", IsIdentity=true },
                    new ColumnSchema { ColumnName="description", DataType="varchar" },
                    new ColumnSchema { ColumnName="super_region", DataType="int" },
                }
            };
        }

        static TableSchema TestSchemaWithActive()
        {
            return new TableSchema
            {
                Schema = "dbo",
                Name = "order",
                Columns = new[]
                {
                    new ColumnSchema { ColumnName="id", DataType="bigint", IsIdentity=true },
                    new ColumnSchema { ColumnName="description", DataType="varchar" },
                    new ColumnSchema { ColumnName="super_region", DataType="int" },
                    new ColumnSchema { ColumnName="is_active", DataType="bit" },
                }
            };
        }
    }
}
