using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BusterWood.Repositories;
using BusterWood.Repositories.InformationSchema;
using System.Transactions;

namespace UnitTests
{
    [TestFixture]
    public class IntegrationTests
    {
        readonly IDbConnectionFactory connectionFactory;
        DataAccessRepository<Region> repo;

        public IntegrationTests()
        {
            connectionFactory = new ConfiguredSqlConnectionFactory("dev");
            var infoRepo = new SchemaDataAccess(connectionFactory);
            var config = new RepositoryConfiguration { Schema="dbo", Table = "REGION" };
            repo = new DataAccessRepository<Region>(config, connectionFactory, infoRepo);
        }

        [Test]
        public void can_select_all()
        {
            var all = repo.Select();
            Assert.AreNotEqual(0, all);
            Console.WriteLine($"Loaded {all.Count} regions");
        }

        [Test]
        public async Task can_select_all_async()
        {
            var all = await repo.SelectAsync();
            Assert.AreNotEqual(0, all);
            Console.WriteLine($"Loaded {all.Count} regions");
        }

        [Test]
        public void can_select_by_id()
        {
            var r = repo.Select(1);
            Assert.IsNotNull(r);
            Assert.AreEqual(1, r.Id);
        }

        [Test]
        public async Task can_select_by_id_async()
        {
            var r = await repo.SelectAsync(1);
            Assert.IsNotNull(r);
            Assert.AreEqual(1, r.Id);
        }

        [Test]
        public void can_insert()
        {
            using (var txn = new TransactionScope())
            {
                var region = repo.Select(1);
                region.Id = 888;
                region.RegionName += "!";
                repo.Insert(region);
                Assert.IsNotNull(repo.Select(1));
            }
        }

        [Test]
        public async Task can_insert_async()
        {
            using (var txn = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                var region = await repo.SelectAsync(1);
                region.Id = 888;
                region.RegionName += "!";
                await repo.InsertAsync(region);
                Assert.IsNotNull(await repo.SelectAsync(1));
            }
        }

        [Test]
        public void can_delete()
        {
            using (var txn = new TransactionScope())
            {
                Assert.IsTrue(repo.Delete(1));
                Assert.IsNull(repo.Select(1));
            }
        }

        [Test]
        public async Task can_delete_async()
        {
            using (var txn = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                Assert.IsTrue(await repo.DeleteAsync(1));
                Assert.IsNull(await repo.SelectAsync(1));
            }
        }

        [Test]
        public void can_update()
        {
            using (var txn = new TransactionScope())
            {
                var before = repo.Select(1);
                before.RegionName += "!";
                Assert.IsTrue(repo.Update(before));
                var after = repo.Select(1);
                Assert.AreEqual(before.RegionName, after.RegionName);
            }
        }

        [Test]
        public async Task can_update_async()
        {
            using (var txn = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                var before = await repo.SelectAsync(1);
                before.RegionName += "!";
                Assert.IsTrue(await repo.UpdateAsync(before));
                var after = await repo.SelectAsync(1);
                Assert.AreEqual(before.RegionName, after.RegionName);
            }
        }

        [Test]
        public void can_fail_to_update()
        {
            using (var txn = new TransactionScope())
            {
                var before = repo.Select(1);
                before.Id = 99999;
                Assert.IsFalse(repo.Update(before));
            }
        }

        [Test]
        public async Task can_fail_to_update_async()
        {
            using (var txn = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
            {
                var before = await repo.SelectAsync(1);
                before.Id = 99999;
                Assert.IsFalse(await repo.UpdateAsync(before));
            }
        }
    }

    abstract class Audited
    {
        public int AuditVersion { get; set; }
        public char AuditType { get; set; }
        public DateTime AuditDateTime { get; set; }
        public string AuditUser { get; set; }
        public string AuditMachine { get; set; }
        public string AuditApplication { get; set; }
        public string AuditSource { get; set; }
    }

    class Region : Audited, IObjectId
    {
        public long Id { get; set; }
        public string RegionName { get; set; }
        public string Isocode { get; set; }
        public long SuperRegionId { get; set; }
    }
}
