using System;
using System.Threading.Tasks;
using BusterWood.Mapper;
using BusterWood.Repositories.InformationSchema;
using System.Collections.Generic;
using System.Reflection;
using System.Linq.Expressions;

namespace BusterWood.Repositories
{
    public class DataAccessRepository<T> : IRepository<T> where T : new()
    {
        readonly RepositoryConfiguration config;
        protected readonly IDbConnectionFactory connectionFactory;
        readonly Lazy<Task<TableSchema>> lazyTable;

        public static Func<T, long> GetId { get; set; }
        public static Action<T, long> SetId { get; set; }

        static DataAccessRepository()
        {
            BindGetAndSetId();
        }

        static void BindGetAndSetId()
        {
            var idName = IdMemberName();
            if (idName != null)
            {
                GetId = GetIdFunc(idName);
                SetId = SetIdFunc(idName);
            }
        }

        static string IdMemberName()
        {
            var idMember = typeof(T).GetMember("Id", BindingFlags.GetField | BindingFlags.GetProperty | BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase);
            return idMember == null || idMember.Length == 0 ? null : idMember[0].Name;
        }

        static Func<T, long> GetIdFunc(string idName)
        {
            var input = Expression.Parameter(typeof(T), "input");
            var val = Expression.PropertyOrField(input, idName);
            return (Func<T, long>)Expression.Lambda(typeof(Func<T, long>), val, input).Compile();
        }

        static Action<T, long> SetIdFunc(string idName)
        {
            var item = Expression.Parameter(typeof(T), "item");
            var id = Expression.Parameter(typeof(long), "id");
            var val = Expression.Assign(Expression.PropertyOrField(item, idName), id);
            return (Action<T, long>)Expression.Lambda(typeof(Action<T, long>), val, item, id).Compile();
        }

        public DataAccessRepository(RepositoryConfiguration config, IDbConnectionFactory connectionFactory, IInformationSchemaRepository infoRepository)
        {
            if (config == null)
                throw new ArgumentNullException(nameof(config));
            if (config.Table == Identifier.Empty)
                throw new ArgumentException($"config {nameof(config.Table)} must not be null");
            if (connectionFactory == null)
                throw new ArgumentNullException(nameof(connectionFactory));
            if (infoRepository == null)
                throw new ArgumentNullException(nameof(infoRepository));
            this.config = config;
            this.connectionFactory = connectionFactory;
            lazyTable = new Lazy<Task<TableSchema>>(() => infoRepository.TableSchemaAsync(config.Table));
        }

        public async Task<List<T>> SelectAsync()
        {
            var sql = config.SelectProc != Identifier.Empty ? (string)config.SelectProc : (await lazyTable.Value).SelectSql();
            using (var cnn = connectionFactory.Create())
            {
                await cnn.OpenAsync();
                return await cnn.QueryAsync(sql).ToListAsync<T>();
            }
        }

        public List<T> Select()
        {
            var sql = config.SelectProc != Identifier.Empty ? (string)config.SelectProc : lazyTable.Value.Result.SelectSql();
            using (var cnn = connectionFactory.Create())
            {
                cnn.Open();
                return cnn.Query(sql).ToList<T>();
            }
        }

        public async Task<T> SelectAsync(long id)
        {
            var sql = config.SelectByIdProc != Identifier.Empty ? (string)config.SelectByIdProc : (await lazyTable.Value).SelectByIdSql();
            using (var cnn = connectionFactory.Create())
            {
                await cnn.OpenAsync();
                return await cnn.QueryAsync(sql, new { id }).SingleOrDefaultAsync<T>();
            }
        }

        public T Select(long id)
        {
            var sql = config.SelectByIdProc != Identifier.Empty ? (string)config.SelectByIdProc : lazyTable.Value.Result.SelectByIdSql();
            using (var cnn = connectionFactory.Create())
            {
                cnn.Open();
                return cnn.Query(sql, new { id }).SingleOrDefault<T>();
            }
        }

        public List<T> SelectBy<U>(U id, string fieldName)
        {
            var sql = lazyTable.Value.Result.SelectByIdSql(fieldName);
            using (var cnn = connectionFactory.Create())
            {
                cnn.Open();
                return cnn.Query(sql, new { id }).ToList<T>();
            }
        }

        public async Task<List<T>> SelectByAsync<U>(U id, string fieldName)
        {
            var sql = (await lazyTable.Value).SelectByIdSql(fieldName);
            using (var cnn = connectionFactory.Create())
            {
                await cnn.OpenAsync();
                return await cnn.QueryAsync(sql, new { id }).ToListAsync<T>();
            }
        }

        public async Task<bool> DeleteAsync(long id)
        {
            string sql = config.DeleteProc != Identifier.Empty ? (string)config.DeleteProc : (await lazyTable.Value).DeleteSql();
            using (var cnn = connectionFactory.Create())
            {
                await cnn.OpenAsync();
                int deleted = await cnn.ExecuteAsync(sql, new { id });
                return deleted > 0;
            }
        }

        public bool Delete(long id)
        {
            var sql = config.DeleteProc != Identifier.Empty ? (string)config.DeleteProc : lazyTable.Value.Result.DeleteSql();
            using (var cnn = connectionFactory.Create())
            {
                cnn.Open();
                int deleted = cnn.Execute(sql, new { id });
                return deleted > 0;
            }
        }

        public async Task InsertAsync(T item)
        {
            var sql = config.InsertProc != Identifier.Empty ? (string)config.InsertProc : (await lazyTable.Value).InsertSql();
            using (var cnn = connectionFactory.Create())
            {
                await cnn.OpenAsync();
                if (SetIdAfterInsert(await lazyTable.Value))
                    SetId(item, await cnn.QueryAsync(sql, item).SingleAsync<long>());
                else
                    await cnn.ExecuteAsync(sql, item);
            }
        }

        static bool SetIdAfterInsert(TableSchema tableSchema) => tableSchema.IdentityColumn != null && SetId != null;

        public void Insert(T item)
        {
            var sql = config.InsertProc != Identifier.Empty ? (string)config.InsertProc : lazyTable.Value.Result.InsertSql();
            using (var cnn = connectionFactory.Create())
            {
                cnn.Open();
                if (lazyTable.Value.Result.IdentityColumn != null && SetId != null)
                    SetId(item, cnn.Query(sql, item).Single<long>());
                else
                    cnn.Execute(sql, item);
            }
        }

        public async Task<bool> UpdateAsync(T item)
        {
            var sql = config.UpdateProc != Identifier.Empty ? (string)config.UpdateProc : (await lazyTable.Value).UpdateSql();
            using (var cnn = connectionFactory.Create())
            {
                await cnn.OpenAsync();
                int updated = await cnn.ExecuteAsync(sql, item);
                return updated > 0;
            }
        }

        public bool Update(T item)
        {
            var sql = config.UpdateProc != Identifier.Empty ? (string)config.UpdateProc : lazyTable.Value.Result.UpdateSql();
            using (var cnn = connectionFactory.Create())
            {
                cnn.Open();
                int updated = cnn.Execute(sql, item);
                return updated > 0;
            }
        }

        public void Save(T item)
        {
            if (GetId == null)
                ThrowGetIdNotSet();
            if (GetId(item) == 0)
                Insert(item);
            else if (!Update(item))
                ThrowUpdateFailed(item);
        }

        static void ThrowGetIdNotSet()
        {
            throw new InvalidOperationException($"{nameof(GetId)} property is not set for type {nameof(T)}");
        }

        public async Task SaveAsync(T item)
        {
            if (GetId == null)
                ThrowGetIdNotSet();
            if (GetId(item) == 0)
                await InsertAsync(item);
            else if (!await UpdateAsync(item))
                ThrowUpdateFailed(item);
        }

        void ThrowUpdateFailed(T item)
        {
            throw new InvalidOperationException($"Failed to update {nameof(T)} with id {(GetId == null ? "?" : GetId(item).ToString())}");
        }
    }

    public class RepositoryConfiguration
    {
        public Identifier Table { get; set; }
        public Identifier SelectProc { get; set; }
        public Identifier SelectActiveProc { get; set; }
        public Identifier SelectByIdProc { get; set; }
        public Identifier InsertProc { get; set; }
        public Identifier UpdateProc { get; set; }
        public Identifier DeleteProc { get; set; }
        //public string MergeProc { get; set; }
        //public string TableType { get; set; }
        //public bool InsertWithTvp { get; set; }
        //public bool UpdateWithTvp { get; set; }
        //public bool MergeWithTvp { get; set; }

    }
}
