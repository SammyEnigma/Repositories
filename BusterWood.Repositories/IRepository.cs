using System.Collections.Generic;
using System.Threading.Tasks;

namespace BusterWood.Repositories
{
    public interface IRepository<T> where T : new()
    {
        List<T> Select();
        Task<List<T>> SelectAsync();

        T Select(long id);
        Task<T> SelectAsync(long id);

        void Insert(T item);
        Task InsertAsync(T item);

        bool Update(T item);
        Task<bool> UpdateAsync(T item);

        bool Delete(long id);
        Task<bool> DeleteAsync(long id);

        void Save(T item);
        Task SaveAsync(T item);
    }
}