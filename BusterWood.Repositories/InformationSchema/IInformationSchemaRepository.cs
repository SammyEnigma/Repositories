using System.Collections.Generic;
using System.Threading.Tasks;

namespace BusterWood.Repositories.InformationSchema
{
    public interface IInformationSchemaRepository
    {
        Task<TableSchema> TableSchemaAsync(string schema, string table);
    }
}