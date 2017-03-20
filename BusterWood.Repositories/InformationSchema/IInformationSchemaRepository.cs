using System.Collections.Generic;
using System.Threading.Tasks;

namespace BusterWood.Repositories.InformationSchema
{
    public interface IInformationSchemaRepository
    {
        Task<TableSchema> TableSchemaAsync(Identifier table);
    }
}